import boto3
import StringIO
import json
import pickle
import threading
import os
import subprocess

from collections import defaultdict

import utils
import utils.clients
import messages


class Manager(object):
    def __init__(self):
        self._tasks_lock = threading.Lock()
        self._tasks = {}
        self._workers = []
        self._workers_lock = threading.Lock()
        self._logger = utils.set_logger('manager')

        # Create new threads
        self._local_thread = ManagerEmployee(self,
                                             'local',
                                             self._tasks_lock,
                                             self._tasks,
                                             self._workers_lock,
                                             self._workers,
                                             self._logger)
        self._worker_thread = ManagerEmployee(self,
                                              'job',
                                              self._tasks_lock,
                                              self._tasks,
                                              self._workers_lock,
                                              self._workers,
                                              self._logger)

    def run(self):

        # Start new Threads
        self._local_thread.start()
        self._worker_thread.start()

        # Wait on both thread
        self._worker_thread.join()
        self._logger.debug('Worker thread went to sleep')

        self._local_thread.join()
        self._logger.debug('Both thread went to sleep, commencing shutdown')

        if 'ROY_IS_THE_BEST' in os.environ:
            subprocess.call('/bin/bash -c "shutdown -h now"', shell=True)

    def flag_local_terminate(self):
        self._local_thread.terminate_local()

    def flag_worker_terminate(self):
        self._worker_thread.flag_terminate()


class ManagerEmployee(threading.Thread):

    def __init__(self, manager, role, tasks_lock, tasks, workers_lock, workers, logger):
        super(ManagerEmployee, self).__init__()
        self._manager = manager
        self._role = role
        self._tasks_lock = tasks_lock
        self._project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',))
        # self._logger_file_path = os.path.join(self._project_path, 'manager.log')
        self._logger = logger.getChild(role)
        self._e2_client = utils.clients.Ec2(logger=self._logger)
        self._s3_client = utils.clients.S3(logger=self._logger)
        self._sqs_client = utils.clients.Sqs(logger=self._logger)
        self._manager_to_local_queue = self._sqs_client.get_or_create_queue(utils.Names.manager_to_local_queue)
        self._local_to_manager_queue = self._sqs_client.get_or_create_queue(utils.Names.local_to_manager_queue)
        self._manager_to_workers_queue = self._sqs_client.get_or_create_queue(utils.Names.manager_to_workers_queue)
        self._workers_to_manager_queue = self._sqs_client.get_or_create_queue(utils.Names.worker_to_manager_queue)
        self._project_bucket = self._s3_client.create_or_get_bucket(utils.Names.project_bucket_name)
        self._setup_script_path = os.path.join(self._project_path, 'setup_scripts/worker_setup.sh')
        self._worker_tag = {'Key': 'Role', 'Value': 'Worker'}
        self._workers = workers
        self._worker_lock = workers_lock
        self._tasks = tasks
        self._workers_statistic = defaultdict(lambda: defaultdict(int))
        self._terminate = False

    def run(self):
        """
        Here we will run 2 threads, each with one flow (queue names are not real):
        Flow 1: loop: read messages from local_to_manager queue -> download input files -> create task for each ->
        -> create workers if needed -> create jobs for the task -> put jobs in manager_to_workers queue
        Flow 2: loop: read messages from workers_to_queue -> update the task object in the manager state ->
        if the task is done create summery file and send to manager_to_local queue + check if we need to kill some
        workers.
        :return:
        """

        if self._role == 'local':
            self._logger.debug('Manager of type local starting')
            self._handle_local_requests()
        else:
            self._logger.debug('Manager of type job starting')
            self._handle_worker_done_jobs()

    def flag_terminate(self):
        self._terminate = True

    def _handle_local_requests(self):
        """
        This is the Flow 1 main function
        :return:
        """
        while True:
            if self._terminate:
                self._logger.debug('Bye bye cruel world')
                exit(0)
            local_messages = self._sqs_client.get_messages(self._local_to_manager_queue,
                                                           timeout=20,
                                                           number_of_messages=1)
            for message in local_messages:
                new_task = self._download_input_file_and_create_task_from_message(message)
                self._tasks_lock.acquire()
                try:
                    self._tasks[new_task.uuid] = new_task
                finally:
                    self._tasks_lock.release()
                self._create_workers_if_needed(new_task)
                self._create_jobs_and_dispetch_them(new_task)
                message.delete()

    def _create_workers_if_needed(self, task):
        self._worker_lock.acquire()
        try:
            number_of_needed_workers = task.number_of_workers
            missing_work_force = number_of_needed_workers - len(self._workers)
            if missing_work_force != 0:

                self._logger.debug('Creating new workers. new_workers: {0}, old_workers {1}'.format(missing_work_force,
                                                                                                    len(self._workers)))
                with open(self._setup_script_path, 'r') as myfile:
                    user_data = myfile.read().format(os.environ.get('AWS_ACCESS_KEY_ID'),
                                                     os.environ.get('AWS_SECRET_ACCESS_KEY'))
                iam_instance_profile = {'Arn': utils.Names.arn}
                new_workers = self._e2_client.create_instance(image_id='ami-b73b63a0',
                                                              tags=[self._worker_tag],
                                                              iam_instance_profile=iam_instance_profile,
                                                              instance_type='t2.nano',
                                                              min_count=1,
                                                              max_count=missing_work_force,
                                                              user_data=user_data)
                # for new_worker in new_workers:
                #    self._workers.append(new_worker)
                self._workers.extend(new_workers)
        finally:
            self._worker_lock.release()

    def _handle_worker_done_jobs(self):
        """
        This is the Flow 2 main function
        :return:
        """
        while True:
            worker_messages = self._sqs_client.get_messages(self._workers_to_manager_queue,
                                                            timeout=20,
                                                            number_of_messages=1)
            for message in worker_messages:
                done_job = message.body
                done_job = pickle.loads(done_job)
                local_uuid = done_job.local_uuid
                self._tasks_lock.acquire()
                try:
                    self._logger.debug('Got new message from worker. Worker_id: {0}'.format(done_job.worker_id))
                    if local_uuid in self._tasks:
                        self.update_workers_statistics(done_job) # get the data from the worker for the worker statistic
                        relevant_task = self._tasks[local_uuid]
                        relevant_task.add_asteroid_list(asteroid_list=done_job.asteroids, date=done_job.date)
                        self._logger.debug("Local {} have {} days left to do".format(local_uuid,
                                                                                     relevant_task.get_how_many_days_left()))
                        if relevant_task.is_done():
                            self._create_summery_file_and_send(relevant_task)
                            del self._tasks[local_uuid]
                            self._delete_workers_if_needed()
                            self._worker_terminate_if_needed()
                except Exception as e:
                    pass
                finally:
                    self._tasks_lock.release()
                message.delete()

    def _delete_workers_if_needed(self):
        self._worker_lock.acquire()
        try:
            if len(self._tasks) == 0:

                # No more tasks, kill workers
                self._logger.debug('No more tasks for now, deleting workers')
                for worker in self._workers:
                    worker.terminate()
                for i in range(0, len(self._workers)):
                    self._workers.pop()
        finally:
            self._worker_lock.release()

    def _create_summery_file_and_send(self, task):

        # Upload summery file to s3
        summery_file = task.make_json()
        file_object = StringIO.StringIO()
        file_object.write(summery_file)
        file_object.seek(0)
        summery_file_name = 'summery_file_' + task.uuid + '.json'
        self._s3_client.upload_object_as_file(self._project_bucket, key=summery_file_name, file_object=file_object)

        # Send message to local about the summery file
        local_queue = self._sqs_client.get_queue(utils.Names.manager_to_local_queue + '_' + task.uuid)
        message_to_local = messages.DoneTask(summery_file_name)
        message_to_local = messages.DoneTask.encode(message_to_local)
        self._sqs_client.send_message(local_queue, message_to_local)

    def _download_and_parse_input_file(self, task):
        input_file = StringIO.StringIO()
        self._logger.debug('Downloading input file as object. local_uuid: {0}'.format(task.local_uuid))
        self._s3_client.download_file_as_object(bucket=self._project_bucket,
                                                key=task.input_file_s3_path,
                                                file_object=input_file)

        task_as_dict = json.loads(input_file.getvalue())
        new_task = utils.Task(uuid=task.local_uuid,
                              start_time=task_as_dict['start-date'],
                              end_time=task_as_dict['end-date'],
                              speed_threshold=task_as_dict['speed-threshold'],
                              diameter_threshold=task_as_dict['diameter-threshold'],
                              miss_threshold=task_as_dict['miss-threshold'],
                              n_days=task.days,
                              n=task.n)
        return new_task

    def _download_input_file_and_create_task_from_message(self, message):
        body = message.body
        local_task = messages.Task.decode(body)
        if local_task.terminate:
            self._manager.flag_worker_terminate()
            self._manager.flag_local_terminate()
        self._logger.debug('Manager handling local message. local_uuid: {0}'.format(local_task.local_uuid))
        return self._download_and_parse_input_file(local_task)

    def _create_jobs_and_dispetch_them(self, task):

        # We create a job for each day in the task
        for day in task.days.iterkeys():
            job = messages.Job(start_date=day,
                               end_date=day,
                               local_uuid=task.uuid,
                               speed=task.speed_threshold,
                               diameter=task.diameter_threshold,
                               miss=task.miss_threshold)
            job_message = messages.Job.encode(job)
            self._sqs_client.send_message(queue=self._manager_to_workers_queue,
                                          body=job_message)

    def update_workers_statistics(self, done_job):
        self._workers_statistic[done_job.worker_id]["total_asteroids"] += done_job.total_asteroids
        self._workers_statistic[done_job.worker_id]["num_of_safe"] += done_job.num_of_safe
        self._workers_statistic[done_job.worker_id]["num_of_dangerous"] += done_job.num_of_dangerous

    def _worker_terminate_if_needed(self):
        if self._terminate and len(self._tasks) == 0:

            # Upload workers statistic file
            worker_stat = StringIO.StringIO()
            worker_stat.write(json.dumps(self._workers_statistic, indent=4))
            worker_stat.seek(0)
            self._s3_client.upload_object_as_file(self._project_bucket, 'workers_statistic.json', worker_stat)

            # No more tasks and got terminate flag
            self._logger.debug('No more tasks and got terminate order, exiting')
            exit(0)

    def terminate_local(self):

        self._logger.debug('Local got terminate order')
        self._terminate = True


if __name__ == '__main__':
    manager_instance = Manager()
    manager_instance.run()

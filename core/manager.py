import boto3
import StringIO
import json
import pickle
import thread

import utils
import utils.clients
import messages


class Manager(object):

    def __init__(self):
        self._logger = utils.set_logger('manager')
        self._e2_client = utils.clients.Ec2()
        self._s3_client = utils.clients.S3()
        self._sqs_client = utils.clients.Sqs()
        self._manager_to_local_queue = self._sqs_client.get_or_create_queue(utils.Names.manager_to_local_queue)
        self._local_to_manager_queue = self._sqs_client.get_or_create_queue(utils.Names.local_to_manager_queue)
        self._manager_to_workers_queue = self._sqs_client.get_or_create_queue(utils.Names.manager_to_workers_queue)
        self._workers_to_manager_queue = self._sqs_client.create_queue(utils.Names.worker_to_manager_queue,
                                                                       visibility_timeout=60 * 1)
        self._project_bucket = self._s3_client.create_or_get_bucket(utils.Names.project_bucket_name)
        self._workers = []
        self._needed_number_of_workers = 0

        self._tasks = {}

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

        thread.start_new_thread(self._handle_local_requests)
        # thread.start_new_thread( self._handle_worker_done_jobs)

        # self._handle_local_requests()
        self._handle_worker_done_jobs()

    def _handle_local_requests(self):
        """
        This is the Flow 1 main function
        :return:
        """
        while True:
            self._update_number_of_workers()
            local_messages = self._sqs_client.get_messages(self._local_to_manager_queue,
                                                           timeout=20,
                                                           number_of_messages=1)
            for message in local_messages:
                new_task = self._download_input_file_and_create_task_from_message(message)
                self._tasks[new_task.uuid] = new_task

                # TODO: finish the flow, separate it to smaller function
                self._create_jobs_and_dispetch_them(new_task)
                message.delete()

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
                relevant_task = self._tasks[local_uuid]
                relevant_task.add_asteroid_list(asteroid_list=done_job.asteroids, date=done_job.date)
                if relevant_task.is_done():
                    self._create_summery_file_and_send(relevant_task)
                    del self._tasks[local_uuid]
                message.delete()

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

    def _update_number_of_workers(self):
        if len(self._tasks):
            needed_workers = max([task.number_of_workers for task in self._tasks.itervalues()])
        pass

    def _download_and_parse_input_file(self, task):
        input_file = StringIO.StringIO()
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
                              days=task.days,
                              n=task.n)
        return new_task

    def _download_input_file_and_create_task_from_message(self, message):
        body = message.body
        local_task = messages.Task.decode(body)
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


if __name__ == '__main__':
    manager_instance = Manager()
    manager_instance.run()

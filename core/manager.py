import boto3
import StringIO
import json

import utils
import utils.clients
import messages


class Manager(object):

    def __init__(self):
        self._e2_client = utils.clients.Ec2()
        self._s3_client = utils.clients.S3()
        self._sqs_client = utils.clients.Sqs()
        self._manager_to_local_queue = self._sqs_client.get_or_create_queue(utils.Names.manager_to_local_queue)
        self._local_to_manager_queue = self._sqs_client.get_or_create_queue(utils.Names.local_to_manager_queue)
        self._manager_to_workers_queue = self._sqs_client.create_queue(utils.Names.manager_to_workers_queue,
                                                                       visibility_timeout=60 * 2)
        self._workers_to_manager_queue = self._sqs_client.create_queue(utils.Names.worker_to_manager_queue,
                                                                       visibility_timeout=60 * 1)
        self._project_bucket = self._s3_client.create_or_get_bucket(utils.Names.project_bucket_name)
        self._workers = []

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

        self._handle_local_requests()

    def _handle_local_requests(self):
        """
        This is the Flow 1 main function
        :return:
        """
        while True:
            local_messages = self._sqs_client.get_messages(self._local_to_manager_queue,
                                                           timeout=20,
                                                           number_of_messages=1)
            for message in local_messages:
                new_task = self._download_input_file_and_create_task_from_message(message)
                self._tasks[new_task.uuid] = new_task

                # TODO: finish the flow, separate it to smaller function
                self._create_jobs_and_dispetch_them(new_task)

    def handle_worker_done_jobs(self):
        """
        This is the Flow 2 main function
        :return:
        """
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
        return self._download_and_parse_input_file(local_task)

    def _create_jobs_and_dispetch_them(self, task):

        # We create a job for each day in the task
        for day in task.days.iterkeys():
            pass


if __name__ == '__main__':
    manager_instance = Manager()
    manager_instance.run()

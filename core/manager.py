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

        self._tasks = {}

    def run(self):
        self._handle_locals()

    def _handle_locals(self):
        while True:
            local_messages = self._sqs_client.get_messages(self._local_to_manager_queue,
                                                           timeout=20,
                                                           number_of_messages=1)
            for message in local_messages:
                body = message.body
                local_task = messages.Task.decode(body)
                self._download_and_parse_input_file(local_task)

    def handle_workers(self):
        pass

    def add_new_task(self):
        pass

    def _download_and_parse_input_file(self, task):
        input_file = StringIO.StringIO()
        self._s3_client.download_file_as_object(bucket=self._project_bucket,
                                                key=task.input_file_s3_path,
                                                file_object=input_file)
        task_as_dict = json.loads(input_file.getvalue())
        new_task = utils.Task(uuid=task.local_uuid,
                              start_time=task_as_dict['start-date'],
                              end_time=task_as_dict['end-time'],
                              speed_threshold=task_as_dict['speed-threshold'],
                              diameter_threshold=task_as_dict['diameter-threshold'],
                              miss_threshold=task_as_dict['miss-threshold'])
        self._tasks[task.local_uuid] = new_task


if __name__ == '__main__':
    manager_instance = Manager()
    manager_instance.run()

import boto3

import utils
import utils.clients


class Manager(object):

    def __init__(self):
        self._e2_client = utils.clients.Ec2()
        self._s3_client = utils.clients.S3()
        self._sqs_client = utils.clients.Sqs()
        self._manager_to_local_queue = self._sqs_client.get_or_create_queue(utils.Names.manager_to_local_queue)
        self._manager_to_workers_queue = self._sqs_client.create_queue(utils.Names.manager_to_workers_queue,
                                                                       visibility_timeout=60 * 2)
        self._workers_to_manager_queue = self._sqs_client.create_queue(utils.Names.worker_to_manager_queue,
                                                                       visibility_timeout=60 * 1)
        self._tasks = {}



if __name__ == '__main__':

    ######  FOR DEBUGGING ONLY FOR NOW!! #####
    ec2_client = utils.clients.Ec2('us-east-1')
    sqs = boto3.resource('sqs',  region_name='us-east-1', aws_access_key_id = "AKIAJRJLQHBZH3PC7QUQ", aws_secret_access_key = "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")
    queue = sqs.create_queue(QueueName='test')

    queue = sqs.get_queue_by_name(QueueName='test')
    queue.delete()
    print(queue.url)


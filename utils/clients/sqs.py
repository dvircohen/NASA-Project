import boto3

import utils


class Sqs(object):
    def __init__(self, region_name='us-east-1'):
        self._sqs = boto3.resource('sqs',  region_name=region_name)
        self._logger = utils.set_logger('sqs_client')

    def get_queue(self, queue_name):
        queue = self._sqs.get_queue_by_name(QueueName=queue_name)

    def create_queue(self, queue_name, visibility_timeout=30 * 60):
        """
        :param queue_name: name of the queue
        :param visibility_timeout: number of seconds for a message to be in in-filght mode.
        :return: queue object
        """
        return self._sqs.create_queue(QueueName=queue_name, VisibilityTimeout=visibility_timeout)

    def delete_quque(self, queue):
        queue.delete()

    def get_messages(self, queue, timeout, number_of_messages):
        return queue.receive_messages(MaxNumberOfMessages=number_of_messages,
                                      WaitTimeSeconds=timeout)

    def send_message(self):
        # TODO: finish this
        pass

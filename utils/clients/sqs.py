import boto3

import utils


class Sqs(object):
    def __init__(self, region_name='us-east-1'):
        self._sqs = boto3.resource('sqs', region_name='us-east-1', aws_access_key_id="AKIAJRJLQHBZH3PC7QUQ",
                             aws_secret_access_key="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")
        self._logger = utils.set_logger('sqs_client')

    def get_queue(self, queue_name):
        queue = None
        self._logger.debug('Getting queue. queue name: {0}'.format(queue_name))
        try:
            queue = self._sqs.get_queue_by_name(QueueName=queue_name)
        except Exception as e:
            self._logger.debug('Queue does not exist. queue_name: {0}'.format(queue_name))
            raise e
        return queue

    def create_queue(self, queue_name, visibility_timeout=2):
        """
        :param queue_name: name of the queue
        :param visibility_timeout: number of seconds for a message to be in in-filght mode.
        :return: queue object
        """
        self._logger.debug('Creating queue. queue_name: {0}, visibility_timeout: {1}'.format(queue_name,
                                                                                             visibility_timeout))
        return self._sqs.create_queue(QueueName=queue_name,
                                      Attributes={'VisibilityTimeout': str(visibility_timeout)})

    def get_or_create_queue(self, queue_name):
        try:
            queue = self.get_queue(queue_name)
        except:
            queue = self.create_queue(queue_name)
        return queue

    def delete_quque(self, queue):
        queue.delete()

    def get_messages(self, queue, timeout, number_of_messages):
        return queue.receive_messages(MaxNumberOfMessages=number_of_messages,
                                      WaitTimeSeconds=timeout)

    def send_message(self, queue, body, attributes=None):
        self._logger.debug('Sending message. body: {0}, queue: {1}, attributes" {2} '.format(body, queue, attributes))
        try:
            if attributes is None:
                queue.send_message(MessageBody=body)
            else:
                queue.send_message(MessageBody=body, MessageAttributes=attributes)
        except Exception as e:
            self._logger.error('Error sending message. Error: {0}'.format(e.message))
            raise e
        self._logger.debug('Message sent successfully')

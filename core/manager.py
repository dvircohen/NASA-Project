import boto3
import utils.clients

if __name__ == '__main__':

    ######  FOR DEBUGGING ONLY FOR NOW!! #####
    ec2_client = utils.clients.Ec2('us-east-1')
    sqs = boto3.resource('sqs',  region_name='us-east-1', aws_access_key_id = "AKIAJRJLQHBZH3PC7QUQ", aws_secret_access_key = "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")
    queue = sqs.create_queue(QueueName='test')

    queue = sqs.get_queue_by_name(QueueName='test')
    queue.delete()
    print(queue.url)


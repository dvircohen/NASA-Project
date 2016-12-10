import json


class DoneTask(object):
    """
    The Task object is the object that is passed from the local to the manager
    """
    def __init__(self, summery_file_s3_key):
        self.summery_file_s3_key = summery_file_s3_key

    @staticmethod
    def encode(done_task):
        if isinstance(done_task, DoneTask):
            return json.dumps(done_task.__dict__)

        # If the instance is not of type Task raise
        raise TypeError('Expected object of type DoneTask, got object of type {0}'.format(done_task.__class__.__name__))

    @staticmethod
    def decode(done_task):
        try:
            done_task = json.loads(done_task)

            # unpacking dict as kargs
            return DoneTask(**done_task)
        except Exception as e:
            raise TypeError('Error parsing the json')

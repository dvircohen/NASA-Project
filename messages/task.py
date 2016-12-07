import json


class Task(object):
    """
    The Task object is the object that is passed from the local to the manager
    """
    def __init__(self, input_file_s3_path, local_uuid, days, n):
        self.input_file_s3_path = input_file_s3_path
        self.local_uuid = local_uuid
        self.days = days
        self.n = n

    @staticmethod
    def encode(task):
        if isinstance(task, Task):
            return json.dumps(task.__dict__)

        # If the instance is not of type Task raise
        raise TypeError('Expected object of type Task, got object of type {0}'.format(task.__class__.__name__))

    @staticmethod
    def decode(task):
        try:
            task = json.loads(task)

            # unpacking dict as kargs
            return Task(**task)
        except Exception as e:
            raise TypeError('Error parsing the json')

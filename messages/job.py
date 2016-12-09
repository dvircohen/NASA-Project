import json


class Job(object):
    """
    The Job object is the object that is passed from the manager to the worker
    """
    def __init__(self, start_date, end_date, local_uuid, speed, diameter, miss):
        self.start_date = start_date
        self.end_date = end_date
        self.local_uuid = local_uuid
        self.speed = speed
        self.diameter = diameter
        self.miss = miss

    @staticmethod
    def encode(job):
        if isinstance(job, Job):
            return json.dumps(job.__dict__)

        # If the instance is not of type Task raise
        raise TypeError('Expected object of type Task, got object of type {0}'.format(job.__class__.__name__))

    @staticmethod
    def decode(job):
        try:
            job = json.loads(job)

            # unpacking dict as kargs
            return Job(**job)
        except Exception as e:
            raise TypeError('Error parsing the json')

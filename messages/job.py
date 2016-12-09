import json


class Job(object):
    """
    The Job object is the object that is passed from the manager to the worker
    """
    def __init__(self, start_date, end_date, local_uuid, speed, diameter, miss):
        self._start_date = start_date
        self._end_date = end_date
        self._local_uuid = local_uuid
        self._speed = speed
        self._diameter = diameter
        self._miss = miss

    @property
    def start_date(self):
        return self._start_date

    @property
    def end_date(self):
        return self._end_date

    @property
    def local_uuid(self):
        return self._local_uuid

    @property
    def speed(self):
        return self._speed

    @property
    def diameter(self):
        return self._diameter

    @property
    def miss(self):
        return self._miss

    @staticmethod
    def encode(job):
        if isinstance(job, Job):
            return json.dumps(task.__dict__)

        # If the instance is not of type Task raise
        raise TypeError('Expected object of type Task, got object of type {0}'.format(task.__class__.__name__))

    @staticmethod
    def decode(task):
        try:
            task = json.loads(task)

            # unpacking dict as kargs
            return Job(**task)
        except Exception as e:
            raise TypeError('Error parsing the json')



class DoneJob(object):
    def __init__(self, local_uuid, date, asteroids):
        self._local_uuid = local_uuid
        self._asteroids = asteroids
        self._date = date

    @property
    def local_uuid(self):
        return self._local_uuid

    @property
    def asteroids(self):
        return self._asteroids

    @property
    def date(self):
        return self._date

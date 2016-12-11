

class DoneJob(object):
    def __init__(self, local_uuid, date, asteroids, worker_id, total_asteroids):
        self._local_uuid = local_uuid
        self._asteroids = asteroids
        self._date = date
        self._worker_id = worker_id
        self._total_asteroids = total_asteroids
        self._num_of_dangerous = len(self._asteroids) if self._asteroids is not None else 0
        self._num_of_safe = self.total_asteroids - self.num_of_dangerous

    @property
    def worker_id(self):
        return self._worker_id

    @property
    def total_asteroids(self):
        return self._total_asteroids

    @property
    def num_of_dangerous(self):
        return self._num_of_dangerous

    @property
    def num_of_safe(self):
        return self._num_of_safe

    @property
    def local_uuid(self):
        return self._local_uuid

    @property
    def asteroids(self):
        return self._asteroids

    @property
    def date(self):
        return self._date

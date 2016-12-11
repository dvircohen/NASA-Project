import datetime
import json


class Task(object):
    def __init__(self, uuid, start_time, end_time, speed_threshold, diameter_threshold, miss_threshold, n_days, n):
        self._uuid = uuid
        self._speed_threshold = speed_threshold
        self._diameter_threshold = diameter_threshold
        self._miss_threshold = miss_threshold
        self._days = {}
        self._n_days = n_days
        start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d')
        end_time = datetime.datetime.strptime(end_time, '%Y-%m-%d')
        delta = end_time - start_time
        periods = delta.days / self._n_days
        self._number_of_workers = periods / n
        self._number_of_workers = max([self._number_of_workers, 1])
        for i in range(delta.days + 1):
            self._days[(start_time + datetime.timedelta(days=i)).strftime('%Y-%m-%d')] = None

    @property
    def uuid(self):
        return self._uuid

    @property
    def number_of_workers(self):
        return self._number_of_workers

    @property
    def days(self):
        return self._days

    @property
    def speed_threshold(self):
        return self._speed_threshold

    @property
    def diameter_threshold(self):
        return self._diameter_threshold

    @property
    def miss_threshold(self):
        return self._miss_threshold

    def add_asteroid_list(self, asteroid_list, date):
        self._days[date] = asteroid_list

    def is_done(self):
        return all(day is not None for day in self._days.values())

    def get_how_many_days_left(self):
        return self._days.values().count(None)

    def make_json(self):
        # json.dumps(self.days)
        return json.dumps(self._days, default=lambda o: o.__dict__, sort_keys=True, indent=4)

import datetime
import json


class Task(object):
    def __init__(self, uuid, start_time, end_time, speed_threshold, diameter_threshold, miss_threshold, days, n):
        self._uuid = uuid
        self._speed_threshold = speed_threshold
        self._diameter_threshold = diameter_threshold
        self._miss_threshold = miss_threshold
        self.days = {}
        start_time = datetime.datetime.strptime(start_time, '%d-%m-%Y')
        end_time = datetime.datetime.strptime(end_time, '%d-%m-%Y')
        delta = end_time - start_time
        periods = delta.days / days
        self._number_of_workers = n * periods
        for i in range(delta.days + 1):
            self.days[(start_time + datetime.timedelta(days=i)).strftime('%d-%m-%Y')] = None
        pass

    @property
    def uuid(self):
        return self._uuid

    @property
    def number_of_workers(self):
        return self._number_of_workers

    def add_asteroid_list(self, asteroid_list, date):
        self.days[date.strftime('%d-%m-%Y')] = asteroid_list

    def is_done(self):
        return all(day is not None for day in self.days.values())

    def make_json(self):
        # json.dumps(self.days)
        return json.dumps(self.days, default=lambda o: o.__dict__, sort_keys=True, indent=4)

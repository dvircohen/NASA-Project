import datetime
import json


class Task(object):
    def __init__(self, uuid, start_time, end_time, speed_threshold, diameter_threshold, miss_threshold):
        self._uuid = uuid
        self._speed_threshold = speed_threshold
        self._diameter_threshold = diameter_threshold
        self._miss_threshold = miss_threshold
        self.days = {}
        delta = end_time - start_time
        for i in range(delta.days + 1):
            self.days[start_time + datetime.timedelta(days=i)] = None
        pass

    def add_asteroid_list(self, asteroid_list, date):
        self.days[date] = asteroid_list

    def is_done(self):
        return all(day is not None for day in self.days.values())

    def make_json(self):
        json.dumps(self.days)

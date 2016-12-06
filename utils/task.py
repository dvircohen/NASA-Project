import datetime


class Task(object):
    def __init__(self, uuid, start_time, end_time, speed_threshold, diameter_threshold, miss_threshold):
        self._uuid = uuid
        self._speed_threshold = speed_threshold
        self._diameter_threshold = diameter_threshold
        self._miss_threshold = miss_threshold

        delta = end_time - start_time

        for i in range(delta.days + 1):
            print start_time + datetime.td(days=i)

import itertools
import json

import datetime
import urllib2
import uuid

import requests
import pickle

import messages
from messages.job import Job
from utils import make_asteroid_from_json
from utils.asteroid import Asteroid
from utils.clients import *
from utils.task import Task


class Worker(object):
    def __init__(self):
        self._sqs_client = boto3.resource("sqs", region_name='us-east-1')
        self._logger = utils.set_logger('worker')
        self._sqs_client = Sqs()
        self.jobs_queue = self._sqs_client.get_queue("jobs")
        self.asteroids_queue = self._sqs_client.get_queue("asteroids")
        self.asteroids_count = 0
        self._uuid = str(uuid.uuid4())
        self.nasa_api_key = "wPGgYuyy7uuIsdsydcMMTeaTV2Td4GpJKmAXVZzr"

        self.start_listening()

        pass

    def start_listening(self):
        """
        get the messages from the queue and precces them
        """
        self._logger.debug('Waiting to get Messages from manager')
        while True:
            # self._check_if_kill_yourself()
            messages_from_manager = self._sqs_client.get_messages(queue=self.jobs_queue,
                                                                  timeout=20,
                                                                  number_of_messages=1)
            for message in messages_from_manager:
                self._logger.debug('Message received from manager')
                job = Job.decode(message.body)

                # process the messages and return a json str
                string_ast_list = self.process_message(job)

                # send it back to the manager
                self._logger.debug('Sending the asteroids list to the manager')
                for string in string_ast_list:
                    self.send_asteroids(string)
                self._logger.debug('Deleting the message')
                message.delete()

    def process_message(self, job):
        """
        get a message and returns the dangerous asteroids list to to queue
        """
        msg_start_date = job.start_date
        msg_end_date = job.end_date
        msg_diameter = job.diameter
        msg_speed = job.speed
        msg_miss = job.miss

        ast_dict = self.get_list_of_asteroids(msg_start_date,
                                              msg_end_date,
                                              msg_diameter,
                                              msg_speed,
                                              msg_miss)

        strings_list = []

        # decode to json
        for date, day in ast_dict.items():
            if len(day) is 0:
                asteroids = []
            else:
                asteroids = day
            result = messages.DoneJob(local_uuid=job.local_uuid,
                                      date=date,
                                      asteroids=asteroids,
                                      worker_id=self._uuid,
                                      total_asteroids=self.asteroids_count)
            result = pickle.dumps(result)
            strings_list.append(result)
        return strings_list

    def send_asteroids(self, json_ast_list):
        """
        send the asteroids to the manager
        """
        self._sqs_client.send_message(queue=self.asteroids_queue, body=json_ast_list)

    def get_list_of_asteroids(self, start_date_str, end_date_str, msg_diameter, msg_speed, msg_miss):
        """
        get the dates and return an asteroids object from NASA
        :return: json objects representing the asteroids list
        """
        self._logger.debug('Getting the data from NASA')
        response = requests.get(
            'https://api.nasa.gov/neo/rest/v1/feed?start_date={}&end_date={}&api_key={}'.format(start_date_str,
                                                                                                end_date_str,
                                                                                                self.nasa_api_key)).json()
        data_per_day_dict = {i: response["near_earth_objects"][i] for i in response["near_earth_objects"].keys()}

        self.asteroids_count = 0  # start counting how many asteroids you got
        dangerous_asteroids_dict = {}
        self._logger.debug('Processing the data')
        for date, day in data_per_day_dict.items():

            # make list of asteroid object
            nasa_asteroids_list = [self._make_asteroid_object(asteroid) for asteroid in day]
            self.asteroids_count += len(nasa_asteroids_list)

            # remove the none dangerous and add the color
            dangerous_asteroids = [asteroid for asteroid in nasa_asteroids_list if
                                   self._check_if_dangerous(asteroid, msg_diameter, msg_speed, msg_miss)]
            dangerous_asteroids_dict[date] = dangerous_asteroids
        return dangerous_asteroids_dict

    @staticmethod
    def _make_asteroid_object(asteroid):
        """
        :param asteroid: a dict representing the asteroid
        :return: a an Asteroid object representing the asteroid without all the unnecessary data
        """
        hazardous = asteroid["is_potentially_hazardous_asteroid"]  # bool
        miss_distance = asteroid["close_approach_data"][0]["miss_distance"]["astronomical"]  # int
        velocity = asteroid["close_approach_data"][0]["relative_velocity"]["kilometers_per_second"]  # int
        diameter_min = asteroid["estimated_diameter"]["meters"]["estimated_diameter_min"]  # int
        diameter_max = asteroid["estimated_diameter"]["meters"]["estimated_diameter_max"]  # int
        name = asteroid["name"]  # str
        approach_date = asteroid["close_approach_data"][0]["close_approach_date"]  # str
        return Asteroid(hazardous, miss_distance, velocity, diameter_min, diameter_max, name, approach_date)

    @staticmethod
    def _check_if_dangerous(asteroid, msg_diameter, msg_speed, msg_miss):
        if asteroid.get_hazardous():
            if asteroid.get_velocity() > msg_speed:
                asteroid.set_color("Green")
                if asteroid.get_diameter_min() > msg_diameter:
                    asteroid.set_color("Yellow")
                    if asteroid.get_miss_distance() > msg_miss:
                        asteroid.set_color("Red")
                return True
        else:
            return False

    def _check_if_kill_yourself(self):
        messages = self._sqs_client.get_messages(queue=self.death_queue,
                                                 timeout=3,
                                                 number_of_messages=1)
        self._logger('Message received from the death queue')
        message_body = messages[0]
        if message_body is not None:

            # message received saying you need to kill yourself :(
            self.kil_yourself()

    def kil_yourself(self):
        self._logger('Goodbye cruel world  :(')
        quit()


if __name__ == '__main__':
    worker_instance = Worker()

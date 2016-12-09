import itertools
import json

import datetime
import requests

import messages
from messages.job import Job
from utils import make_asteroid_from_json
from utils.asteroid import Asteroid
from utils.clients import *
from utils.task import Task


class Worker(object):
    def __init__(self):
        self._sqs_client = boto3.resource("sqs", region_name='us-east-1', aws_access_key_id="AKIAJRJLQHBZH3PC7QUQ",
                             aws_secret_access_key="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")
        # self.queue = _sqs_client.get_queue_by_name(QueueName='worker_queue')
        self._logger = utils.set_logger('worker')
        self._sqs_client = Sqs()
        self.jobs_queue = self._sqs_client.get_queue("jobs")
        # self.death_queue = self._sqs_client.get_queue("deaths")
        self.asteroids_queue = self._sqs_client.get_queue("asteroids")
        self.start_listening()

    def start_listening(self):
        """
        get the messages from the queue and precces them
        """
        # TODO find a way to kill a worker
        while True:
            # self._check_if_kill_yourself()
            messages_from_manager = self._sqs_client.get_messages(queue=self.jobs_queue,
                                                                  timeout=20,
                                                                  number_of_messages=1)
            for message in messages_from_manager:
                self._logger.debug('Message received from manager')
                job = Job.decode(message.body)
                # process the messages and return a json str
                json_ast_list = self.process_message(job)
                # send it back to the manager
                self._logger.debug('Sending the asteroids list to the manager')
                self.send_asteroids(json_ast_list)
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
        msg_local_uuid = job.local_uuid

        ast_list = self.get_list_of_asteroids(msg_start_date,
                                              msg_end_date,
                                              msg_local_uuid,
                                              msg_diameter,
                                              msg_speed,
                                              msg_miss)

        # decode to json
        json_ast_list = [x.__dict__ for x in ast_list]
        json_ast_list = json.dumps(json_ast_list)
        return json_ast_list

    def send_asteroids(self, json_ast_list):
        """
        send the asteroids to the manager
        """
        self._sqs_client.send_message(queue=self.asteroids_queue, body=json_ast_list)

    def get_list_of_asteroids(self, start_date_str, end_date_str, msg_local_uuid, msg_diameter, msg_speed, msg_miss):
        """
        get the dates and return an asteroids object from NASA
        :return: json objects representing the asteroids list
        """
        nasa_api_key = "wPGgYuyy7uuIsdsydcMMTeaTV2Td4GpJKmAXVZzr"
        response = requests.get(
            'https://api.nasa.gov/neo/rest/v1/feed?start_date={}&end_date={}&api_key={}'.format(start_date_str,
                                                                                                end_date_str,
                                                                                                nasa_api_key)).json()
        data_per_day_list = [response["near_earth_objects"][i] for i in response["near_earth_objects"].keys()]
        asteroids_list = list(itertools.chain.from_iterable(data_per_day_list))
        # make list of asteroid object
        nasa_asteroids_list = [self._make_asteroid_object(asteroid, msg_local_uuid) for asteroid in asteroids_list]
        # remove the none dangerous and add the color
        dangerous_asteroids = [asteroid for asteroid in nasa_asteroids_list if
                               self._check_if_dangerous(asteroid, msg_diameter, msg_speed, msg_miss)]
        # add the local id
        return dangerous_asteroids

    @staticmethod
    def _make_asteroid_object(asteroid, msg_local_uuid):
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
        return Asteroid(hazardous, miss_distance, velocity, diameter_min, diameter_max, name, approach_date, msg_local_uuid)

    @staticmethod
    def _check_if_dangerous(asteroid, msg_diameter, msg_speed, msg_miss):
        if asteroid.get_hazardous():
            if asteroid.get_velocity() < msg_speed:
                asteroid.set_color("Green")
            else:
                if asteroid.get_diameter_max() < msg_diameter:
                    asteroid.set_color("Yellow")
                else:
                    if asteroid.get_miss_distance() < msg_miss:
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
        if  messages[0] is not None:
            # message received saying you need to kill yourself :(
            self.kil_yourself()

    def kil_yourself(self):
        self._logger('Goodbye cruel world  :(')
        quit()
        # TODO suicide


# FOR DEBUGGING
start_date = "2016-11-19"
end_date = "2016-11-26"
api_key = "wPGgYuyy7uuIsdsydcMMTeaTV2Td4GpJKmAXVZzr"
local_id = "worker1"
worker = Worker()
data = worker.get_list_of_asteroids(start_date, end_date, local_id, 200, 10, 0.3)
json_ast_list = json.dumps(json_ast_list)
task = json.loads(json_ast_list)
start = datetime.datetime.strptime("2016-11-12", '%Y-%m-%d')
end = datetime.datetime.strptime("2016-11-19", '%Y-%m-%d')

# a = Task(None, start, end, None, None, None)
# a.add_asteroid_list(data, start)
# json1 = a.make_json()
# ast1 = make_asteroid_from_json(json1)
pass

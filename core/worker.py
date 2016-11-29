import itertools

import boto3
import requests

from utils import make_asteroid_from_json
from utils.asteroid import Asteroid
from utils.clients import Sqs


class Worker(object):
    def __init__(self):
        # sqs = boto3.resource('sqs', region_name='us-east-1', aws_access_key_id="AKIAJRJLQHBZH3PC7QUQ",
        #                      aws_secret_access_key="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")
        # self.queue = sqs.get_queue_by_name(QueueName='worker_queue')

        self.sqs = Sqs()
        self.queue = sqs.get_queue("worker_queue")
        self.start_listening()

    def start_listening(self):
        """
        get the messages from the queue and precces them
        """
        for message in self.sqs.get_messages(self.queue, 5, 5):
            if message.message_attributes is not None:
                if message.message_attributes.get('type').get('StringValue') is "incoming":
                    self.procces_message(message)

    def procces_message(self, message):
        """
        get a message and returns the dangerous asteroids list to to queue
        """
        # TODO get other parameter, and the local id
        msg_start_date = message.message_attributes.get('start_date').get('StringValue')
        msg_end_date = message.message_attributes.get('end_date').get('StringValue')
        local_id = message.message_attributes.get('local_id').get('StringValue')
        ast_list = self.get_list_of_asteroids(msg_start_date, msg_end_date)
        # TODO send back to the queue the astroid json and the local id
        json_ast_list = [x.to_json() for x in ast_list]
        self.send_asteroids(json_ast_list)

    def send_asteroids(self, json_ast_list):
        for ast in json_ast_list:
            self.sqs.send_message(self.queue, "astroid", ast)


    def get_list_of_asteroids(self, start_date_str, end_date_str):
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
        nasa_asteroids_list = [self._make_asteroid_object(asteroid) for asteroid in asteroids_list]
        dangerous_asteroids = [asteroid for asteroid in nasa_asteroids_list if self.check_if_dangerous(asteroid)]
        return dangerous_asteroids

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
    def check_if_dangerous(asteroid):
        if asteroid.get_hazardous():
            if asteroid.get_velocity() < 10:
                asteroid.set_color("Green")
            else:
                if asteroid.get_diameter_max() < 200:
                    asteroid.set_color("Yellow")
                else:
                    if asteroid.get_miss_distance() < 0.3:
                        asteroid.set_color("Red")

            return True
        else:
            return False



# FOR DEBUGGING
start_date = "2016-11-19"
end_date = "2016-11-26"
api_key = "wPGgYuyy7uuIsdsydcMMTeaTV2Td4GpJKmAXVZzr"
worker = Worker()
data = worker.get_list_of_asteroids(start_date, end_date)
json1 = data[0].to_json()
ast1 = make_asteroid_from_json(json1)
pass

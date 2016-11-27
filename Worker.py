import itertools
import requests

from Asteroid import Asteroid


class Worker(object):
    def __init__(self):
        pass

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
        return nasa_asteroids_list

    @staticmethod
    def _make_asteroid_object(asteroid):
        """
        :param asteroid: a dict representing the asteroid
        :return: a an Asteroid object representing the asteroid without all the unnecessary data
        """
        hazardous = asteroid["is_potentially_hazardous_asteroid"]  # bool
        miss_distance = asteroid["close_approach_data"][0]["miss_distance"]["kilometers"]  # int
        velocity = asteroid["close_approach_data"][0]["relative_velocity"]["kilometers_per_second"]  # int
        diameter_min = asteroid["estimated_diameter"]["kilometers"]["estimated_diameter_min"]  # int
        diameter_max = asteroid["estimated_diameter"]["kilometers"]["estimated_diameter_max"]  # int
        name = asteroid["name"]  # str
        approach_date = asteroid["close_approach_data"][0]["close_approach_date"]  # str
        return Asteroid(hazardous, miss_distance, velocity, diameter_min, diameter_max, name, approach_date)


start_date = "2016-11-25"
end_date = "2016-11-26"
api_key = "wPGgYuyy7uuIsdsydcMMTeaTV2Td4GpJKmAXVZzr"
worker = Worker()
data = worker.get_list_of_asteroids(start_date, end_date)
pass

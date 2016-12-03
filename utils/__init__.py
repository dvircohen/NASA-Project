import json
import logging

from utils.asteroid import Asteroid
from utils.task import Task


class Names(object):
    local_to_manager_queue = 'tasks'
    manager_to_local_queue = 'done_tasks'
    worker_to_manager_queue = "asteroids"
    workers_death_queue = "deaths"
    arn = 'arn:aws:iam::673333208134:instance-profile/manager'
    project_bucket_name = '673333208134-very-secret-do-not-enter'


def set_logger(logger_name, logger_file=None):

    # create logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    # create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    # add the handler to the logger
    logger.addHandler(console_handler)

    # handle file logger
    if logger_file is not None:
        file_handler = logging.FileHandler(logger_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger


def make_asteroid_from_json(json_string):
    json_string = json.loads(json_string)
    hazardous = json_string["hazardous"]
    miss_distance = json_string["miss_distance"]
    velocity = json_string["velocity"]
    diameter_min = json_string["diameter_min"]
    diameter_max = json_string["diameter_max"]
    name = json_string["name"]
    approach_date = json_string["approach_date"]
    color = json_string["color"]
    ast = Asteroid(hazardous, miss_distance, velocity, diameter_min, diameter_max, name, approach_date)
    ast.set_color(color)
    return ast



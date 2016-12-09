import json


class Asteroid(object):

    def __init__(self, hazardous, miss_distance, velocity, diameter_min, diameter_max, name, approach_date, local_uuid):
        self.hazardous = hazardous
        self.miss_distance = float(miss_distance)
        self.velocity = float(velocity)
        self.diameter_min = float(diameter_min)
        self.diameter_max = float(diameter_max)
        self.name = name
        self.approach_date = approach_date
        self.color = "Black"
        self.local_id = local_uuid

    def get_hazardous(self):
        return self.hazardous

    def get_miss_distance(self):
        return self.miss_distance

    def get_velocity(self):
        return self.velocity

    def get_diameter_min(self):
        return self.diameter_min

    def get_diameter_max(self):
        return self.diameter_min

    def get_name(self):
        return self.name

    def get_approach_date(self):
        return self.approach_date

    def get_color(self):
        return self.color

    def set_color(self, color):
        self.color = color

    def set_local_id(self, local_id):
        self.local_id = local_id

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    def decode(self):
        # TODO make json
        pass


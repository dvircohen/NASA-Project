import boto3


class Ec2(object):

    def __init__(self):
        self._ec2 = boto3.resource('ec2')

    def get_instance_with_tag(self, wanted_tag):
        """
        returns a list with of instances that fits the given tag
        :param wanted_tag: {key: value}
        :return: return a list of instances that has the wanted tags
        """
        instances = []

        # iterate over all alive instances
        for instance in self._ec2.instances.all():
            for tag in instance.tags():
                if tag == wanted_tag:
                    instances.append(instance)

        return instances

    def create_instances(self, instances):
        """
        Create wanted instances
        :param instances: List of instances that should include kargs:
        ImageId, MinCount, MaxCount, InstanceType, Tags,
        :return: list of creates instances
        """
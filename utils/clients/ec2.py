import boto3

import utils


class Ec2(object):

    def __init__(self, region_name='us-east-1', logger=None):
        self._ec2 = boto3.resource('ec2', region_name=region_name)
        if logger is None:
            self._logger = utils.set_logger('ec2_client')
        else:
            self._logger = logger.getChild('ec2_client')

    def get_instance_with_tag(self, wanted_tag):
        """
        returns a list with of instances that fits the given tag
        :param wanted_tag: {key: value}
        :return: return a list of instances that has the wanted tags
        """

        self._logger.debug('Searching instances with a given tag. tag: {0}'.format(wanted_tag))
        instances = []
        log_message = 'Not instances found'

        # iterate over all alive instances
        for instance in self._ec2.instances.all():
            if instance.state['Name'] == 'running' and instance.tags is not None:
                for tag in instance.tags:
                    if tag == wanted_tag:
                        instances.append(instance)

        if instances:
            log_message = 'Instances found'
        self._logger.debug(log_message)
        return instances

    def create_instance(self,
                        image_id,
                        tags,
                        iam_instance_profile,
                        user_data,
                        instance_type='t2.nano',
                        min_count=1,
                        max_count=1,
                        instance_initiated_shutdown_behavior='stop'):

        # Create the instances
        self._logger.debug('Creating instances')
        instances = self._ec2.create_instances(ImageId=image_id,
                                               MinCount=min_count,
                                               MaxCount=max_count,
                                               InstanceType=instance_type,
                                               IamInstanceProfile=iam_instance_profile,
                                               UserData=user_data,
                                               KeyName="test_key",
                                               InstanceInitiatedShutdownBehavior=instance_initiated_shutdown_behavior)

        # Add tags to the instances
        self._logger.debug('Instances created. Number of instances: {0}'.format(len(instances)))
        self._logger.debug('Setting tags to instances')
        for instance in instances:
            instance.create_tags(Tags=tags)

        return instances

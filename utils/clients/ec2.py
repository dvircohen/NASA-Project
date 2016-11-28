import boto3


class Ec2(object):

    def __init__(self, region_name='us-east-1'):
        self._ec2 = boto3.resource('ec2', region_name=region_name)

    def get_instance_with_tag(self, wanted_tag):
        """
        returns a list with of instances that fits the given tag
        :param wanted_tag: {key: value}
        :return: return a list of instances that has the wanted tags
        """
        instances = []

        # iterate over all alive instances
        for instance in self._ec2.instances.all():
            if instance.state['Name'] == 'running' and instance.tags is not None:
                for tag in instance.tags:
                    if tag == wanted_tag:
                        instances.append(instance)

        return instances

    def create_instance(self,
                        image_id,
                        tags,
                        iam_instance_profile,
                        user_data,
                        instance_type='t2.nano',
                        min_count=1,
                        max_count=1):

        # Create the instances
        instances = self._ec2.create_instances(ImageId=image_id,
                                               MinCount=min_count,
                                               MaxCount=max_count,
                                               InstanceType=instance_type,
                                               IamInstanceProfile=iam_instance_profile,
                                               UserData=user_data)

        # Add tags to the instances
        for instance in instances:
            instance.create_tags(Tags=tags)

        return instances

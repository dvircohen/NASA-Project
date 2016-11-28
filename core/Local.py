import boto3

import utils.clients

class Local(object):

    def __init__(self, code, input_file_local_path, output_file_path, n, d):
        self._code = code
        self._input_file_local_path = input_file_local_path
        self._output_file_path = output_file_path
        self._number_of_days_per_period = d
        self._number_periods_per_worker = n
        self._input_file_s3_path = None
        self._SQS = None
        self._ec2_client = utils.clients.Ec2()
        self._manager = None

    def _ensure_manager_is_up(self):
        """
        Checks if the manager is up. if not, set one up
        :return:
        """

        if not self._ec2_client.get_instance_with_tag({'Rule': 'Manager'}):

            # TODO: create manager here
            pass

    def _upload_file_and_job(self):
        """
        Upload the input file to S3
        Send a message to the SQS with the S3 path
        :return:
        """

    def _on_summery_file_ready(self, summery_file_s3_path):
        """
        Downloads summery file from s3
        Create summery html file and save it in place
        :param summery_file_s3_path:
        :return:
        """

    def _kill_manager(self):
        """
        Send termention message to the manager
        :return:
        """



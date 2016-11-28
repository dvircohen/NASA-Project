import argparse
import os

import utils.clients


class Local(object):

    def __init__(self, args):
        # TODO: zip code here
        self._args = args
        self._code = None
        self._input_file_local_path = args.input_file_name
        self._output_file_path = args.output_file_name
        self._number_of_days_per_period = args.d
        self._number_periods_per_worker = args.n
        self._terminate = args.terminate
        self._input_file_s3_path = None
        self._SQS = None
        self._ec2_client = utils.clients.Ec2()
        self._manager = None
        self._manager_tag = {'Key': 'Role', 'Value': 'Manager'}
        self._project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',))
        self._setup_script_path = os.path.join(self._project_path, 'setup_scripts/manager_setup.sh')

    def run_task(self):
        """
        Used to start the task
        :return:
        """
        # TODO: make sure manager is up
        # run the task given in constructor
        # create the html file and finish
        self._ensure_manager_is_up()
        self._kill_manager()

    def _ensure_manager_is_up(self):
        """
        Checks if the manager is up. if not, set one up
        :return:
        """

        manager = self._ec2_client.get_instance_with_tag(self._manager_tag)
        if manager:

            # There should be only one manager so the list should have only one element
            self._manager = manager[0]
        else:

            # No manager up, create a new one (with tag)
            iam_instance_profile = {'Arn': 'arn:aws:iam::673333208134:instance-profile/manager'}
            self._manager = self._ec2_client.create_instance(image_id='ami-b66ed3de',
                                                             tags=[self._manager_tag],
                                                             iam_instance_profile=iam_instance_profile,
                                                             instance_type='t2.nano',
                                                             min_count=1,
                                                             max_count=1,
                                                             user_data=self._setup_script_path)

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
        Terminate the manager
        :return:
        """
        # TODO: need to check the assagiment specefication, maybe we need to let the manager finish all it current
        # tasks before killing it
        if self._terminate:
            response = self._manager.terminate()


def _register_arguments(parser):

    # register argument
    parser.add_argument(
            '-i', '--input-file-name',
            help='input file to use',
            type=str,
            required=True)

    parser.add_argument(
            '-o', '--output-file-name',
            help='path to put the output file in',
            type=str,
            required=True)

    parser.add_argument(
            '-n',
            help='workers - periods ratio (how many periods per worker)',
            type=str,
            required=True)

    parser.add_argument(
            '-d', '--d',
            help='days - how many days in each sampling period (between 1 and 7)',
            type=str,
            required=True)

    parser.add_argument(
            '-t', '--terminate',
            help='Flag - if used will terminate the manager after the task is done',
            action='store_true',
            default=False)


def _run(args):

    # Create local instance
    local = Local(args)

    # Run the task
    local.run_task()


if __name__ == '__main__':

    # create an argument parser
    ap = argparse.ArgumentParser()

    # register all arguments
    _register_arguments(ap)

    # go ahead and run
    _run(ap.parse_args())



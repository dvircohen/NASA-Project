import argparse
import os

import shutil

import utils
import utils.clients


class Local(object):

    def __init__(self, args):
        # TODO: zip code here
        self._args = args
        self._logger = utils.set_logger('local')
        self._code = None
        self._input_file_local_path = args.input_file_name
        self._output_file_path = args.output_file_name
        self._number_of_days_per_period = args.d
        self._number_periods_per_worker = args.n
        self._terminate = args.terminate
        self._input_file_s3_path = None
        self._SQS = None
        self._ec2_client = utils.clients.Ec2()
        self._s3_client = utils.clients.S3()
        self._manager = None
        self._manager_tag = {'Key': 'Role', 'Value': 'Manager'}
        self._project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',))
        self._setup_script_path = os.path.join(self._project_path, 'setup_scripts/manager_setup.sh')
        self._project_bucket_name = '673333208134-very-secret-do-not-enter'
        self._project_bucket = None

        self._logger.debug('Local initialized with args: {0}'.format(args))

    def run_task(self):
        """
        Used to start the task
        :return:
        """
        # TODO: make sure manager is up
        # run the task given in constructor
        # create the html file and finish
        self._ensure_bucket_exist()
        self._upload_input_file(os.path.join(self._project_path, 'core/local.py'), 'local.py')
        self._zip_and_upload_code()
        self._ensure_manager_is_up()
        self._kill_manager()

    def _ensure_manager_is_up(self):
        """
        Checks if the manager is up. if not, set one up
        :return:
        """

        self._logger.debug('Ensuring manager is alive')
        manager = self._ec2_client.get_instance_with_tag(self._manager_tag)
        if manager:

            # There should be only one manager so the list should have only one element
            self._logger.debug('Manager already alive')
            self._manager = manager[0]
        else:

            # No manager up, create a new one (with tag)
            self._logger.debug('No manager up, creating manager')
            iam_instance_profile = {'Arn': 'arn:aws:iam::673333208134:instance-profile/manager'}
            created_instances = self._ec2_client.create_instance(image_id='ami-b66ed3de',
                                                                 tags=[self._manager_tag],
                                                                 iam_instance_profile=iam_instance_profile,
                                                                 instance_type='t2.nano',
                                                                 min_count=1,
                                                                 max_count=1,
                                                                 user_data=self._setup_script_path)

            # We only asked for one instance so we take the first element
            self._logger.debug('Manager created')
            self._manager = created_instances[0]


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

    def _upload_input_file(self, file_path, file_name):
        self._logger.debug('Uploading file. filename: {0}, file path: {1}'.format(file_name, file_path))
        self._s3_client.upload_file(self._project_bucket, file_path, file_name)

    def _ensure_bucket_exist(self):
        self._logger.debug('Ensuring bucket exist. bucket name: {0}'.format(self._project_bucket_name))
        self._project_bucket = self._s3_client.create_or_get_bucket(self._project_bucket_name)

    def _kill_manager(self):
        """
        Terminate the manager
        :return:
        """
        # TODO: need to check the assagiment specefication, maybe we need to let the manager finish all it current
        # tasks before killing it
        if self._terminate:
            self._logger.debug('Terminating manager')
            response = self._manager.terminate()

    def _zip_and_upload_code(self):
        self._logger.debug('Zipping project code in prevention to send')
        archive_dir = os.path.abspath(os.path.join(self._project_path, '..'))
        place = os.path.join(self._project_path, 'full_code')
        shutil.make_archive(base_name=place,
                            format='zip',
                            root_dir=archive_dir,
                            base_dir='NASA-Project')
        self._logger.debug('Zipping done')
        self._s3_client.upload_file(self._project_bucket,
                                    os.path.join(self._project_path, 'full_code.zip'),
                                    'full_code.zip')


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


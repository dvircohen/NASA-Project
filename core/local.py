import argparse
import os
import uuid
import json
import StringIO
from jinja2 import Template

import shutil

import utils
import utils.clients
import messages


class Local(object):

    def __init__(self, args):
        # TODO: zip code here
        self._args = args
        self._logger = utils.set_logger('local')
        self._code = None
        self._input_file_local_path = args.input_file_name
        self._output_file_path = args.output_file_name
        self._number_of_days_per_period = int(args.days)
        self._number_periods_per_worker = args.n
        self._terminate = args.terminate
        self._input_file_s3_path = None
        self._sqs_client = utils.clients.Sqs()
        self._ec2_client = utils.clients.Ec2()
        self._s3_client = utils.clients.S3()
        self._manager = None
        self._manager_tag = {'Key': 'Role', 'Value': 'Manager'}
        self._project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',))
        self._setup_script_path = os.path.join(self._project_path, 'setup_scripts/manager_setup.sh')
        self._project_bucket_name = utils.Names.project_bucket_name
        self._project_bucket = None
        self._queue_to_manager = None
        self._queue_from_manager = None
        self._uuid = str(uuid.uuid4())
        self._queue_from_manager_name = utils.Names.manager_to_local_queue + '_' + self._uuid
        self._input_file_s3_name = 'input_file_{0}'.format(self._uuid)
        self._files_folder = os.path.join(self._project_path, 'files')

        self._logger.debug('Local initialized with args: {0}'.format(args))

    def run_task(self):
        """
        Used to start the task
        :return:
        """
        # TODO: make sure manager is up
        # run the task given in constructor
        # create the html file and finish
        self._ensure_sqs_queues_exist()
        self._ensure_bucket_exist()
        self._upload_files_and_job()
        self._ensure_manager_is_up()
        self._wait_on_summery_file_and_proccess()
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
            with open(self._setup_script_path, 'r') as myfile:
                user_data = myfile.read().format(os.environ.get('AWS_ACCESS_KEY_ID'), os.environ.get('AWS_SECRET_ACCESS_KEY'))
            iam_instance_profile = {'Arn': utils.Names.arn}
            created_instances = self._ec2_client.create_instance(image_id='ami-b73b63a0',
                                                                 tags=[self._manager_tag],
                                                                 iam_instance_profile=iam_instance_profile,
                                                                 instance_type='t2.nano',
                                                                 min_count=1,
                                                                 max_count=1,
                                                                 user_data=user_data)

            # We only asked for one instance so we take the first element
            self._logger.debug('Manager created')
            self._manager = created_instances[0]

    def _ensure_sqs_queues_exist(self):
        try:
            self._queue_to_manager = self._sqs_client.get_queue(utils.Names.local_to_manager_queue)
        except Exception as e:
            self._queue_to_manager = self._sqs_client.create_queue(utils.Names.local_to_manager_queue)

        self._queue_from_manager = self._sqs_client.create_queue(self._queue_from_manager_name)

    def _upload_files_and_job(self):
        """
        Upload the input and code file to S3
        Send a message to the SQS with the S3 path
        :return:
        """
        self._logger.debug('Uploading files')
        self._upload_input_file()
        self._zip_and_upload_code()

        self._logger.debug('Sending job to manager via sqs')
        task_message = messages.Task(local_uuid=self._uuid,
                                     input_file_s3_path=self._input_file_s3_name,
                                     days=self._args.days,
                                     n=self._args.n)
        body = messages.Task.encode(task_message)
        self._sqs_client.send_message(queue=self._queue_to_manager, body=body)

    def _wait_on_summery_file_and_proccess(self):
        """
        Wait for summery file from manager (wait on sqs message)
        Downloads summery file from s3
        Create summery html file and save it in place
        :param summery_file_s3_path:
        """
        self._logger.debug('Waiting on message from manager')
        manager_messages = []
        while not manager_messages:
            manager_messages = self._sqs_client.get_messages(queue=self._queue_from_manager,
                                                             timeout=20,
                                                             number_of_messages=1)
            for message in manager_messages:
                self._logger.debug('Message received from manager')
                done_job = messages.DoneTask.decode(message.body)
                summery_file_json = StringIO.StringIO()
                self._s3_client.download_file_as_object(bucket=self._project_bucket,
                                                        key=done_job.summery_file_s3_key,
                                                        file_object=summery_file_json)

                self._logger.debug('Summery file downloaded')
                self._create_html_output(summery_file_json.getvalue())

    def _create_html_output(self, summery_file_json):
        asteroids = json.loads(summery_file_json)
        final_asteroids = []
        for day in asteroids.itervalues():
            for asteroid in day:
                new_asteroid = utils.Asteroid(hazardous=asteroid['hazardous'],
                                              diameter_max=asteroid['diameter_max'],
                                              diameter_min=asteroid['diameter_max'],
                                              approach_date=asteroid['approach_date'],
                                              velocity=asteroid['velocity'],
                                              miss_distance=asteroid['miss_distance'],
                                              name=asteroid['name'])
                new_asteroid.set_color(asteroid['color'])
                final_asteroids.append(new_asteroid)

        template_path = os.path.join(self._project_path, 'resources/output_template.html')
        with open(template_path, 'rb') as template_file:
            template = Template(template_file.read())

        with open(self._output_file_path, 'wb') as o:
            o.write(template.render({'asteroids': final_asteroids, 'we_gonna_die': 'true'}))


    def _upload_input_file(self):
        self._logger.debug('Uploading file. filename: {0}, file path: {1}'.format(self._input_file_s3_name,
                                                                                  self._input_file_local_path))
        self._s3_client.upload_file(self._project_bucket,
                                    self._input_file_local_path,
                                    self._input_file_s3_name)

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
                            base_dir='NASA_Project')
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
            type=int,
            required=True)

    parser.add_argument(
            '-d', '--days',
            help='days - how many days in each sampling period (between 1 and 7)',
            type=int,
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



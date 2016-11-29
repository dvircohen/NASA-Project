import boto3

import utils


class S3(object):

    def __init__(self, region_name='us-east-1'):
        self._s3 = boto3.resource('s3', region_name=region_name)
        self._logger = utils.set_logger('s3_client')

    def create_or_get_bucket(self, name):
        self._logger('Getting or creating bucket. bucket name{0}'.format(name))
        return self._s3.create_bucket(Bucket=name)

    def upload_file(self, bucket, file_path, file_name):
        """
        upload a file
        :param bucket: Bucket object
        :param file_path: file path to upload
        :param file_name: file name to upload
        :return: return (return_code, Exception/None)
        """
        try:
            self._logger.debug('Uploading file. file name: {0} file path {1} bucket {2}'.format(file_name,
                                                                                                file_path,
                                                                                                bucket))
            bucket.upload_file(file_path, file_name)
            self._logger.debug('File uploaded successfully')
        except Exception as e:
            self._logger.debug('Error while uploading file')
            return 1, e
        return 0, None

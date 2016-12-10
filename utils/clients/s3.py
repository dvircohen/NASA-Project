import boto3

import utils


class S3(object):

    def __init__(self, region_name='us-east-1', logger=None):
        self._s3 = boto3.resource('s3', region_name=region_name)
        if logger is None:
            self._logger = utils.set_logger('s3_client')
        else:
            self._logger = logger.getChild('s3_client')

    def create_or_get_bucket(self, name):
        self._logger.debug('Getting or creating bucket. bucket name: {0}'.format(name))
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
            self._logger.debug('Uploading file. file name: {0}, file path: {1}, bucket: {2}'.format(file_name,
                                                                                                    file_path,
                                                                                                    bucket))
            bucket.upload_file(file_path, file_name)
            self._logger.debug('File uploaded successfully')
        except Exception as e:
            self._logger.error('Error while uploading file. error: {0}'.format(e.message))
            raise e

    def upload_object_as_file(self, bucket, key, file_object):
        try:
            self._logger.debug('Uploading object as file. key: {0}, bucket: {1}'.format(key, bucket))
            bucket.upload_fileobj(file_object, key)
        except Exception as e:
            self._logger.error('Error while uploading file. error: {0}'.format(e.message))
            raise e

    def download_file(self, bucket, key, local_filename):
        """
        download a file
        :param bucket: The bucket to download from
        :param key: The name of the file in the bucket
        :param local_filename: local path to save the downloaded file
        """
        try:
            self._logger.debug('Downloading file. key: {0}, local_filename {1}, bucket {2}'.format(key,
                                                                                                   local_filename,
                                                                                                   bucket))
            bucket.download_file(key, local_filename)
            self._logger.debug('File downloaded successfully')
        except Exception as e:
            self._logger.error('Error while downloading file. error: {0}'.format(e.message))
            raise e

    def download_file_as_object(self, bucket, key, file_object):
        """
        download a file
        :param bucket: The bucket to download from
        :param key: The name of the file in the bucket
        :param file_object: file-like object to save the file in
        """
        try:
            self._logger.debug('Downloading file. key: {0}, local_filename {1}, bucket {2}'.format(key,
                                                                                                   file_object,
                                                                                                   bucket))
            bucket.download_fileobj(key, file_object)
            self._logger.debug('File downloaded successfully')
        except Exception as e:
            self._logger.error('Error while downloading file. error: {0}'.format(e.message))
            raise e

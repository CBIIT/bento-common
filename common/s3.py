#!/usr/bin/env python
import os

import boto3
import botocore
from botocore.exceptions import ClientError

from .utils import get_logger, get_md5_hex_n_base64, remove_leading_slashes

class S3Bucket:
    def __init__(self, bucket):
        self.bucket_name = bucket
        self.client = boto3.client('s3')
        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(bucket)
        self.log = get_logger('S3 Bucket')

    def upload_file_obj(self, key, data, md5_base64):
        return self.bucket.put_object(Key=key, Body=data, ContentMD5=md5_base64)

    def download_file(self, key, filename):
        return self.bucket.download_file(key, filename)

    def download_file_obj(self, key, obj):
        self.bucket.download_fileobj(key, obj)

    def delete_file(self, key):
        response = self.bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key
                    }
                ]
            }
        )
        if 'Errors' in response:
            self.log.error('S3: delete file {} failed!'.format(key))
            return False
        else:
            return True

    def same_file_exists_on_s3(self, key, md5):
        '''
        Check if same file already exists in S3, return True only if file with same MD5 exists

        :param key: file path
        :param md5: file MD5
        :return: boolean
        '''
        try:
            self.client.head_object(Bucket=self.bucket.name, Key=key, IfMatch=md5)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] in ['404', '412']:
                return False
            else:
                self.log.error('Unknown S3 client error!')
                self.log.exception(e)

    def upload_file(self, key, fileName):
        with open(fileName, 'rb') as data:
            safer_key = remove_leading_slashes(key)
            md5_obj = get_md5_hex_n_base64(fileName)
            md5_base64 = md5_obj['base64']
            md5_hex = md5_obj['hex']
            if self.same_file_exists_on_s3(safer_key, md5_hex):
                self.log.info('Same file already exists, skip uploading!')
                return {'bucket': self.bucket.name, 'key': safer_key, 'md5': md5_hex}
            else:
                obj = self.upload_file_obj(safer_key, data, md5_base64)
                if obj:
                    return {'bucket': self.bucket.name, 'key': safer_key, 'md5': obj.e_tag[1:-1]}
                else:
                    message = "Upload file {} to S3 failed!".format(fileName)
                    self.log.error(message)
                    return None

    def download_files_in_folder(self, folder, local_path):
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
            result = self.client.list_objects_v2(Bucket=self.bucket_name,  Prefix=folder)
            for file in result.get('Contents', []):
                if file['Size'] > 0:
                    key = file['Key']
                    base_name = os.path.basename(key)
                    file_name = os.path.join(local_path, base_name)
                    self.log.info('Downloading "{}" from AWS S3'.format(base_name))
                    self.download_file(key, file_name)
            return True
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 403:
                self.log.error('Don\'t have permission to access for Bucket: "{}"'.format(self.bucket_name))
            elif error_code == 404:
                self.log.error('Bucket: "{}" does NOT exist!'.format(self.bucket_name))
            return False

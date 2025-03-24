import json
import os
import pathlib
from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


class S3Handler:
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None, **kwargs):

        if aws_access_key_id and aws_secret_access_key:
            self.s3_resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
                                              aws_secret_access_key=aws_secret_access_key,
                                              config=Config(signature_version='s3v4'))
        else:
            self.s3_resource = boto3.resource('s3', config=Config(signature_version='s3v4'))
        self.s3_client = self.s3_resource.meta.client
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.s3_session = None

    def get_client(self):
        return self.s3_client

    def bucket_exists(self, bucket_name):
        """Determine whether bucket_name exists and the user has permission to access it

        :param bucket_name: string
        :return: True if the referenced bucket_name exists, otherwise False
        """
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
        except ClientError:
            return False
        return True

    def connection_pooling(self, bucket_name):
        self.s3_client.head_bucket(Bucket=bucket_name)

    def get_s3_session(self):
        if self.s3_session:
            return self.s3_session
        return self.create_s3_session()

    def list_top_level_objects_from_s3_url(self, s3_url):
        paths = []
        bucket, path = self.get_bucket_name_and_file_path_from_s3_url(s3_url)
        paginator = self.s3_client.get_paginator('list_objects_v2')
        result = paginator.paginate(Bucket=bucket, Delimiter='/', Prefix=path)
        for prefix in result.search('CommonPrefixes'):
            paths.append(f's3://{bucket}/{prefix.get("Prefix")}')
        return paths

    def create_s3_session(self):
        if not self.aws_access_key_id:
            self.s3_session = boto3.session.Session()
        else:
            self.s3_session = boto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
            )
        return self.s3_session

    def upload(self, filename, bucket, s3_path=None, s3_full_path=None, callback=None, extra_args=None):
        if not s3_full_path:
            if not s3_path:
                raise ValueError('We must have s3_full_path or s3_path')
            s3_full_path = os.path.join(s3_path, os.path.basename(filename))
        s3_full_path = str(pathlib.Path(s3_full_path))
        self.s3_client.upload_file(filename, bucket, s3_full_path,
                                   callback, extra_args)
        s3_url = 's3://{bucket}/{key}'.format(bucket=bucket, key=s3_full_path)
        return s3_url

    def delete_all_s3_data_from_path(self, bucket_name, path):
        if not path.endswith('/'):
            path = f'{path}/'
        bucket = self.s3_resource.Bucket(bucket_name)
        bucket.objects.filter(Prefix=path).delete()

    def copy_file(self, source_bucket_name, source_file_path, dest_bucket_name, dest_file_path):
        copy_source = {
            'Bucket': source_bucket_name,
            'Key': source_file_path
        }
        self.s3_client.copy(copy_source, dest_bucket_name, dest_file_path)

    def copy_folder(self, source_bucket_name, source_folder_path, dest_bucket_name, dest_folder_path):
        source_bucket = self.s3_resource.Bucket(source_bucket_name)

        for object_summary in source_bucket.objects.filter(Prefix=source_folder_path):
            source_file_path = object_summary.key
            dest_file_path = source_file_path.replace(source_folder_path, dest_folder_path, 1)
            self.copy_file(source_bucket_name=source_bucket_name, source_file_path=source_file_path,
                           dest_bucket_name=dest_bucket_name, dest_file_path=dest_file_path)

    def search_files_in_s3_folder_by_extension(self, path, extension):
        parse_url_result = urlparse(path)
        bucket_name = parse_url_result.netloc
        bucket = self.s3_resource.Bucket(bucket_name)
        prefix = parse_url_result.path[1:]
        file_list = []
        for file in bucket.objects.filter(Prefix=prefix):
            if file.key.endswith(extension):
                file_list.append(f'{parse_url_result.scheme}://{bucket_name}/{file.key}')
        return file_list

    @staticmethod
    def get_file_name_from_s3_url(url):
        file = urlparse(url)
        return Path(file.path).name

    @staticmethod
    def get_bucket_name_and_file_path_from_s3_url(s3_url):
        parse_url_result = urlparse(s3_url)
        bucket_name = parse_url_result.netloc
        file_path = parse_url_result.path[1:]
        file_path = file_path[1:] if file_path.startswith('/') else file_path
        return bucket_name, file_path

    def download_file(self, s3_file_url, out_file_path):
        bucket_name, s3_file_path = self.get_bucket_name_and_file_path_from_s3_url(s3_file_url)
        self.s3_client.download_file(bucket_name, s3_file_path, out_file_path)

    def get_files_added_after_a_specific_timewise_file_name(self, s3_folder_url, specific_timewise_file_name,
                                                            is_include_this_file=False, file_prefix=''):
        parse_url_result = urlparse(s3_folder_url)
        bucket_name = parse_url_result.netloc
        folder_path = parse_url_result.path[1:]
        if folder_path.endswith('/'):
            folder_path = folder_path[:-1]
        paginator = self.s3_client.get_paginator('list_objects')
        page_iterator = paginator.paginate(
            Bucket=bucket_name, Prefix=f'{folder_path}/{file_prefix}')
        filtered_iterator = page_iterator.search(f"Contents[?Key > `{folder_path}/{specific_timewise_file_name}`][]")
        resp_items = [items.get('Key') for items in filtered_iterator]
        if not resp_items:
            return []
        file_path_list = [f's3://{bucket_name}/{item}' for item in resp_items if
                          is_include_this_file or item != specific_timewise_file_name]
        return file_path_list

    def write_dict_to_json_in_s3(self, json_data, s3_file_url):
        return self.write_file_to_s3(bytes(json.dumps(json_data).encode('UTF-8')), s3_file_url)

    def write_file_to_s3(self, file_content, s3_file_url):
        bucket_name, s3_file_path = self.get_bucket_name_and_file_path_from_s3_url(s3_file_url)
        s3object = self.s3_resource.Object(bucket_name, s3_file_path)

        s3object.put(
            Body=file_content
        )

    def get_dict_from_s3_json_file(self, s3_file_url):
        bucket_name, s3_file_path = self.get_bucket_name_and_file_path_from_s3_url(s3_file_url)
        s3object = self.s3_resource.Object(bucket_name, s3_file_path)

        return json.load(s3object.get()["Body"])

    def check_s3_url_exist(self, s3_file_url):
        bucket_name, s3_file_path = self.get_bucket_name_and_file_path_from_s3_url(s3_file_url)

        result = self.s3_client.list_objects(Bucket=bucket_name, Prefix=s3_file_path)
        is_exist = False
        if 'Contents' in result:
            is_exist = True
        return is_exist


s3_handler = S3Handler()
# print(s3_handler.list_top_level_objects_from_s3_url('s3://mylong-datalake-prod/public/'))

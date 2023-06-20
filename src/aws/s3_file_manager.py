import logging
from s3_client import S3Client
from botocore.exceptions import ClientError
import pandas as pd


class S3FileManager:

    def __init__(self, s3_client: S3Client):
        self.s3_client = s3_client

    def list_objects(self, bucket_name: str, folder_path: str) -> list:
        """
        List all objects in a folder in an S3 bucket.

        :param bucket_name: The name of the S3 bucket.
        :type bucket_name: str

        :param folder_path: The path to the folder in the bucket.
        :type folder_path: str

        :return: A list of object keys in the specified folder.
        :rtype: list
        """
        try:
            response = self.s3_client.s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
            objects = [obj['Key'] for obj in response.get('Contents', [])]
            return objects
        except ClientError as e:
            logging.error(e)
        except ValueError as e:
            raise e

    def read_csv(self, bucket_name: str, object_key: str) -> bytes:
        """
        Read an object from an S3 bucket.

        :param bucket_name: The name of the S3 bucket.
        :type bucket_name: str

        :param object_key: The key of the object to read.
        :type object_key: str

        :return: The data of the object.
        :rtype: bytes
        """
        try:
            response = self.s3_client.s3.get_object(Bucket=bucket_name, Key=object_key)
            data = response['Body'].read()
            return data
        except ClientError as e:
            logging.error(e)
        except ValueError as e:
            raise e
import os
import io
from datetime import datetime
import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa


class S3Client:

    def __init__(self):
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        )

    def write_json(self, bucket_name: str, file_path: str, data: bytes) -> bool:
        """
        Write a json file to an S3 bucket.

        :param bucket_name: The name of the S3 bucket where the file will be saved.
        :type bucket_name: str

        :param file_path: The path and name of the file that will be written to the S3 bucket.
        :type file_path: str

        :param data: The object that contains the data to be stored in the S3 bucket.
        :type data: bytes

        :return: True if the file was uploaded successfully, else False.
        :rtype: bool
        """
        try:

            self.s3.put_object(Bucket=bucket_name, Key=file_path, Body=data)
            print(f"Data was written to S3://{bucket_name}/{file_path}")
            
        except Exception as e:
            print(f"Error: {e}")
            return False
        return True
    
    def write_parquet(self, bucket_name: str, file_path: str, df: pd.DataFrame, partition_cols: list = None) -> bool:
        """
        Write a DataFrame to an S3 bucket in Parquet format.

        :param bucket_name: The name of the S3 bucket where the file will be saved.
        :type bucket_name: str

        :param file_path: The path and name of the file that will be written to the S3 bucket.
        :type file_path: str

        :param df: The DataFrame that contains the data to be stored in the S3 bucket.
        :type df: pd.DataFrame

        :param partition_cols: The list of columns to be used for partitioning the data.
        :type partition_cols: list, optional

        :return: True if the file was uploaded successfully, else False.
        :rtype: bool
        """
        try:

            if partition_cols:

                table = pa.Table.from_pandas(df, preserve_index=False)
                pq.write_to_dataset(table, f"s3://{bucket_name}/{file_path}", partition_cols=partition_cols)

            else:

                table = pa.Table.from_pandas(df, preserve_index=False)
                pq.write_table(table, f"s3://{bucket_name}/{file_path}")

            print(f"Data was written to S3://{bucket_name}/{file_path}")

            return True
        
        except Exception as e:

            print(f"Error: {e}")
            
            return False
import io
from s3_file_manager import S3FileManager
import pandas as pd


class S3FileConcatenator:
    
    def __init__(self, s3_file_manager: S3FileManager):

        self.s3_file_manager = s3_file_manager

    def concatenate_files(self, bucket_name: str, folder_path: str, file_format: str) -> pd.DataFrame:

        """
        Concatenate all CSV files in a folder in an S3 bucket into a single pandas DataFrame.

        :param bucket_name: The name of the S3 bucket.
        :type bucket_name: str

        :param folder_path: The path to the folder in the bucket that contains the CSV files.
        :type folder_path: str

        :return: A pandas DataFrame containing the concatenated data from the CSV files.
        :rtype: pd.DataFrame
        """

        object_keys = self.s3_file_manager.list_objects(bucket_name, folder_path)
        data_frames = []

        for object_key in object_keys:

            if file_format == '.csv':

                data = self.s3_file_manager.read_csv(bucket_name, object_key)
                df = pd.read_csv(io.BytesIO(data))
                data_frames.append(df)

        concatenated_df = pd.concat(data_frames)

        return concatenated_df
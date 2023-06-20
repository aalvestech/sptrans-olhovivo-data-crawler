import os
import json
from datetime import datetime
from src.aws.s3_client import S3Client
from src.sptrans_client.sptrans_client import SPTransClient
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

class Crawler():


    def __init__(self):

        self.sptc = SPTransClient()
        self.aws = S3Client()
        self.BUCKET_NAME = os.getenv('BUCKET_NAME')
        
    
    def get_auth(self):

        '''
            This function is responsible for authenticating us with the SPTrans Olho Vivo API using the TOKEN_API as the key.

            :return: It returns the text of the response from the request, which can be either 'true' or 'false', and the HTTP status code of the request (200, 400, 500, etc...).
             As inherited from the SPTrans interface.
            :rtype: list
        '''

        return self.sptc.auth()
    

    def get_all_bus_positions(self, folder_path):
        
        bus_position_json = self.sptc.bus_position()
        df_bus_position = pd.DataFrame(bus_position_json)
        file_name = 'all_bus_positions'

        self.aws.write_parquet(self.BUCKET_NAME, folder_path, file_name, df_bus_position)
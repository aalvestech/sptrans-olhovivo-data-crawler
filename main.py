from src.crawler.crawler import Crawler
from src.aws.s3_client import S3Client

crwl = Crawler()
s3_client = S3Client()



crwl.get_auth()
bus_position = crwl.get_all_bus_positions('raw/bus_position/')
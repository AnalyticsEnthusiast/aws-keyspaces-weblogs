import boto3
from kinesis_stream import KinesisStream
import configparser

config = configparser.ConfigParser()
config.read("aws-personal.cfg")

STREAM_NAME = config.get("KINESIS", "STREAM_NAME")
AWS_ACCESS_KEY_K = config.get('KINESIS', 'AWS_ACCESS_KEY')
AWS_SECRET_KEY_K = config.get('KINESIS', 'AWS_SECRET_KEY')
AWS_REGION = config.get("MASTER", "AWS_REGION")

LOG_FILE="data/access_log_sample.txt"


def main():
    
    client = boto3.client('kinesis', aws_access_key_id=AWS_ACCESS_KEY_K, aws_secret_access_key=AWS_SECRET_KEY_K, region_name=AWS_REGION)
    
    ks = KinesisStream(client)
    
    ks.describe(STREAM_NAME)
    
    return ks.get_records(5)
    
    
    
    
if __name__  == "__main__":
    d = main()
    
    for record in d:
        print(d)
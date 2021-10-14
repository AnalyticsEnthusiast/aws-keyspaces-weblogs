import boto3
import json
import configparser
import time

config = configparser.ConfigParser()
config.read("aws-personal.cfg")

STREAM_NAME = config.get("KINESIS", "STREAM_NAME")
AWS_ACCESS_KEY_K = config.get('KINESIS', 'AWS_ACCESS_KEY')
AWS_SECRET_KEY_K = config.get('KINESIS', 'AWS_SECRET_KEY')
AWS_REGION = config.get("MASTER", "AWS_REGION")

LOG_FILE="data/access_log_sample.txt"


def generate(stream_name, kinesis_client):
    
    lines=100
    
    with open(f"{LOG_FILE}", "r") as f:
        
        while lines > 0:
            data = f.readline()
            print(data)
            
            time.sleep(2)

            kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(data),
                PartitionKey="log-partition")

            lines -= 1
            
    print(f"Finished writing to stream {stream_name}")


if __name__ == '__main__':
    generate(STREAM_NAME, boto3.client('kinesis', aws_access_key_id=AWS_ACCESS_KEY_K, aws_secret_access_key=AWS_SECRET_KEY_K, region_name=AWS_REGION))
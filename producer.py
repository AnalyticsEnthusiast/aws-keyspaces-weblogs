import boto3
import json
import configparser
import time
import sys

config = configparser.ConfigParser()
config.read("aws-personal.cfg")

STREAM_NAME = config.get("KINESIS", "STREAM_NAME")
AWS_ACCESS_KEY_K = config.get('KINESIS', 'AWS_ACCESS_KEY')
AWS_SECRET_KEY_K = config.get('KINESIS', 'AWS_SECRET_KEY')
AWS_REGION = config.get("MASTER", "AWS_REGION")

LOG_FILE="data/access_log_sample.txt"


def shard_summary(stream_name, kinesis_client):
    try:
        response = kinesis_client.describe_stream_summary(
            StreamName=stream_name
        )
        return response
    except Exception as e:
        print(e)
        return None


def delete_stream(stream_name, kinesis_client):
    try:
        kinesis_client.delete_stream(
            StreamName=stream_name,
            EnforceConsumerDeletion=True
        )
        print(f"{stream_name} has been deleted")
        
    except Exception as e:
        print(e)
        print("Stream deletion Failed") 

    
    
def create_stream(stream_name, kinesis_client, shard_count=1):
    try:
        kinesis_client.create_stream(
            StreamName=stream_name,
            ShardCount=shard_count
        )
        print(f"{stream_name} has been created")
        
    except Exception as e:
        print(e)
        print("Stream Creation Failed")
        

def generate(stream_name, kinesis_client):
    
    lines=100
    
    # Check if stream exists first
    try:
        summary = shard_summary(stream_name, kinesis_client)
        
        if summary['StreamDescriptionSummary']['StreamStatus'] == "DELETING":
            print("Stream is being deleted")
            return
        
        while summary['StreamDescriptionSummary']['StreamStatus'] != "ACTIVE":
            time.sleep(5)
        
    except Exception as e:
        print(e)
        return
    
    
    with open(f"{LOG_FILE}", "r") as f:
        
        while lines > 0:
            data = f.readline()
            print(data)
            
            time.sleep(1)

            kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(data.strip("\n")),
                PartitionKey="log-partition")

            lines -= 1
            
    print(f"Finished writing to stream {stream_name}")


if __name__ == '__main__':
    
    args = [i.upper() for i in sys.argv[1:len(sys.argv)]]
    
    client = boto3.client('kinesis', aws_access_key_id=AWS_ACCESS_KEY_K, aws_secret_access_key=AWS_SECRET_KEY_K, region_name=AWS_REGION)
    
    if "CREATE" in args:
        create_stream(STREAM_NAME, client)
        
    elif "DELETE" in args:
        delete_stream(STREAM_NAME, client)
        
    if "GENERATE" in args:
        generate(STREAM_NAME, client)
    
    if "SUMMARY" in args:
        print(shard_summary(STREAM_NAME, client))
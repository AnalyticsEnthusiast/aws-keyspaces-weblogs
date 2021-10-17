import boto3
import json
import configparser
import time
import sys
import argparse

config = configparser.ConfigParser()
config.read("aws-personal.cfg")

STREAM_NAME = config.get("KINESIS", "STREAM_NAME")
AWS_ACCESS_KEY_K = config.get('KINESIS', 'AWS_ACCESS_KEY')
AWS_SECRET_KEY_K = config.get('KINESIS', 'AWS_SECRET_KEY')
AWS_REGION = config.get("MASTER", "AWS_REGION")

LOG_FILE="data/access_ordered_sample.log"


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
        

def generate(stream_name, kinesis_client, delay=1.0):
    
    lines=100
    
    # Check if stream exists first
    try:
        summary = shard_summary(stream_name, kinesis_client)
        
        if summary['StreamDescriptionSummary']['StreamStatus'] == "DELETING":
            print("Stream is being deleted")
            return
        
        while summary['StreamDescriptionSummary']['StreamStatus'] != "ACTIVE":
            time.sleep(delay)
        
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
    
    
    parser = argparse.ArgumentParser(
        prog='producer.py',
        description='Operations for Kinesis stream',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-c', '--create', help='Create Kinesis Stream', action="store_true")
    parser.add_argument('-d', '--delete', help='Delete Kinesis Stream', action="store_true")
    parser.add_argument('-s', '--summary', help='Stream details summary', action="store_true")
    parser.add_argument('-g', '--generate', dest='generate', type=float, help='Frequency of load')
    
    args = parser.parse_args(sys.argv[1:])
    
    #Validate arguments
    if args.create and args.delete:
        sys.exit("Cannot create and delete a kinesis stream at the same time")
    elif args.create and args.summary:
        sys.exit("Cannot create and summarise a kinesis stream at the same time")
    elif args.delete and args.summary:
        sys.exit("Cannot create and summarise a kinesis stream at the same time")
    
    client = boto3.client('kinesis', aws_access_key_id=AWS_ACCESS_KEY_K, aws_secret_access_key=AWS_SECRET_KEY_K, region_name=AWS_REGION)
    
    if args.create:
        create_stream(STREAM_NAME, client)
        
    elif args.delete:
        delete_stream(STREAM_NAME, client)
        
    if args.generate is not None:
        delay = args.generate
        generate(STREAM_NAME, client, delay)
    
    if args.summary:
        print(shard_summary(STREAM_NAME, client))
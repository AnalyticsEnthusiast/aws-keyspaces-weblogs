import json
import base64
import boto3
from parseWebLogs import parse_web_logs
from generateSalt import generate_salt
import logging

def lambda_handler(event, context):
    """
    Description: 
        Main Lambda Function for parsing incoming web logs from producer. Performs etl, creates partition key using a salt then writes data to a dynamoDB table called 'log-table'.
    
    Arguments:
        event - Incoming Json event object. Data is base64 encoded.
        conext - Stores metadata about the event
        
    Returns:
        result - Parsed json object
    """
    for record in event['Records']:
        data = record['kinesis']['data']
        
        #Decode Base64 string
        base64_bytes = data.encode('utf-8')
        message_bytes = base64.b64decode(base64_bytes)
        message = message_bytes.decode('utf-8')
        
        #Use parse web logs function
        result = parse_web_logs(message)
        result = dict(result)
        
        m = json.loads(result['body'])
        record = m['message']
        
        #Create partition key
        salt = generate_salt()
        partition_key = str(salt) + "#" + str(record[0]['host_ip'])
        
        try:
            
            client = boto3.client('dynamodb')
            
            if str(result['statusCode']) == "200":
                
                client.put_item(
                    TableName='log-table',    
                    Item={
                        "id": {
                            'S': f"{partition_key}"
                        },
                        "host_ip": {
                            'S': f"{record[0]['host_ip']}"
                        }, 
                        "rfc931": {
                            'S': f"{record[0]['rfc931']}"
                        },
                        "username": {
                            'S': f"{record[0]['username']}"
                        }, 
                        "datetime": {
                           'S': f"{record[0]['datetime']}"
                        },
                        "http_method": {
                            'S': f"{record[0]['http_method']}"
                        },
                        "http_url": {
                            'S': f"{record[0]['http_url']}"
                        },
                        "http_protocol": {
                            'S': f"{record[0]['http_protocol']}"
                        },
                        "http_status_code": {
                            'S': f"{record[0]['http_status_code']}"
                        }, 
                        "bytes_sent": {
                            'S': f"{record[0]['bytes_sent']}"
                        },
                        "referrer_url": {
                            'S': f"{record[0]['referrer_url']}"
                        },
                        "user_agent": {
                            'S': f"{record[0]['user_agent']}"
                        }, 
                        "cookies": {
                            'S': f"{record[0]['cookies']}"
                        },
                        "hostname": {
                            'S': f"{record[0]['hostname']}"
                        }
                    }
                )
                
            else:
                return result
        
        except Exception as e:
            print(e)
            return result
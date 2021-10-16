import json
import re
import logging
from hostNameLookup import host_name_lookup

def parse_web_logs(input_record):
    """
    Description: Parses the incoming web logs from the kinesis data stream one by one.
    
    Arguments:
        input_record - Should look like "54.36.148.87 - - [22/Jan/2019:03:56:34 +0330] "GET /filter/p65%2Cv1%7C%D9%86%D9%82%D8%B1%D9%87%20%D8%A7%DB%8C.%2C6315%7C%D8%AA%D8%AE%D8%AA%20%28%20Flat%20%29?o=6315 HTTP/1.1" 302 0 "-" "Mozilla/5.0 (compatible; AhrefsBot/6.1; +http://ahrefs.com/robot/)" "-""
    
    Returns:
        Json object with each group extracted from regex. Each data row should have 12 fields.
    """
    
    combined_regex = r'^(\S+) (\S+) (\S+) \[(\w{1,2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}\s[+\-]\d{4})\] \\"(\S+)\s?(\S+)?\s?(\S+)?\\" (\d{3}|-) (\d+|-)\s?\\"?([^"]*)\\"?\s?\\"?([^"]*)?\\"\s?\\"?([^"]*)?\\"?"$'
    
    try:
        #record = str(input_record).replace("\\n","")
        #print(input_record.replace("\\n",""))
        #record = input_record.replace("\\n","")
        print(input_record)
        re_pattern = re.search(combined_regex, str(input_record))
        #print(re_pattern.group(1))
        print(f"This -> {re_pattern.groups()}")
        
        host_ip = re_pattern.group(1).replace('\"','')
        
        
        if len(re_pattern.groups()) != 12:
                return {
                    "statusCode": 500,
                    "headers": {
                        "Content-Type": "application/json"
                    },
                    "body": json.dumps({
                        "message": [{
                            "host_ip": "None", 
                            "rfc931": "",
                            "username": "", 
                            "datetime": "",
                            "http_method": "",
                            "http_url": "",
                            "http_protocol": "",
                            "http_status_code": "", 
                            "bytes_sent": "",
                            "referrer_url": "",
                            "user_agent": "", 
                            "cookies": "",
                            "hostname": ""
                        }]
                    })
                }
        
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "message": [{
                    "host_ip": f"{host_ip}", 
                    "rfc931": f"{re_pattern.group(2)}",
                    "username": f"{re_pattern.group(3)}", 
                    "datetime": f"{re_pattern.group(4)}",
                    "http_method": f"{re_pattern.group(5)}",
                    "http_url": f"{re_pattern.group(6)}",
                    "http_protocol": f"{re_pattern.group(7)}",
                    "http_status_code": f"{re_pattern.group(8)}", 
                    "bytes_sent": f"{re_pattern.group(9)}",
                    "referrer_url": f"{re_pattern.group(10)}",
                    "user_agent": f"{re_pattern.group(11)}", 
                    "cookies": f"{re_pattern.group(12)}",
                    "hostname": f"{host_name_lookup(host_ip)}"
                }]
        })
        }
    
    except Exception as e:
        print(e)
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                        "message": [{
                            "host_ip": "Error", 
                            "rfc931": "",
                            "username": "", 
                            "datetime": "",
                            "http_method": "",
                            "http_url": "",
                            "http_protocol": "",
                            "http_status_code": "", 
                            "bytes_sent": "",
                            "referrer_url": "",
                            "user_agent": "", 
                            "cookies": "",
                            "hostname": ""
                        }]
                    })
        }
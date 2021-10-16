import socket
import re

def host_name_lookup(ip_address=""):
    """
    Description: Does a DNS lookup of client IP address in order to enrich the dataset.
    
    Arguments:
        ip_address - IP Address of client
        
    Returns:
        Hostname, DNS error or empty string
    """
    ip_regex='^(\d+).(\d+).(\d+).(\d+)$'
    pattern = re.search(ip_regex, str(ip_address))
    
    
    if ip_address == "":
        return ""
        
    elif len(pattern.groups()) == 4:
        
        try:
            lookup = socket.gethostbyaddr(f"{ip_address}")
            return str(lookup[0])
        except Exception as e:
            return e
            
    else:
        return ""
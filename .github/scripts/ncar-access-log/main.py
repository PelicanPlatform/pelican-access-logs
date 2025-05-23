from datetime import date
import time
import requests

from opensearchpy import OpenSearch

TIMEOUT = 300

HOST = "https://gracc.opensciencegrid.org/q"
INDEX = "xrd-stash*"
DATA_PATH = "ncar-access-log"

query = {
    "size": 1000,
    "_source": ["@timestamp", "filename", "host", "server", "read", "write", "operation_time", "site", "appinfo"],
    "query": {
        "bool": {
            "filter": [
                {
                    "range": {
                        "@timestamp": {
                            "gte": "now-7d/d",
                            "lte": "now/d"
                        }
                    }
                },
             ],
            "should": [
                {
                    "match_phrase": {
                        "filename": "ncar/"
                    }
                },
                {
                    "match_phrase": {
                        "filename": "ncar-rda/"
                    }
                },
                {
                    "match_phrase": {
                        "filename": "ncar-rda-test/"
                    }
                },
                {
                    "match_phrase": {
                        "filename": "ncar-cesm2-lens/"
                    }
                }
            ],
            "minimum_should_match": 1  # Ensures at least one match is required
        }
    }
}

def parse_server_response(retries=3, delay=5):
    """
    Fetches and parses the JSON response from the given endpoint.

    Args:
        endpoint (str): The URL of the API endpoint.

    Returns:
        dict: A dictionary mapping 'name' to a tuple of ('latitude', 'longitude') for entries with a valid 'name'.
    """
    endpoint = "https://osdf-director.osg-htc.org/api/v1.0/director_ui/servers"
    for attempt in range(retries):
        try:
            response = requests.get(endpoint)
            response.raise_for_status()  # Raise an exception for HTTP errors
            data = response.json()  # Parse the JSON response

            # Map 'name' to ('latitude', 'longitude') only if 'name' exists
            servers = {}
            for entry in data:
                name = entry.get("name")
                if name:  # Only process entries with a valid 'name'
                    latitude = float(entry.get("latitude", 0))  # Default to 0 if not found
                    longitude = float(entry.get("longitude", 0))  # Default to 0 if not found
                    servers[name] = (latitude, longitude)

            return servers

        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                print("Max retries reached. Returning empty server data.")
                return {}
        except ValueError as e:
            print(f"Error parsing JSON response: {e}")
            return {}

def ipv_cleanup(host_str):
    if not host_str:
        return "N/A"
    
    if host_str[0] == "[":
        host_str = host_str[1:]
        
    if host_str[-1] == "]":
        host_str = host_str[:len(host_str)-1]
    # Convert the string to an IPv6 address object

    if host_str.startswith("::"):
        host_str = host_str[2:]
        
    return host_str 

def determine_server_type(site):
    if site == "NCAR_OSDF_ORIGIN":
        return "[ServerType:origin]"
    elif not site or site == "N/A":
        return "[ServerType:unknown]"
    else:
        return "[ServerType:cache]"
    
def is_pelican_or_python(appinfo):
    if "xrdcl-pelican" in appinfo or "python" in appinfo:
        return True
    return False

def print_error(d, depth=0):
    pre = depth*"\t"
    for k, v in d.items():
        if k == "failed_shards":
            print(f"{pre}{k}:")
            print_error(v[0], depth=depth+1)
        elif k == "root_cause":
            print(f"{pre}{k}:")
            print_error(v[0], depth=depth+1)
        elif isinstance(v, dict):
            print(f"{pre}{k}:")
            print_error(v, depth=depth+1)
        elif isinstance(v, list):
            nt = f"\n{pre}\t"
            print(f"{pre}{k}:\n{pre}\t{nt.join(v)}")
        else:
            print(f"{pre}{k}:\t{v}")

def write_to_files(files, content):
    for f in files:
        try:
            f.write(content)
        except IOError as e:
            print(f"Error writing to file {f.name}: {e}")

def main():
    client = OpenSearch(hosts=[HOST], request_timeout=TIMEOUT,)
    
    with open(f"{DATA_PATH}/latest.log", "w") as f1, open(f"{DATA_PATH}/{date.today()}.log", "w") as f2:
        try:
            # Initialize the scroll
            response = client.search(
                body=query,
                index=INDEX,
                scroll="2m",  # Keep the scroll context alive for 2 minutes
                size=1000     # Number of entries per batch
            )
            scroll_id = response["_scroll_id"]  # Get the scroll ID
            total_hits = response["hits"]["total"]["value"]  # Total number of hits
            print(f"Total hits: {total_hits}")

            servers = parse_server_response()

            batch_count = 1
            while True:
                print(f"Processing batch {batch_count}...")
                for src in response['hits']['hits']:
                    hit = src['_source']
                    timestamp = hit.get('@timestamp', 'N/A')
                    filename = hit.get('filename', 'N/A')
                    host_str = hit.get('host', 'N/A')
                    server = hit.get('server', 'N/A')
                    site = hit.get('site', 'N/A')
                    appinfo = hit.get('appinfo', 'N/A')
                    operation_time = str(hit.get('operation_time', '-1'))

                    host = ipv_cleanup(host_str)
                    if host == None:
                        host = host_str

                    read = str(hit.get('read', 'N/A'))
                    write = str(hit.get('write', 'N/A'))
                    server_type = determine_server_type(site)

                    p_client = str(is_pelican_or_python(appinfo))

                    if site in servers:
                        latitude, longitude = servers[site]
                    else:
                        latitude, longitude = 0, 0

                    geoip = f"[Latitude:{latitude}] [Longitude:{longitude}]"

    
                    content = f"[{timestamp}] [Objectname:{filename}] [Site:{site}] [Host:{host}] [Server:{server}]  {server_type} {geoip} [AppInfo:{appinfo}] [PelicanClient:{p_client}] [Read:{read}] [Write:{write}] [OpTime:{operation_time}s]\n"

                    write_to_files([f1, f2], content)

                response = client.scroll(scroll_id=scroll_id, scroll="2m")  # Get the next batch of results
                if not response["hits"]["hits"]:
                    break

                batch_count += 1

        except KeyboardInterrupt:
            print("Process interrupted by user.")
            pass

        except Exception as err:
            print(f"Error: {err}")
            if hasattr(err, 'info') and isinstance(err.info, dict):
                print_error(err.info)
            else:
                print("Error info not available or not in expected format.")
                raise err
        finally:
            if "scoll_id" in locals():
                client.clear_scroll(scroll_id=scroll_id)


if __name__ == "__main__":
    main()

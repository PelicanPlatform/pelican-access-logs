from datetime import date
import time
import requests
import math
from opensearchpy import OpenSearch
import argparse
from datetime import datetime, timedelta

TIMEOUT = 3600  # Increased from 300 to 3600 (1 hour)

HOST = "https://gracc.opensciencegrid.org/q"
INDEX = "xrd-stash*"
DATA_PATH = "ncar-access-log"

# Query for non-NCAR_OSDF_ORIGIN entries
def build_non_origin_query(start_date, end_date):
    non_origin_query = {
    "size": 10000,
    "_source": ["@timestamp", "filename", "host", "server", "read", "write", "operation_time", "site", "appinfo"],
    "query": {
        "bool": {
            "must": [
                {
                    "bool": {
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
                        "minimum_should_match": 1
                    }
                }
            ],
            "filter": [
                {
                    "range": {
                        "@timestamp": {
                            "gte": start_date,
                            "lte": end_date
                        }
                    }
                },
                {
                    "bool": {
                        "must_not": [
                            {
                                "term": {
                                    "site.keyword": "NCAR_OSDF_ORIGIN"
                                }
                            }
                        ]
                    }
                }
            ]
        }
    }
    }

    return non_origin_query

# Query for aggregated NCAR_OSDF_ORIGIN entries
def build_origin_composite_query(start_date, end_date, after_key=None):
    """
    Constructs the composite aggregation query for NCAR_OSDF_ORIGIN entries.

    Args:
        after_key (dict or None): Optional after_key for pagination.

    Returns:
        dict: Elasticsearch/OpenSearch query body.
    """
    composite_body = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {"term": {"site.keyword": "NCAR_OSDF_ORIGIN"}},
                    {"exists": {"field": "filename"}},
                    {
                        "bool": {
                            "must_not": [
                                {"term": {"filename.keyword": ""}},
                                {"term": {"filename.keyword": "missing directory"}}
                            ]
                        }
                    }
                ],
                "filter": [
                    {
                        "range": {
                            "@timestamp": {
                                "gte": start_date,
                                "lte": end_date
                            }
                        }
                    }
                ]
            }
        },
        "aggs": {
            "composite_buckets": {
                "composite": {
                    "size": 500,
                    "sources": [
                        {
                            "interval": {
                                "date_histogram": {
                                    "field": "@timestamp",
                                    "fixed_interval": "5m"
                                }
                            }
                        },
                        {
                            "filename": {
                                "terms": {
                                    "field": "filename.keyword"
                                }
                            }
                        }
                    ]
                },
                "aggs": {
                    "total_read": {"sum": {"field": "read"}},
                    "total_write": {"sum": {"field": "write"}},
                    "total_operation_time": {"sum": {"field": "operation_time"}},
                    "count": {"value_count": {"field": "site.keyword"}}
                }
            }
        }
    }

    if after_key:
        composite_body["aggs"]["composite_buckets"]["composite"]["after"] = after_key

    return composite_body



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


def estimate_composite_bucket_count(client, start_date, end_date):
    """
    Estimates the number of unique (5-minute interval, filename) pairs
    using a cardinality aggregation with a scripted key.
    """
    query = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {"term": {"site.keyword": "NCAR_OSDF_ORIGIN"}},
                    {"exists": {"field": "filename"}},
                    {
                        "bool": {
                            "must_not": [
                                {"term": {"filename.keyword": ""}},
                                {"term": {"filename.keyword": "missing directory"}}
                            ]
                        }
                    }
                ],
                "filter": [
                    {
                        "range": {
                            "@timestamp": {
                                "gte": start_date,
                                "lte": end_date
                            }
                        }
                    }
                ]
            }
        },
        "aggs": {
            "unique_pairs": {
                "cardinality": {
                    "script": {
                        # Round timestamp to 5-minute buckets and combine with filename
                        "source": "doc['filename.keyword'].value + '|' + (doc['@timestamp'].value.toInstant().toEpochMilli() / 300000)",
                        "lang": "painless"
                    }
                }
            }
        }
    }

    response = client.search(
        body=query,
        index=INDEX,
        request_timeout=TIMEOUT
    )

    return response["aggregations"]["unique_pairs"]["value"]

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--date', type=str, required=True, help='Date to run the script for')
    args = parser.parse_args()

    # Convert to datetime
    target_date = datetime.strptime(args.date, "%Y-%m-%d")
    start_date = (target_date - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
    end_date = target_date.strftime("%Y-%m-%dT00:00:00Z")


    client = OpenSearch(hosts=[HOST], request_timeout=3600, timeout=3600)  # Increased from 120 to 3600 (1 hour)
    
    # Open files for cache entries
    with open(f"{DATA_PATH}/{args.date}-cache.log", "w") as f2_cache:
        try:
            # First get non-NCAR_OSDF_ORIGIN entries
            print("Processing non-NCAR_OSDF_ORIGIN entries...")
            non_origin_query = build_non_origin_query(start_date, end_date)
            response = client.search(
                body=non_origin_query,
                index=INDEX,
                scroll="1h",
                size=10000
            )
            scroll_id = response["_scroll_id"]
            total_hits = response["hits"]["total"]["value"]
            print(f"Total non-NCAR_OSDF_ORIGIN hits: {total_hits}")

            total_batches = math.ceil(total_hits/10000)
            print(f"Total batches: {total_batches}")

            servers = parse_server_response()

            batch_count = 1
            while True:
                print(f"Processing batch {batch_count} of {total_batches}...")
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

                    content = f"[{timestamp}] [Objectname:{filename}] [Site:{site}] [Host:{host}] [Server:{server}] {server_type} {geoip} [AppInfo:{appinfo}] [PelicanClient:{p_client}] [Read:{read}] [Write:{write}] [OpTime:{operation_time}s]\n"

                    write_to_files([f2_cache], content)

                response = client.scroll(scroll_id=scroll_id, scroll="1h")
                if not response["hits"]["hits"]:
                    break

                batch_count += 1

            # Clear the scroll context
            client.clear_scroll(scroll_id=scroll_id)

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


    
    # Open files for origin entries
    with open(f"{DATA_PATH}/{args.date}-origin.log", "w") as f2_origin:
        # Then get aggregated NCAR_OSDF_ORIGIN entries
        print("\nProcessing aggregated NCAR_OSDF_ORIGIN entries...")
        try:
            print("\nEstimating number of composite buckets...")
            estimated_total_buckets = estimate_composite_bucket_count(client, start_date, end_date)
            estimated_pages = (estimated_total_buckets + 499) // 500
            print(f"Estimated total buckets: {estimated_total_buckets}")
            print(f"Estimated pages to fetch (size=500): {estimated_pages}")

            after_key = None
            current_page=1


            while True:
                print(f"\nFetching composite page {current_page} of ~{estimated_pages}...")

                composite_query = build_origin_composite_query(start_date, end_date, after_key)
                response = client.search(
                    body=composite_query,
                    index=INDEX,
                    request_timeout=3600  # Increased from 120 to 3600 (1 hour)
                )
            
                if 'aggregations' not in response:
                    print("No aggregations found in response")
                    print("Response keys:", response.keys())
                    return


                buckets = response["aggregations"]["composite_buckets"]["buckets"]
                print(f"Fetched {len(buckets)} buckets")

                for bucket in buckets:
                    key = bucket["key"]
                    filename = key["filename"]
                    timestamp = key["interval"]
                    total_read = bucket["total_read"]["value"]
                    total_write = bucket["total_write"]["value"]
                    total_operation_time = bucket["total_operation_time"]["value"]
                    count = bucket["count"]["value"]

                    server_type = "[ServerType:origin]"
                    content = f"[{timestamp}] [Objectname:{filename}] [Site:NCAR_OSDF_ORIGIN] {server_type} [Read:{total_read}] [Write:{total_write}] [OpTime:{total_operation_time}s] [Count:{count}]\n"

                    write_to_files([f2_origin], content)

                after_key = response["aggregations"]["composite_buckets"].get("after_key")
                if not after_key:
                    break

                current_page += 1
        
        except Exception as search_err:
            print(f"Error in search query: {search_err}")
            if hasattr(search_err, 'info') and isinstance(search_err.info, dict):
                print_error(search_err.info)
            raise search_err

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

if __name__ == "__main__":
    main()

from datetime import date
import time
import json
import os
import requests
import math
from opensearchpy import OpenSearch
import argparse
from datetime import datetime, timedelta

TIMEOUT = 3600  # Increased from 300 to 3600 (1 hour)

HOST = "https://gracc.opensciencegrid.org/q"
INDEX = "xrd-stash*"
DATA_PATH = "routeviews-access-log"

# The OSDF origin site name for RouteViews (NCAR's is "NCAR_OSDF_ORIGIN").
ORIGIN_SITE = "KENNESAW_OSSTORE_PUBLIC"

# The RouteViews namespace prefix to filter on. There is a single top-level
# namespace ("routeviews/"); the per-collector paths (e.g. route-views6/) live
# beneath it, so filtering on the top-level prefix captures them all.
NAMESPACES = [
    "routeviews/",
]

# Bool clause matching any of the RouteViews namespaces by filename. Used by every
# query so that both the cache and origin streams are restricted to RouteViews data.
# (The origin site KENNESAW_OSSTORE_PUBLIC is a shared origin that also serves large
# volumes of /pelican/monitoring/ health-check traffic, so filtering the origin
# stream by site alone would drown the real accesses in monitoring noise.)
def namespace_should_clause():
    return {
        "bool": {
            "should": [
                {"match_phrase": {"filename": namespace}}
                for namespace in NAMESPACES
            ],
            "minimum_should_match": 1
        }
    }

# Query for non-origin (cache) entries
def build_non_origin_query(start_date, end_date):
    non_origin_query = {
    "size": 10000,
    "_source": ["@timestamp", "filename", "host", "server", "read", "write", "operation_time", "site", "appinfo"],
    "query": {
        "bool": {
            "must": [
                namespace_should_clause()
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
                                    "site.keyword": ORIGIN_SITE
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

# Query for aggregated origin entries
def build_origin_composite_query(start_date, end_date, after_key=None):
    """
    Constructs the composite aggregation query for origin entries.

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
                    {"term": {"site.keyword": ORIGIN_SITE}},
                    {"exists": {"field": "filename"}},
                    namespace_should_clause(),
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
        return None

    if host_str[0] == "[":
        host_str = host_str[1:]

    if host_str[-1] == "]":
        host_str = host_str[:len(host_str)-1]
    # Convert the string to an IPv6 address object

    if host_str.startswith("::"):
        host_str = host_str[2:]

    return host_str

def determine_server_type(site):
    if site == ORIGIN_SITE:
        return "origin"
    elif not site or site == "N/A":
        return "unknown"
    else:
        return "cache"

def is_pelican_client(appinfo):
    # True only for the Pelican client proper: the Go client ("pelican-client/7.25.0")
    # and the XRootD Pelican plugin ("xrdcl-pelican/1.2.1"). Matched as explicit tokens
    # (case-insensitive) so other clients are excluded, including Python/pelicanfs
    # ("Python-urllib/...", "Python/... aiohttp/..."), "xrdcl-curl", curl, and browsers.
    # The client version, when present, is preserved verbatim in the output `appinfo`.
    a = (appinfo or "").lower()
    return "pelican-client" in a or "xrdcl-pelican" in a

def to_epoch_ms(timestamp):
    """
    Converts an OpenSearch @timestamp value to integer epoch milliseconds.

    Cache-stream timestamps arrive as ISO-8601 strings with nanosecond precision
    (e.g. "2025-07-06T23:02:39.026689897Z"). Python's datetime only handles
    microseconds, so the fractional part is truncated to 6 digits before parsing.
    Returns None if the value can't be parsed.
    """
    if timestamp is None or timestamp == "N/A":
        return None
    # Already epoch ms (int or numeric string)?
    if isinstance(timestamp, (int, float)):
        return int(timestamp)
    ts = str(timestamp)
    if ts.isdigit():
        return int(ts)
    # Normalize the trailing Z and truncate sub-microsecond precision.
    ts = ts.replace("Z", "+00:00")
    if "." in ts:
        head, frac = ts.split(".", 1)
        # frac looks like "026689897+00:00" -> keep <=6 fractional digits.
        tz = ""
        for sign in ("+", "-"):
            if sign in frac:
                frac, tz = frac.split(sign, 1)
                tz = sign + tz
                break
        frac = frac[:6]
        ts = f"{head}.{frac}{tz}"
    try:
        dt = datetime.fromisoformat(ts)
        return int(dt.timestamp() * 1000)
    except ValueError as e:
        print(f"Could not parse timestamp {timestamp!r}: {e}")
        return None

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
                    {"term": {"site.keyword": ORIGIN_SITE}},
                    {"exists": {"field": "filename"}},
                    namespace_should_clause(),
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

    # Make sure the output directory exists before opening files in it.
    os.makedirs(DATA_PATH, exist_ok=True)

    client = OpenSearch(hosts=[HOST], request_timeout=3600, timeout=3600)  # Increased from 120 to 3600 (1 hour)

    # Open files for cache entries
    with open(f"{DATA_PATH}/latest-cache.jsonl", "w") as f1_cache, open(f"{DATA_PATH}/{args.date}-cache.jsonl", "w") as f2_cache:
        try:
            # First get non-origin (cache) entries
            print("Processing non-origin (cache) entries...")
            non_origin_query = build_non_origin_query(start_date, end_date)
            response = client.search(
                body=non_origin_query,
                index=INDEX,
                scroll="1h",
                size=10000
            )
            scroll_id = response["_scroll_id"]
            total_hits = response["hits"]["total"]["value"]
            print(f"Total non-origin hits: {total_hits}")

            total_batches = math.ceil(total_hits/10000)
            print(f"Total batches: {total_batches}")

            servers = parse_server_response()

            batch_count = 1
            while True:
                print(f"Processing batch {batch_count} of {total_batches}...")
                for src in response['hits']['hits']:
                    hit = src['_source']
                    # `@timestamp` (ISO-8601, always present, ms precision) is the only
                    # consistent time field across producers; the native epoch `timestamp`
                    # field is absent on RouteViews records and unit-inconsistent elsewhere.
                    timestamp = hit.get('@timestamp', 'N/A')
                    filename = hit.get('filename', 'N/A')
                    host_str = hit.get('host', 'N/A')
                    server = hit.get('server', 'N/A')
                    site = hit.get('site', 'N/A')
                    appinfo = hit.get('appinfo', 'N/A')
                    operation_time = hit.get('operation_time')

                    host = ipv_cleanup(host_str)

                    read = hit.get('read')
                    write = hit.get('write')
                    server_type = determine_server_type(site)

                    p_client = is_pelican_client(appinfo)

                    if site in servers:
                        latitude, longitude = servers[site]
                    else:
                        latitude, longitude = 0, 0

                    record = {
                        "timestamp": to_epoch_ms(timestamp),
                        "object_name": filename,
                        "site": site,
                        "remote_ip": host,
                        "server": server,
                        "server_type": server_type,
                        "latitude": latitude,
                        "longitude": longitude,
                        "appinfo": appinfo,
                        "pelican_client": p_client,
                        "bytes_sent": read,
                        "bytes_rcvd": write,
                        "op_time": operation_time,
                    }

                    content = json.dumps(record) + "\n"

                    write_to_files([f1_cache, f2_cache], content)

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
    with open(f"{DATA_PATH}/latest-origin.jsonl", "w") as f1_origin, open(f"{DATA_PATH}/{args.date}-origin.jsonl", "w") as f2_origin:
        # Then get aggregated origin entries
        print("\nProcessing aggregated origin entries...")
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
                    # Aggregation sums come back as floats; the underlying values are
                    # whole bytes / counts, so emit them as ints for clean JSON.
                    total_read = bucket["total_read"]["value"] or 0
                    total_write = bucket["total_write"]["value"] or 0
                    total_operation_time = bucket["total_operation_time"]["value"] or 0
                    count = bucket["count"]["value"] or 0

                    record = {
                        "timestamp": to_epoch_ms(timestamp),
                        "object_name": filename,
                        "site": ORIGIN_SITE,
                        "server_type": "origin",
                        "bytes_sent": int(total_read),
                        "bytes_rcvd": int(total_write),
                        "op_time": int(total_operation_time),
                        "count": int(count),
                    }
                    content = json.dumps(record) + "\n"

                    write_to_files([f1_origin, f2_origin], content)

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

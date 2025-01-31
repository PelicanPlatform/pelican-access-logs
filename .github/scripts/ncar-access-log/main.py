from datetime import date
import ipaddress

from opensearchpy import OpenSearch

TIMEOUT = 300

HOST = "https://gracc.opensciencegrid.org/q"
INDEX = "xrd-stash*"
DATA_PATH = "ncar-access-log"

query = {
    "size": 10000,
    "_source": ["@timestamp", "filename", "host", "read", "write"],
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
                {
                    "bool": {
                        "should": [
                            {
                                "wildcard": {
                                    "filename": {
                                        "value": "*ncar*"
                                    }
                                }
                            },
                            {
                                "wildcard": {
                                    "filename": {
                                        "value": "*ncar-rda*"
                                    }
                                }
                            },
                            {
                                "wildcard": {
                                    "filename": {
                                        "value": "*ncar-rda-test*"
                                    }
                                }
                            },
                            {
                                "wildcard": {
                                    "filename": {
                                        "value": "*ncar-cesm2-lens*"
                                    }
                                }
                            },
                        ],
                        "minimum_should_match": 1
                    }
                }
            ]
        }
    },
}

def convert_ipv6_to_ipv4(ipv6_str):
    try:

        # Convert the string to an IPv6 address object
        ipv6 = ipaddress.IPv6Address(ipv6_str)

        # Check if it's an embedded IPv4 address
        if ipv6.ipv4_mapped:
            return str(ipv6.ipv4_mapped)  # Return the embedded IPv4 address
        else:
            return None  # Not an IPv6 address with an embedded IPv4 address
    except ValueError:
        return None  # Invalid IPv6 address


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
        f.write(content)

def main():
    client = OpenSearch(hosts=[HOST], request_timeout=TIMEOUT,)
    
    with open(f"{DATA_PATH}/latest.log", "w") as f1, open(f"{DATA_PATH}/{date.today()}.log", "w") as f2:


        try:
            response = client.search(body=query, index=INDEX)
            for src in response['hits']['hits']:
                hit = src['_source']
                timestamp = hit.get('@timestamp', 'N/A')
                filename = hit.get('filename', 'N/A')
                ipv6host = hit.get('host', 'N/A')
                host = convert_ipv6_to_ipv4(ipv6host)
                if host == None:
                    host = 'N/A'

                read = str(hit.get('read', 'N/A'))
                write = str(hit.get('write', 'N/A'))
    
                content = f"[{timestamp}] [Objectname:{filename}] [Host:{host}] [Read:{read}] [Write:{write}]\n"

                write_to_files([f1, f2], content)



        except KeyboardInterrupt:
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

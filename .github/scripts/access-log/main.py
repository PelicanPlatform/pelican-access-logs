from datetime import datetime

from opensearchpy import OpenSearch

TIMEOUT = 300

HOST = "https://gracc.opensciencegrid.org/q"
INDEX = "xrd-stash*"
DATA_PATH = "access-log"

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


def main():
    client = OpenSearch(hosts=[HOST], request_timeout=TIMEOUT,)
    
    with open(f"{DATA_PATH}/latest.log", "w") as f:


        try:
            response = client.search(body=query, index=INDEX)
            for src in response['hits']['hits']:
                hit = src['_source']
                timestamp = hit.get('@timestamp', 'N/A')
                filename = hit.get('filename', 'N/A')
                host = hit.get('host', 'N/A')
                read = str(hit.get('read', 'N/A'))
                write = str(hit.get('write', 'N/A'))
    
                f.write(f"[{timestamp}] [Objectname:{filename}] [Host:{host}] [Read:{read}] [Write:{write}]")



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

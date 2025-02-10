from datetime import date
import ipaddress

from opensearchpy import OpenSearch

TIMEOUT = 300

HOST = "https://gracc.opensciencegrid.org/q"
INDEX = "xrd-stash*"
DATA_PATH = "ncar-access-log"

query = {
    "size": 10000,
    "_source": ["@timestamp", "filename", "host", "server", "read", "write"],
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

def ipv_cleanup(host_str):
    if host_str[0] == "[":
        host_str = host_str[1:]
        
    if host_str[-1] == "]":
        host_str = host_str[:len(host_str)-1]
    # Convert the string to an IPv6 address object

    if host_str.startswith("::"):
        host_str = host_str[2:]
        
    return host_str 


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
                host_str = hit.get('host', 'N/A')
                server = hit.get('server', 'N/A')

                host = ipv_cleanup(host_str)
                if host == None:
                    host = host_str

                read = str(hit.get('read', 'N/A'))
                write = str(hit.get('write', 'N/A'))
    
                content = f"[{timestamp}] [Objectname:{filename}] [Host:{host}] [Server:{server}] [Read:{read}] [Write:{write}]\n"

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

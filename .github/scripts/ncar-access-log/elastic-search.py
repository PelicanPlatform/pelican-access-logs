from datetime import date

from opensearchpy import OpenSearch

TIMEOUT = 300

HOST = "https://elastic.osg.chtc.io/q"
INDEX = "adstash-ospool-transfer*"
DATA_PATH = "test-elastic-search"

query = {
    "size": 1000,
    "_source": ["Endpoint", "TransferType", "TransferProtocol", "TransferError"],
    "query": {
        "bool": {
            "filter": [
                {
                    "range": {
                        "TransferStartTime": {
                            "gte": "now-2d/d",
                            "lte": "now/d"
                        }
                    }
                },
                {
                    "term": {
                        "TransferSuccess": False
                    }
                }
             ],
        }
    }
}

def main():
    client = OpenSearch(
        hosts=[HOST],
        http_auth=('username', 'password'),  # Replace with your actual credentials
        request_timeout=TIMEOUT,
    )
    
   
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

    batch_count = 1
    while True:
        print(f"Processing batch {batch_count}...")
        for src in response['hits']['hits']:
            hit = src['_source']
            content = f"{hit.get("Endpoint")} [{hit.get("TransferType")}] [{hit.get("TransferProtocol")}] {hit.get("TransferError")}"
            print(content)

        response = client.scroll(scroll_id=scroll_id, scroll="2m")  # Get the next batch of results
        if not response["hits"]["hits"]:
            break

        batch_count += 1


if __name__ == "__main__":
    main()
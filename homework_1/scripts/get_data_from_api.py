import itertools
import logging
import os

import dotenv
import requests
import ujson


logging.basicConfig(level=logging.INFO)
dotenv.load_dotenv()


if __name__ == "__main__":
    n_api_requests = int(os.getenv("N_API_REQUESTS"))
    
    records = list(itertools.chain.from_iterable(
        requests.get(os.getenv("API_GET_ENDPOINT")).json() 
        for _ in range(n_api_requests)
    ))
    logging.info(f"Got {len(list(records))} records from API endpoint {os.getenv('API_GET_ENDPOINT')}")
    
    with open(os.getenv("OUTPUT_FILE_NAME"), "w") as f:
        ujson.dump(records, f)


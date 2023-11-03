import itertools
import logging
import time
import os

import dotenv
import minio
import pulsar
import requests
import ujson
from tqdm import tqdm


logging.basicConfig(level=logging.INFO)
dotenv.load_dotenv()

OUTPUT_DIR = os.getenv("OUTPUT_DIR")
OUTPUT_FILE_NAME = os.getenv("OUTPUT_FILE_NAME")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, OUTPUT_FILE_NAME)


if __name__ == "__main__":
    n_api_requests = int(os.getenv("N_API_REQUESTS"))

    logging.info(f"Getting batched records from API endpoint {os.getenv('API_GET_ENDPOINT')}...")
    records = list(itertools.chain.from_iterable(
        requests.get(os.getenv("API_GET_ENDPOINT")).json() 
        for _ in tqdm(range(n_api_requests))
    ))
    n_records = len(records)
    logging.info(f"Got {n_records} records.")

    logging.info("Pushing received records to pulsar queue...")
    client = pulsar.Client(os.getenv("PULSAR_URL"))
    producer = client.create_producer(
                os.getenv("PULSAR_TOPIC"),
                block_if_queue_full=True,
                batching_enabled=True,
                batching_max_publish_delay_ms=10
            )
    for record in tqdm(records):
        producer.send_async(ujson.dumps(record).encode("utf-8"), lambda res, msg: None)
    logging.info("Done.")
    time.sleep(6)

    logging.info("Retrieving records from pulsar queue...")
    client = pulsar.Client(os.getenv("PULSAR_URL"))
    consumer = client.subscribe(os.getenv("PULSAR_TOPIC"), os.getenv("PULSAR_SUBSCRIPTION"))
    records = []
    counter = 0
    while True:
        msg = consumer.receive()
        try:
            records.append(ujson.loads(msg.data().decode("utf-8")))
            consumer.acknowledge(msg)
            counter += 1
        except Exception as e:
            logging.error(e)
            consumer.negative_acknowledge(msg)
        if counter% 1000 == 0:
            logging.info(f"Received {counter} records")
        if len(records) == n_records:
            break
    logging.info(f"Read {len(records)} records from the queue.")
    client.close()
    print(records[-1])

    logging.info(f"Writing records to local file {OUTPUT_PATH}...")
    with open(OUTPUT_PATH, "w") as f:
        ujson.dump(records, f)
    logging.info("Done.")

    logging.info("Uploading file to MinIO...")
    logging.info("Connecting to MinIO...")
    client = minio.Minio(
        endpoint=os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )

    bucket_name = os.getenv("MINIO_BUCKET")
    logging.info(f"Creating bucket {bucket_name}...")
    if not client.bucket_exists(bucket_name):
        client.make_bucket(os.getenv("MINIO_BUCKET"))
    logging.info("Bucket created.")

    client.fput_object(
        "dev", OUTPUT_FILE_NAME, OUTPUT_PATH, content_type="application/json"
    )
    logging.info("File uploaded to MinIO.")


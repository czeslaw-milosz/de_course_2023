import logging
import os

import dotenv
import minio
import ujson
from tqdm import tqdm

import utils


logging.basicConfig(level=logging.INFO)
dotenv.load_dotenv()

OUTPUT_DIR = os.getenv("OUTPUT_DIR")
OUTPUT_FILE_NAME = os.getenv("OUTPUT_FILE_NAME")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, OUTPUT_FILE_NAME)
N_RECORDS = int(os.getenv("N_RECORDS"))


if __name__ == '__main__':
    logging.info(f"Creating {N_RECORDS} records in {OUTPUT_FILE_NAME}...")
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    with open(OUTPUT_PATH, "w") as f:
        records = [utils.generate_random_record() for _ in tqdm(range(N_RECORDS))]
        ujson.dump(records, f)

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
    

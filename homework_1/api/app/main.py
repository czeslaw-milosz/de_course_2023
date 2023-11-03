import os

import dotenv
from fastapi import FastAPI

from app import utils

dotenv.load_dotenv()
app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/mock_data/")
def read_mock_data():
    return [utils.generate_random_record() for _ in range(int(os.getenv("API_BATCH_SIZE")))]

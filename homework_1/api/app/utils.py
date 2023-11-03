import datetime
import random
import string
import uuid
from typing import Dict

import names
import randomtimestamp


def generate_random_record() -> Dict[str, str|int|datetime.datetime]:
    return {
            "user_id": str(uuid.uuid4()),
            "content_id": random.randint(0, 1_000_000),
            "name": names.get_full_name(),
            "timestamp": randomtimestamp.randomtimestamp(start_year=1999, end_year=2024),
            "content": generate_random_text(150),
    }


def generate_random_text(length: int) -> str:
    return "".join(random.choices(string.ascii_letters, k=length))

import random
import string
import uuid
from typing import Dict

import names


def generate_random_record() -> Dict[str, str|int]:
    return {
            "user_id": str(uuid.uuid4()),
            "content_id": random.randint(0, 1_000_000),
            "name": names.get_full_name(),
            "age": random.randint(18, 100),
            "content": generate_random_text(150),
    }


def generate_random_text(length: int) -> str:
    return "".join(random.choices(string.ascii_letters, k=length))

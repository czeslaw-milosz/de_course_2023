import random
import string
import uuid

import names
import ujson


def generate_random_record() -> str:
    return ujson.dumps(
        {
            "user_id": str(uuid.uuid4()),
            "comment_id": random.randint(0, 1_000_000),
            "name": names.get_full_name(),
            "age": random.randint(18, 100),
            "content": generate_random_text(100),
        }
    )


def generate_random_text(length: int) -> str:
    return "".join(random.choices(string.ascii_letters, k=length))
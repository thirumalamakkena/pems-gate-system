import json
import random
import time
from datetime import datetime, UTC

from kafka import KafkaProducer

from app.config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    QR_SCAN_TOPIC
)

from app.utils.event_id import generate_event_id


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

PEMS = [
    "PEMS-A", "PEMS-B", "PEMS-C", "PEMS-D", "PEMS-E",
    "PEMS-F", "PEMS-G", "PEMS-H", "PEMS-I", "PEMS-J"
]

USERS = [
    f"USR{i:03d}"
    for i in range(1, 101)
]

duration = 20  # seconds
start = time.time()

while time.time() - start < duration:

    event = {
        "eventId": generate_event_id(),
        "userId": random.choice(USERS),
        "pemId": random.choice(PEMS),
        "scanTimestamp": datetime.now(UTC).isoformat(),
        "source": "SIMULATOR"
    }

    producer.send(
        QR_SCAN_TOPIC,
        event
    )

    producer.flush()

    print(event)

    time.sleep(random.uniform(0.1, 1.0))
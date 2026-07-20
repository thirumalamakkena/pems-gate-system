import json
import random
import time

from kafka import KafkaProducer

from app.config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    QR_SCAN_TOPIC
)
from app.utils.current_time_stamp import CurrentTimeStamp
from app.utils.event_builder import EventBuilder
from app.utils.event_id_generator import generate_event_id


stamp = CurrentTimeStamp()

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

MAX_EVENTS = 1000
RUN_DURATION = 60 * 60  # 60 minutes

start_time = time.time()
events_sent = 0

print("=" * 60)
print("Starting PEMS Traffic Simulator")
print(f"Duration   : {RUN_DURATION // 60} minutes")
print(f"Max Events : {MAX_EVENTS}")
print("=" * 60)

while (
    time.time() - start_time < RUN_DURATION
    and events_sent < MAX_EVENTS
):

    event = EventBuilder.qr_scan_event(
        event_id=generate_event_id(),
        user_id=random.choice(USERS),
        pem_id=random.choice(PEMS),
        event_time=stamp.current_time_iso(),
        source="SIMULATOR"
    )

    producer.send(QR_SCAN_TOPIC, event)

    events_sent += 1

    elapsed = int(time.time() - start_time)

    print(
        f"[{events_sent:03d}/{MAX_EVENTS}] "
        f"Time={elapsed:4d}s | "
        f"User={event['payload']['userId']} | "
        f"Gate={event['payload']['pemId']}"
    )

    # --------------------------------------------------------
    # Realistic traffic pattern
    #
    # 20% -> Rush traffic
    # 40% -> Normal traffic
    # 40% -> Quiet traffic
    # --------------------------------------------------------

    traffic = random.random()

    if traffic < 0.20:
        # Rush traffic
        delay = random.uniform(0.3, 1.0)

    elif traffic < 0.60:
        # Normal traffic
        delay = random.uniform(2.0, 4.0)

    else:
        # Quiet traffic
        delay = random.uniform(5.0, 8.0)

    time.sleep(delay)

producer.flush()
producer.close()

print("\n" + "=" * 60)
print("Simulation Completed")
print(f"Events Generated : {events_sent}")
print(f"Elapsed Time     : {int(time.time() - start_time)} seconds")
print("=" * 60)
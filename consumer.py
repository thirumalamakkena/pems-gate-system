from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient

import json


# ==================================
# MongoDB
# ==================================

MONGODB_URI = "mongodb+srv://admin:auYyyyVgYCcBVwWc@cluster0.mckklym.mongodb.net/?appName=Cluster0"

client = MongoClient(MONGODB_URI)

db = client["pems"]

users_collection = db["users"]


# ==================================
# Load Users Into Cache
# ==================================

user_cache = {}

print("Loading users into cache...")

for user in users_collection.find({}, {"_id": 0}):

    user_cache[user["userId"]] = user

print(f"Loaded {len(user_cache)} users")


# ==================================
# Kafka Producer
# ==================================

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


# ==================================
# Kafka Consumer
# ==================================

consumer = KafkaConsumer(
    "qr-scans",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    group_id="validation-group",
    value_deserializer=lambda x: json.loads(
        x.decode("utf-8")
    )
)

print("Listening on qr-scans...")


# ==================================
# Validation Logic
# ==================================

for message in consumer:

    event = message.value

    print("Received:", event)

    user_id = event["userId"]
    pem_id = event["pemId"]

    user = user_cache.get(user_id)

    # --------------------------
    # Validate
    # --------------------------

    if user is None:

        status = "INVALID_USER"

    elif user["status"] != "ACTIVE":

        status = "INACTIVE_USER"

    elif pem_id not in user["allowedPems"]:

        status = "ACCESS_DENIED"

    else:

        status = "VALID"

    result = {
        "eventId": event["eventId"],
        "userId": user_id,
        "pemId": pem_id,
        "status": status
    }

    print("Result:", result)

    producer.send(
        "validation-results",
        value=result
    )

    producer.flush()
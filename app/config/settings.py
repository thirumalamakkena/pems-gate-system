from dotenv import load_dotenv
import os

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")

DATABASE_NAME = os.getenv("DATABASE_NAME")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS"
)

QR_SCAN_TOPIC = os.getenv(
    "QR_SCAN_TOPIC"
)

VALIDATION_RESULTS_TOPIC = os.getenv(
    "VALIDATION_RESULTS_TOPIC"
)
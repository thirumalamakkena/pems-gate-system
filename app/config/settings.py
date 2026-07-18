from dotenv import load_dotenv
import os,json

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

RETRY_VALIDATION_TOPIC = os.getenv(
    "RETRY_VALIDATION_TOPIC"
)

DEAD_LETTER_TOPIC = os.getenv(
    "DEAD_LETTER_TOPIC"
)

MAX_RETRY_ATTEMPTS = int(os.getenv(
    "MAX_RETRY_ATTEMPTS"
))

INITIAL_RETRY_DELAY = int(os.getenv(
    "INITIAL_RETRY_DELAY"
))

ACTIVE_CONNECTIONS = json.loads(os.getenv(
    "ACTIVE_CONNECTIONS"
))
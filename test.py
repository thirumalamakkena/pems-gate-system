from app.config.kafka import producer
from app.config.settings import QR_SCAN_TOPIC
from datetime import datetime, timezone

event = {
    "eventId": "EVT-20260629-0000021",
    "userId": "USR001",
    "pemId": "PEMS-A",
    "timestamp": datetime.now().isoformat()
}

producer.send(QR_SCAN_TOPIC, event)
producer.flush()

print("Event Sent")
from datetime import datetime

from app.config.kafka import (
    producer
)
from app.config.settings import (
    RETRY_VALIDATION_TOPIC
)

from app.repositories.dead_letter_repository import (
    DeadLetterRepository
)

dlq_repo = DeadLetterRepository()


print("Replying dlq events started.....")

unreplyed_events = dlq_repo.get_all_unreplayed()

for event in  unreplyed_events:
    try:
        event["metadata"]["retryCount"] = 0

        event["metadata"]["nextRetryAt"] = None

        event["metadata"]["eventType"] = "VALIDATION_RETRY"

        event["metadata"]["producer"] = "dlq-replay"

        event["metadata"]["replayedAt"] = event["metadata"]["replayedAt"].isoformat()


        producer.send(
            RETRY_VALIDATION_TOPIC,
            event
        )

        producer.flush()
        
        event_id = event["metadata"]["eventId"]
        dlq_repo.mark_replayed(event_id)
        print(f"event {event_id} : Sent to Retry validation topic and marked as replayed...")

    except Exception as e:
        print(
            f"Error processing event: {e}"
        )
        


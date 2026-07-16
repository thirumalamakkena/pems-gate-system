from app.config.kafka import (
    dead_letter_consumer
)

from app.metrics import metrics_buffer
from app.metrics.metrics_flusher import MetricsFlusher

from app.repositories.dead_letter_repository import (
    DeadLetterRepository
)

MetricsFlusher().start()

dead_letter_repository = DeadLetterRepository()
print("dlq consumer is listening....")
for message in dead_letter_consumer:

    try:
        event = message.value

        metadata = event["metadata"]
        payload = event["payload"]

        print("=" * 60)
        print("DEAD LETTER EVENT")
        print(f"Event ID      : {metadata['eventId']}")
        print(f"Retry Count   : {metadata['retryCount']}")
        print(f"Failure Reason: {payload['failureReason']}")
        print("=" * 60)

        dead_letter_repository.insert(event)

        dead_letter_consumer.commit()

        metrics_buffer.increment(
            "dlq-consumer",
            "eventsProcessed"
        )
    
    except Exception as e:

        metrics_buffer.increment(
            "dlq-consumer",
            "eventsFailed"
        )


        print(
            f"DLQ Error: {str(e)}"
        )

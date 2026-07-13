from app.config.kafka import (
    dead_letter_consumer
)

from app.repositories.dead_letter_repository import (
    DeadLetterRepository
)

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
    
    except Exception as e:
        print(
            f"Error processing event: {e}"
        )

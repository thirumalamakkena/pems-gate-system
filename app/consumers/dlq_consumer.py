import atexit
from app.metrics import metrics_buffer
from app.metrics.metrics_flusher import MetricsFlusher
from prometheus_client import start_http_server
from app.observability import prometheus_metrics

from app.repositories.dead_letter_repository import (
    DeadLetterRepository
)

from app.config.kafka import (
    dead_letter_consumer
)

SERVICE_NAME = "dlq-consumer"

MetricsFlusher().start()

dead_letter_repository = DeadLetterRepository()
print("dlq consumer is listening....")

start_http_server(8004)

prometheus_metrics.consumer_started(
    SERVICE_NAME
)

atexit.register(
    lambda: prometheus_metrics.consumer_stopped(
        SERVICE_NAME
    )
)


for message in dead_letter_consumer:

    try:
        event = message.value

        prometheus_metrics.record_kafka_consumed(
            service=SERVICE_NAME,
            topic="dead-letter-validation"
        )

        metadata = event["metadata"]
        payload = event["payload"]

        print("=" * 60)
        print("DEAD LETTER EVENT")
        print(f"Event ID      : {metadata['eventId']}")
        print(f"Retry Count   : {metadata['retryCount']}")
        print(f"Failure Reason: {payload['failureReason']}")
        print("=" * 60)

        dead_letter_repository.insert(event)

        prometheus_metrics.record_mongodb_write(
            service=SERVICE_NAME,
            collection="dead_letter_events"
        )

        dead_letter_consumer.commit()

        metrics_buffer.increment(
            SERVICE_NAME,
            "eventsProcessed"
        )

        prometheus_metrics.record_processed_event(
            SERVICE_NAME
        )
    
    except Exception as e:

        metrics_buffer.increment(
            SERVICE_NAME,
            "eventsFailed"
        )

        prometheus_metrics.record_failed_event(
            SERVICE_NAME
        )


        print(
            f"DLQ Error: {str(e)}"
        )

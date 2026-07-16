from app.utils.current_time_stamp import CurrentTimeStamp
from app.metrics import metrics_buffer
from app.metrics.metrics_flusher import MetricsFlusher
from app.config.kafka import (
    audit_consumer
)

from app.repositories.audit_repository import (
    AuditRepository
)


time_stamp = CurrentTimeStamp()

audit_repository = AuditRepository()

MetricsFlusher().start()

print("Audit Consumer Started...")

for message in audit_consumer:

    try:

        event = message.value

        metadata = event["metadata"]
        payload = event["payload"]


        event["recordedAt"] = time_stamp.current_time()

        audit_repository.insert_event(
            event
        )

        audit_consumer.commit()

        metrics_buffer.increment(
            "audit-consumer",
            "eventsProcessed"
        )

        print(
            f"Audit Saved: {metadata['eventId']}"
        )

    except Exception as e:

        metrics_buffer.increment(
            "audit-consumer",
            "eventsFailed"
        )

        print(
            f"Audit Error: {str(e)}"
        )
from datetime import datetime, timezone

from app.config.kafka import (
    audit_consumer
)

from app.repositories.audit_repository import (
    AuditRepository
)

from app.repositories.metrics_repository import (
    MetricsRepository
)


audit_repository = AuditRepository()
metrics_repository = MetricsRepository()

print("Audit Consumer Started...")

for message in audit_consumer:

    try:

        event = message.value

        event["scanTimestamp"] = datetime.fromisoformat(
            event["scanTimestamp"]
        )

        event["recordedAt"] = datetime.now()

        audit_repository.insert_event(
            event
        )

        metrics_repository.increment_processed(
            "audit-consumer"
        )

        print(
            f"Audit Saved: {event['eventId']}"
        )

    except Exception as e:

        metrics_repository.increment_failed(
            "audit-consumer"
        )

        print(e)
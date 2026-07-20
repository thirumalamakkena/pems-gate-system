import atexit
from datetime import datetime
from app.utils.current_time_stamp import CurrentTimeStamp
from app.metrics import metrics_buffer
from app.metrics.metrics_flusher import MetricsFlusher
from app.config.kafka import (
    audit_consumer
)

from app.repositories.audit_repository import (
    AuditRepository
)

from prometheus_client import start_http_server
from app.observability import prometheus_metrics

SERVICE_NAME = "audit-consumer"

time_stamp = CurrentTimeStamp()

audit_repository = AuditRepository()

MetricsFlusher().start()

print("Audit Consumer Started...")

start_http_server(8005)

prometheus_metrics.consumer_started(
    SERVICE_NAME
)

atexit.register(
    lambda: prometheus_metrics.consumer_stopped(
        SERVICE_NAME
    )
)

for message in audit_consumer:

    try:

        event = message.value

        prometheus_metrics.record_kafka_consumed(
            service=SERVICE_NAME,
            topic="validation-results"
        )

        metadata = event["metadata"]
        payload = event["payload"]

        metadata["eventTime"] = datetime.fromisoformat(
            metadata["eventTime"]
        )

        metadata["arrivedAt"] = datetime.fromisoformat(
            metadata["arrivedAt"]
        )

        metadata["processedAt"] = datetime.fromisoformat(
            metadata["processedAt"]
        )


        event["recordedAt"] = time_stamp.current_time()

        audit_repository.insert_event(
            event
        )

        prometheus_metrics.record_mongodb_write(
            service=SERVICE_NAME,
            collection="audit_logs"
        )

        audit_consumer.commit()

        metrics_buffer.increment(
            SERVICE_NAME,
            "eventsProcessed"
        )

        prometheus_metrics.record_processed_event(
            SERVICE_NAME
        )

        print(
            f"Audit Saved: {metadata['eventId']}"
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
            f"Audit Error: {str(e)}"
        )
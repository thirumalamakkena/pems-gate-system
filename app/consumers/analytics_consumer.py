import time,atexit  
from datetime import datetime
from app.metrics import metrics_buffer
from app.metrics.metrics_flusher import MetricsFlusher
from prometheus_client import start_http_server
from app.observability import prometheus_metrics

from app.config.kafka import (
    analytics_consumer
)

from app.repositories.analytics_repository import (
    AnalyticsRepository
)

SERVICE_NAME = "analytics-consumer"

analytics_repository = AnalyticsRepository()

MetricsFlusher().start()

print("Analytics Consumer Started...")
print("Listening on validation-results...")

start_http_server(8002)

prometheus_metrics.consumer_started(
    SERVICE_NAME
)

atexit.register(
    lambda: prometheus_metrics.consumer_stopped(
        SERVICE_NAME
    )
)

for message in analytics_consumer:

    try:
        start_time = time.time()
        event = message.value

        prometheus_metrics.record_kafka_consumed(
            service=SERVICE_NAME,
            topic="validation-results"
        )

        metadata = event["metadata"]
        payload = event["payload"]

        end_time = time.time()

        processing_time_ms = round(
            (end_time - start_time) * 1000,
            2
        )

        metadata["eventTime"] = datetime.fromisoformat(
            metadata["eventTime"]
        )
        
        analytics_repository.update_hourly_metrics(
            {**payload,
             "processingTimeMs":processing_time_ms,
             "eventTime": metadata["eventTime"]
            }
        )

        prometheus_metrics.record_mongodb_write(
            service=SERVICE_NAME,
            collection="analytics_hourly"
        )

        analytics_consumer.commit()

        
        metrics_buffer.increment(
            SERVICE_NAME,
            "eventsProcessed"
        )

        prometheus_metrics.record_processed_event(
            SERVICE_NAME
        )

        metrics_buffer.add(
            SERVICE_NAME,
            "totalProcessingLatencyMs",
            processing_time_ms
        )

        prometheus_metrics.record_processing_latency(
            SERVICE_NAME,
            processing_time_ms
        )


        print(
            f"Analytics Updated: {metadata["eventId"]}"
        )

    except Exception as e:

        metrics_buffer.increment(
            SERVICE_NAME,
            "eventsFailed"
        )

        prometheus_metrics.record_failed_event(
            SERVICE_NAME
        )

        print(f"Analytics Error: {str(e)}")
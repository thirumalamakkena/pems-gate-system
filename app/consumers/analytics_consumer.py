import time  
from datetime import datetime
from app.metrics import metrics_buffer
from app.metrics.metrics_flusher import MetricsFlusher

from app.config.kafka import (
    analytics_consumer
)

from app.repositories.analytics_repository import (
    AnalyticsRepository
)


analytics_repository = AnalyticsRepository()

MetricsFlusher().start()

print("Analytics Consumer Started...")
print("Listening on validation-results...")

for message in analytics_consumer:

    try:
        start_time = time.time()
        event = message.value

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

        analytics_consumer.commit()

        
        metrics_buffer.increment(
            "analytics-consumer",
            "eventsProcessed"
        )

        metrics_buffer.add(
            "analytics-consumer",
            "totalProcessingLatencyMs",
            processing_time_ms
        )



        print(
            f"Analytics Updated: {metadata["eventId"]}"
        )

    except Exception as e:

        metrics_buffer.increment(
            "analytics-consumer",
            "eventsFailed"
        )

        print(f"Analytics Error: {str(e)}")

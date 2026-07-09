from datetime import datetime
import time  

from app.config.kafka import (
    analytics_consumer
)

from app.repositories.analytics_repository import (
    AnalyticsRepository
)

from app.repositories.metrics_repository import (
    MetricsRepository
)


analytics_repository = AnalyticsRepository()
metrics_repository = MetricsRepository()

print("Analytics Consumer Started...")
print("Listening on validation-results...")

for message in analytics_consumer:

    try:
        start_time = time.time()
        event = message.value

        print(f"Received: {event}")

        event["scanTimestamp"] =  datetime.fromisoformat(
            event["scanTimestamp"]
        )

        analytics_repository.update_hourly_metrics(
            event
        )
        
        end_time = time.time()

        processing_time_ms = round(
            (end_time - start_time) * 1000,
            2
        )

        metrics_repository.update_latency("analytics-consumer",processing_time_ms)
        
        metrics_repository.increment_processed(
            "analytics-consumer"
        )


        print(
            f"Analytics Updated: {event['eventId']}"
        )

    except Exception as e:

        metrics_repository.increment_failed(
            "analytics-consumer"
        )

        print(f"Analytics Error: {str(e)}")
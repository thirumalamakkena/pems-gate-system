import time,atexit
from prometheus_client import start_http_server
from app.observability import prometheus_metrics
from app.metrics import metrics_buffer
from app.metrics.metrics_flusher import MetricsFlusher
from app.utils.current_time_stamp import (
    CurrentTimeStamp
)

from app.config.kafka import (
    validation_consumer,
    producer
)

from app.config.settings import (
    VALIDATION_RESULTS_TOPIC,
    RETRY_VALIDATION_TOPIC,
)

from app.services.validation_service import (
    ValidationService
)

from app.repositories.metrics_repository import (
    MetricsRepository
)

SERVICE_NAME = "validation-consumer"


metrics_repository = MetricsRepository()

validation_service = ValidationService()
current_time_stamp = CurrentTimeStamp()

MetricsFlusher().start()

print("Validation Consumer Started...")
print("Listening on qr-scans topic...")

start_http_server(8001)

prometheus_metrics.consumer_started(
    SERVICE_NAME
)

atexit.register(
    lambda: prometheus_metrics.consumer_stopped(
        SERVICE_NAME
    )
)

for message in validation_consumer:

    try:
        arrived_at = current_time_stamp.current_time_iso()
        start_time = time.time()

        event = message.value

        prometheus_metrics.record_kafka_consumed(
            service=SERVICE_NAME,
            topic="qr-scans"
        )


        metadata = event["metadata"]
        payload = event["payload"]

        user_id = payload["userId"]
        pem_id = payload["pemId"]


        print(
            f"Partition={message.partition} "
            f"Offset={message.offset} "
            f"Event={metadata['eventId']}"
        )


        if user_id == "USR095" or user_id == "USR010":
            raise Exception("Stimulated exception")

        validation_result = (
            validation_service.validate(
                    user_id,
                    pem_id
                )
            )
        
        prometheus_metrics.record_validation_result(
            validation_result["status"]
        )

        processed_at = current_time_stamp.current_time_iso()
        end_time = time.time()

        processing_time_ms = round(
            (end_time - start_time) * 1000,
            2
        )

        kafka_topic_result = {
            "metadata":{
                **metadata,
                "eventType": "VALIDATION_COMPLETED",
                "producer": SERVICE_NAME,
                "arrivedAt": arrived_at,
                "processedAt": processed_at
            }
             ,
             "payload":{
                 **payload,
                 "validationStatus": validation_result["status"]
             }
        }

        producer.send(
            VALIDATION_RESULTS_TOPIC,
            kafka_topic_result
        )

        producer.flush()

        prometheus_metrics.record_kafka_produced(
            service=SERVICE_NAME,
            topic=VALIDATION_RESULTS_TOPIC
        )

        validation_consumer.commit()

        cache_hits = validation_service.get_cache_hits()

        user_cache_hits = cache_hits.get("user")
        gateway_cache_hits = cache_hits.get("gateway")

        if user_cache_hits.get("userCacheHit") :
            metrics_buffer.increment(
                SERVICE_NAME,
                "userCacheHit"
            )

            prometheus_metrics.record_cache_hit(
                SERVICE_NAME
            )
        else:
            metrics_buffer.increment(
                SERVICE_NAME,
                "userCacheMiss"
            )

            prometheus_metrics.record_cache_miss(
                SERVICE_NAME
            )


        if gateway_cache_hits.get("gatewayCacheHit") :
            metrics_buffer.increment(
                SERVICE_NAME,
                "gatewayCacheHit"
            )
        else:
            metrics_buffer.increment(
                SERVICE_NAME,
                "gatewayCacheMiss"
            )


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
       
    except Exception as e:
        
        metrics_buffer.increment(
            SERVICE_NAME,
            "eventsFailed"
        )

        prometheus_metrics.record_failed_event(
            SERVICE_NAME
        )

        metadata["retryCount"] += 1

        metadata["eventType"] = "VALIDATION_RETRY"
        metadata["nextRetryAt"]  = current_time_stamp.next_retry_time()

        prometheus_metrics.record_retry_enqueued(
            SERVICE_NAME
        )

        producer.send(
            RETRY_VALIDATION_TOPIC,
            event
        )

        producer.flush()

        prometheus_metrics.record_kafka_produced(
            service=SERVICE_NAME,
            topic=RETRY_VALIDATION_TOPIC
        )

        validation_consumer.commit()

        print(
            f"Error processing event: {e}"
        )


        
       


                    

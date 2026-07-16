import time

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
metrics_repository = MetricsRepository()

validation_service = ValidationService()
current_time_stamp = CurrentTimeStamp()

MetricsFlusher().start()

print("Validation Consumer Started...")
print("Listening on qr-scans topic...")


for message in validation_consumer:

    try:
        arrived_at = current_time_stamp.current_time_iso()
        start_time = time.time()

        event = message.value


        metadata = event["metadata"]
        payload = event["payload"]

        user_id = payload["userId"]
        pem_id = payload["pemId"]

        print(
            f"Partition={message.partition} "
            f"Offset={message.offset} "
            f"Event={metadata['eventId']}"
        )

        validation_result = (
            validation_service.validate(
                    user_id,
                    pem_id
                )
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
                "producer": "validation-consumer",
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

        validation_consumer.commit()

        metrics_buffer.increment(
            "validation-consumer",
            "eventsProcessed"
        )

        metrics_buffer.add(
            "validation-consumer",
            "totalProcessingLatencyMs",
            processing_time_ms
        )
       
    except Exception as e:
        
        metrics_buffer.increment(
            "validation-consumer",
            "eventsFailed"
        )

        metadata["retryCount"] += 1

        metadata["eventType"] = "VALIDATION_RETRY"
        metadata["nextRetryAt"]  = current_time_stamp.next_retry_time()

        producer.send(
            RETRY_VALIDATION_TOPIC,
            event
        )

        validation_consumer.commit()

        print(
            f"Error processing event: {e}"
        )


        
       


                    

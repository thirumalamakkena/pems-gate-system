import time
from datetime import datetime
from app.metrics import metrics_buffer
from app.metrics.metrics_flusher import MetricsFlusher
from app.utils.current_time_stamp import (
    CurrentTimeStamp
)
 
from app.config.kafka import (
    retry_consumer,
    producer
)

from app.config.settings import (
    RETRY_VALIDATION_TOPIC,
    DEAD_LETTER_TOPIC,
    VALIDATION_RESULTS_TOPIC,
    MAX_RETRY_ATTEMPTS
)

from app.services.validation_service import (
    ValidationService
)

MetricsFlusher().start()
validation_service = ValidationService()

current_time_stamp = CurrentTimeStamp()

print("Retry Consumer Started...")
print("Listening on retry topic...")


for message in retry_consumer:

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

        
        if metadata["nextRetryAt"] != None:

            now = current_time_stamp.current_time()
            next_retry = datetime.fromisoformat(
                metadata["nextRetryAt"].replace("Z", "+00:00")
            )

            if now < next_retry:
                time.sleep((next_retry - now).total_seconds())

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
                "producer": "retry-consumer",
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

        retry_consumer.commit() 

        metrics_buffer.increment(
            "retry-consumer",
            "eventsProcessed"
        )

        metrics_buffer.add(
            "retry-consumer",
            "totalProcessingLatencyMs",
            processing_time_ms
        )

    except Exception as e:

        metrics_buffer.increment(
            "retry-consumer",
            "eventsFailed"
        )

        retry_count = int(metadata["retryCount"])
        if retry_count < MAX_RETRY_ATTEMPTS:

            metadata["eventType"] = "VALIDATION_RETRY"

            metadata["producer"] = "retry-consumer"
            metadata["retryCount"] += 1
            metadata["nextRetryAt"] = current_time_stamp.next_retry_time()

            producer.send(
                RETRY_VALIDATION_TOPIC,
                event
            )

        else:

            event["metadata"]["eventType"] = "VALIDATION_FAILED"

            event["payload"]["failureReason"] = str(e)
            metadata.update({
                "replayed": False,
                "replayedBy": None
            })

            producer.send(
                DEAD_LETTER_TOPIC,
                event
            )

        retry_consumer.commit()

        print(
            f"Error processing event: {e}"
        )


                    

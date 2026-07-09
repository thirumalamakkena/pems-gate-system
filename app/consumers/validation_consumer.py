import time
from datetime import datetime, timezone
import traceback
from pymongo.errors import DuplicateKeyError


from app.config.kafka import (
    validation_consumer,
    producer
)

from app.config.settings import (
    VALIDATION_RESULTS_TOPIC
)

from app.services.validation_service import (
    ValidationService
)

from app.repositories.scan_repository import (
    ScanRepository
)

from app.repositories.metrics_repository import (
    MetricsRepository
)

metrics_repository = MetricsRepository()

validation_service = ValidationService()
scan_repository = ScanRepository()

print("Validation Consumer Started...")
print("Listening on qr-scans topic...")


for message in validation_consumer:

    try:

        start_time = time.time()

        event = message.value

        print(f"Received Event: {event}")

        user_id = event["userId"]
        pem_id = event["pemId"]

        validation_result = (
            validation_service.validate_scan(
                user_id,
                pem_id
            )
        )

        end_time = time.time()

        processing_time_ms = round(
            (end_time - start_time) * 1000,
            2
        )

        result = {
            "eventId": event["eventId"],
            "userId": user_id,
            "pemId": pem_id,
            "validationStatus": validation_result["status"],
            "scanTimestamp": datetime.now(),
            "processingTimeMs": processing_time_ms,
            "createdAt": datetime.now(),
            "source": "QR_SCANNER"
        }

        scan_repository.insert_scan(result)

        metrics_repository.increment_processed(
            "validation-consumer"
        )


        metrics_repository.update_latency(
            "validation-consumer",
            processing_time_ms
        )

        if validation_result["hitOrMiss"]:
            metrics_repository.increment_cache_hit("validation-consumer")
        else:
            metrics_repository.increment_cache_miss("validation-consumer")

        kaka_topic_result = {
            "eventId": event["eventId"],
            "userId": user_id,
            "pemId": pem_id,
            "validationStatus": validation_result["status"],
            "scanTimestamp": datetime.now().isoformat(),
            "processingTimeMs": processing_time_ms,
            "createdAt": datetime.now().isoformat(),
            "source": "QR_SCANNER"
        }
    

        producer.send(
            VALIDATION_RESULTS_TOPIC,
            kaka_topic_result
        )
        producer.flush()
        validation_consumer.commit() 

    except DuplicateKeyError:
        print(f"Duplicate Event: {event['eventId']}")
        validation_consumer.commit()
       
    except Exception as e:
        metrics_repository.increment_failed("validation-consumer")
        print(
            f"Error processing event: {e}"
        )
        traceback.print_exc()


                    

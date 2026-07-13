import uuid

from datetime import datetime

from app.models.event_envelope import EventEnvelope


class EventBuilder:

    @staticmethod
    def qr_scan_event(
        event_id: str,
        user_id: str,
        pem_id: str,
        event_time: str,
        source : str
    ):

        return EventEnvelope(

            metadata={
                "eventId": event_id,
                "version": "3.0",
                "eventType": "QR_SCAN",
                "eventTime": event_time,
                "producer": "gateway-service",
                "retryCount": 0,
                "nextRetryAt": None,
                "arrivedAt": None,
                "processedAt": None,
                "traceId": str(uuid.uuid4()),
                "correlationId": str(uuid.uuid4())
            },

            payload= {
                "userId": user_id,
                "pemId": pem_id,
                "source": source
            }

        ).to_dict()
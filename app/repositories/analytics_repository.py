from datetime import datetime, timezone

from app.config.database import db


class AnalyticsRepository:

    def __init__(self):
        self.collection = db["analytics_hourly"]

    def update_hourly_metrics(
        self,
        event
    ):

        scan_time = event["scanTimestamp"]

        # Round down to the beginning of the hour
        hour = scan_time.replace(
            minute=0,
            second=0,
            microsecond=0
        )

        hourly_id = (
            f"{hour.strftime('%Y-%m-%dT%H')}"
            f"-{event['pemId']}"
        )

        update = {
            "$inc": {
                "totalScans": 1,
                "validScans": (
                    1
                    if event["validationStatus"] == "VALID"
                    else 0
                ),
                "invalidScans": (
                    1
                    if event["validationStatus"] != "VALID"
                    else 0
                ),
                "processingTimeSum": event["processingTimeMs"]
            },
            "$set": {
                "hour": hour,
                "pemId": event["pemId"],
                "lastUpdated": datetime.now()
            }
        }

        self.collection.update_one(
            {
                "_id": hourly_id
            },
            update,
            upsert=True
        )

    def get_hourly_analytics(self):

        return list(
            self.collection.find(
                {},
                {
                    "_id": 0
                }
            ).sort(
                "hour",
                -1
            )
        )

    def get_gateway_analytics(
        self,
        pem_id
    ):
        return list(
            self.collection.find(
                {
                    "pemId": pem_id
                },
                {
                    "_id": 0
                }
            ).sort(
                "hour",
                -1
            )
        )
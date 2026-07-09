from datetime import datetime, timezone

from app.config.database import db


class MetricsRepository:

    def __init__(self):
        self.collection = db["metrics"]
        self.events_processed = None

    def increment_processed(self, service_name):

        self.collection.update_one(
            {"serviceName": service_name},
            {
                "$inc": {
                    "eventsProcessed": 1
                },
                "$set": {
                    "lastUpdated": datetime.now()
                }
            }
        )

        self.events_processed = self.collection.find_one({"serviceName": service_name})

    def increment_failed(self, service_name):

        self.collection.update_one(
            {"serviceName": service_name},
            {
                "$inc": {
                    "eventsFailed": 1
                },
                "$set": {
                    "lastUpdated": datetime.now()
                }
            }
        )

    def increment_cache_hit(self, service_name):

        self.collection.update_one(
            {"serviceName": service_name},
            {
                "$inc": {
                    "cacheHits": 1
                },
                "$set": {
                    "lastUpdated": datetime.now()
                }
            }
        )

    def increment_cache_miss(self, service_name):

        self.collection.update_one(
            {"serviceName": service_name},
            {
                "$inc": {
                    "cacheMisses": 1
                },
                "$set": {
                    "lastUpdated": datetime.now()
                }
            }
        )

    def update_latency(
        self,
        service_name,
        latency_ms
    ):

        self.collection.update_one(
            {"serviceName": service_name},
            {
                "$inc": {
                    "totalProcessingLatencyMs": latency_ms
                },
                "$set": {
                    "lastUpdated": datetime.now()
                }
            }
        )
    
    def get_all_metrics(self):

        return list(
            self.collection.find(
                {},
                {
                    "_id": 0
                }
            )
        )


    def get_metrics_by_service(
        self,
        service_name
    ):

        return self.collection.find_one(
            {
                "_id": service_name
            },
            {
                "_id": 0
            }
        )
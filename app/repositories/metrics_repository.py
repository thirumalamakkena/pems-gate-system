from app.utils.current_time_stamp import (
    CurrentTimeStamp
)

from app.config.database import db

time_stamp = CurrentTimeStamp()
class MetricsRepository:

    def __init__(self):
        self.collection = db["metrics"]

    def update_metrics(  
            self,
            service_name,
            metrics
        ):

        self.collection.update_one(
            {"_id": service_name},
            {
                "$inc": metrics,
                "$set": {
                    "lastUpdated": time_stamp.current_time()
                }
            },
            upsert=True
        )

    
    def get_all_metrics(self):

        return list(
            self.collection.find(
                {}
            )
        )


    def get_metrics_by_service(
        self,
        service_name
    ):

        return self.collection.find_one(
            {
                "_id": service_name
            }
        )
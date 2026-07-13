from app.config.database import db

from app.utils.current_time_stamp import (
    CurrentTimeStamp
)

currrent_time_stamp = CurrentTimeStamp()

class DeadLetterRepository:

    def __init__(self):
        self.collection = db["dead_letter_events"]

    def insert(self, event):
        self.collection.insert_one(event)

    def get_by_event_id(self, event_id):
        return self.collection.find_one(
            {"metadata.eventId": event_id},
            {"_id": 0}
        )

    def get_all_unreplayed(self):
        return self.collection.find(
            {"metadata.replayed": {"$ne": True}},
            {"_id": 0}
        )

    def mark_replayed(self, event_id):
        self.collection.update_one(
            {"metadata.eventId": event_id},
            {
                "$set": {
                    "metadata.replayed": True,
                    "metadata.replayedAt": currrent_time_stamp.current_time(),
                    "metadata.replayedBy": "manual"
                }
            }
        )
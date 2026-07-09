from app.config.database import db


class ScanRepository:

    def __init__(self):
        self.collection = db["scan_history"]

    def insert_scan(self, scan_document):
        return self.collection.insert_one(
            scan_document
        )

    def get_user_history(
        self,
        user_id,
        limit=20
    ):
        return list(
            self.collection.find(
                {"userId": user_id}
            )
            .sort("scanTimestamp", -1)
            .limit(limit)
        )
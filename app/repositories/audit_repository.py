from app.config.database import db


class AuditRepository:

    def __init__(self):
        self.collection = db["audit_logs"]

    def insert_event(self, event):

        self.collection.insert_one(event)
from app.config.database import db


class UserRepository:

    def __init__(self):
        self.collection = db["users"]

    def find_by_user_id(self, user_id):
        return self.collection.find_one(
            {"userId": user_id}
        )

    def get_all_users(self):
        return list(
            self.collection.find({})
        )
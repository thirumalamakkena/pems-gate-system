from pymongo import MongoClient

from app.config.settings import (
    MONGODB_URI,
    DATABASE_NAME
)

client = MongoClient(MONGODB_URI)

db = client[DATABASE_NAME]
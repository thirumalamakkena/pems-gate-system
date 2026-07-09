from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError, CollectionInvalid

uri = "mongodb+srv://admin:auYyyyVgYCcBVwWc@cluster0.mckklym.mongodb.net/?appName=Cluster0"

from datetime import datetime, timezone
from pymongo import MongoClient

# MongoDB Atlas Connection
client = MongoClient(uri)

# Database
db = client["pems"]

# Metrics Collection

db["scan_history"].delete_many({})
print("Successfully Deleted.....")

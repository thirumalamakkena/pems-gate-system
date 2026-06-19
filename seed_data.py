from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError

MONGODB_URI = "mongodb+srv://admin:auYyyyVgYCcBVwWc@cluster0.mckklym.mongodb.net/?appName=Cluster0"

users_data = [
    {
        "userId": "USR001",
        "name": "Thirumala",
        "role": "SYSTEM_ENGINEER",
        "status": "ACTIVE",
        "allowedPems": ["PEMS-A"]
    },
    {
        "userId": "USR002",
        "name": "Ravi",
        "role": "ASSISTANT_SYSTEM_ENGINEER",
        "status": "ACTIVE",
        "allowedPems": ["PEMS-A", "PEMS-B"]
    },
    {
        "userId": "USR003",
        "name": "Shanmukh",
        "role": "MANAGER",
        "status": "ACTIVE",
        "allowedPems": ["PEMS-A", "PEMS-B", "PEMS-C"]
    },
    {
        "userId": "USR004",
        "name": "Priya",
        "role": "HR",
        "status": "ACTIVE",
        "allowedPems": ["PEMS-C"]
    },
    {
        "userId": "USR005",
        "name": "Arjun",
        "role": "DIRECTOR",
        "status": "ACTIVE",
        "allowedPems": ["PEMS-A", "PEMS-B", "PEMS-C"]
    },
    {
        "userId": "USR006",
        "name": "Sneha",
        "role": "SYSTEM_ENGINEER",
        "status": "ACTIVE",
        "allowedPems": ["PEMS-A"]
    },
    {
        "userId": "USR007",
        "name": "Vikram",
        "role": "SYSTEM_ENGINEER",
        "status": "INACTIVE",
        "allowedPems": ["PEMS-A"]
    },
    {
        "userId": "USR008",
        "name": "Anjali",
        "role": "PROJECT_MANAGER",
        "status": "ACTIVE",
        "allowedPems": ["PEMS-B", "PEMS-C"]
    },
    {
        "userId": "USR009",
        "name": "Rahul",
        "role": "ASSISTANT_SYSTEM_ENGINEER",
        "status": "ACTIVE",
        "allowedPems": ["PEMS-B"]
    },
    {
        "userId": "USR010",
        "name": "Meena",
        "role": "HR",
        "status": "INACTIVE",
        "allowedPems": ["PEMS-C"]
    },
    {
        "userId": "USR011",
        "name": "Suresh",
        "role": "MANAGER",
        "status": "ACTIVE",
        "allowedPems": ["PEMS-A", "PEMS-B"]
    },
    {
        "userId": "USR012",
        "name": "Deepika",
        "role": "SYSTEM_ENGINEER",
        "status": "ACTIVE",
        "allowedPems": ["PEMS-A"]
    },
    {
        "userId": "USR013",
        "name": "Nikhil",
        "role": "SECURITY_ADMIN",
        "status": "ACTIVE",
        "allowedPems": ["PEMS-A", "PEMS-B", "PEMS-C"]
    },
    {
        "userId": "USR014",
        "name": "Pooja",
        "role": "PROJECT_MANAGER",
        "status": "INACTIVE",
        "allowedPems": ["PEMS-B", "PEMS-C"]
    },
    {
        "userId": "USR015",
        "name": "Harish",
        "role": "DIRECTOR",
        "status": "ACTIVE",
        "allowedPems": ["PEMS-A", "PEMS-B", "PEMS-C"]
    }
]


try:
    print("Connecting to MongoDB...")

    client = MongoClient(MONGODB_URI)

    # Verify connection
    client.admin.command("ping")

    print("MongoDB connection successful")

    db = client["pems"]
    users_collection = db["users"]

    # Optional: clear existing users
    users_collection.delete_many({})
    print("Old users removed")

    result = users_collection.insert_many(users_data)

    print(f"Successfully inserted {len(result.inserted_ids)} users")

except ConnectionFailure as e:
    print("MongoDB connection failed")
    print(e)

except PyMongoError as e:
    print("MongoDB operation failed")
    print(e)

except Exception as e:
    print("Unexpected error")
    print(e)

finally:
    try:
        client.close()
        print("MongoDB connection closed")
    except:
        pass
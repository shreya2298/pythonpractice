from pymongo import MongoClient
import datetime
from data_processing import proc_data

# MongoDB connection details
MONGO_URI = "mongodb://localhost:27017/"  # Replace with your MongoDB connection URI
DATABASE_NAME = "mongodb"
COLLECTION_NAME = "table"

def conn(message):
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]

        # Insert the message into the collection
        result = collection.insert_one(message)
        return (f"Message inserted with ID: {result.inserted_id}")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the MongoDB connection
        client.close()


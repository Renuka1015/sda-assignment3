import json

from kafka import KafkaConsumer
from pymongo import MongoClient


# ==============================
# MongoDB Atlas Configuration
# ==============================

MONGODB_URI = "mongodb+srv://mongoadmin:mongoadmin046@cluster0.0skqrcd.mongodb.net/"

DATABASE_NAME = "SDA_Streaming"
COLLECTION_NAME = "ride_events"


# ==============================
# Connect to MongoDB Atlas
# ==============================

print("Connecting to MongoDB Atlas...")

mongo_client = MongoClient(MONGODB_URI)

# Test MongoDB connection
mongo_client.admin.command("ping")

print("MongoDB Atlas connection successful!")

database = mongo_client[DATABASE_NAME]
collection = database[COLLECTION_NAME]


# ==============================
# Connect to Kafka
# ==============================

print("Connecting to Kafka...")

consumer = KafkaConsumer(
    "ride_events",
    bootstrap_servers="localhost:9093",

    # Read existing messages if this consumer group
    # has never consumed this topic before
    auto_offset_reset="earliest",

    # Remember which messages have already been consumed
    enable_auto_commit=True,

    group_id="sda-streaming-consumer",

    # Convert Kafka JSON message into Python dictionary
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Kafka connection successful!")
print("Listening for ride events...\n")


# ==============================
# Consume and store events
# ==============================

try:

    for message in consumer:

        event = message.value

        # Insert event into MongoDB
        collection.insert_one(event)

        print(
            "Inserted:",
            event["ride_id"],
            "|",
            event["city"],
            "|",
            event["ride_type"],
            "| Fare:",
            event["fare"]
        )


except KeyboardInterrupt:

    print("\nConsumer stopped by user.")


finally:

    consumer.close()
    mongo_client.close()

    print("Connections closed.")
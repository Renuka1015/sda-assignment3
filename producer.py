import csv
import json
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


# --------------------------------------------------
# Kafka configuration
# --------------------------------------------------

KAFKA_SERVER = "localhost:9093"
TOPIC_NAME = "ride_events"


# --------------------------------------------------
# Create Kafka producer
# --------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# --------------------------------------------------
# Read sample data
# --------------------------------------------------

with open("sample_data.csv", "r", newline="", encoding="utf-8") as file:

    reader = csv.DictReader(file)

    print("Starting Uber ride event producer...")
    print("Sending events to Kafka topic:", TOPIC_NAME)
    print("Press Ctrl+C to stop.\n")

    try:

        while True:

            for row in reader:

                # Convert numeric fields from text to numbers
                row["distance_km"] = float(row["distance_km"])
                row["fare"] = float(row["fare"])
                row["surge_multiplier"] = float(row["surge_multiplier"])
                row["rating"] = float(row["rating"])

                # Add current streaming timestamp
                row["timestamp"] = datetime.now(timezone.utc).isoformat()

                # Send event to Kafka
                producer.send(TOPIC_NAME, value=row)

                print("Sent:", row)

                # Wait before sending the next event
                time.sleep(2)

            # CSV has ended, so start again
            file.seek(0)
            reader = csv.DictReader(file)

    except KeyboardInterrupt:

        print("\nProducer stopped by user.")

    finally:

        producer.flush()
        producer.close()
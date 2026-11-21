
import json
from confluent_kafka import Consumer
from database import get_database

# Connect to the SQLite database
db = get_database()

# Kafka Consumer Configuration
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "history-writer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

# Subscribe to the hospital readings topic
consumer.subscribe(["hospital-readings"])

try:
    while True:
        # Read a message from Kafka
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Decode the JSON message
        reading = json.loads(msg.value().decode("utf-8"))

        # Insert the reading into SQLite
        cursor = db.execute(
            """
            INSERT OR IGNORE INTO readings
            (patient, sign, timestamp, value, unit, ward, room)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                reading["patient"],
                reading["sign"],
                reading["timestamp"],
                reading["value"],
                reading["unit"],
                reading["ward"],
                reading["room"]
            )
        )

        # Save the reading in the database
        db.commit()

        # Commit the Kafka offset after saving
        consumer.commit(message=msg)

        if cursor.rowcount:
            print(
                f"Stored: {reading['patient']} - "
                f"{reading['sign']} - {reading['value']}"
            )
        else:
            print(
                f"Duplicate: {reading['patient']} - "
                f"{reading['sign']}"
            )

except KeyboardInterrupt:
    print("\nStopping history writer...")

finally:
    consumer.close()
    db.close()
    print("History writer stopped")

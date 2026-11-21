
import json
import os
from datetime import datetime
from confluent_kafka import Consumer
from database import get_database

# Average calculation window (in seconds)
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "60"))

# Connect to the SQLite database
db = get_database()

# Kafka Consumer Configuration
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "avg-calculator",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

# Subscribe to the hospital readings topic
consumer.subscribe(["hospital-readings"])

# Store accumulated readings for each patient, sign and window
accumulators = {}


def get_window_start(timestamp):
    # Convert ISO 8601 timestamp to Unix seconds
    epoch_seconds = int(
        datetime.fromisoformat(
            timestamp.replace("Z", "+00:00")
        ).timestamp()
    )

    # Calculate the beginning of the time window
    window_start = epoch_seconds - (epoch_seconds % WINDOW_SECONDS)

    return window_start


def accumulate_reading(reading):
    patient = reading["patient"]
    sign = reading["sign"]
    value = float(reading["value"])
    timestamp = reading["timestamp"]
    unit = reading["unit"]

    window_start = get_window_start(timestamp)

    # Identify the patient, sign and time window
    key = (patient, sign, window_start)

    # Initialize the accumulator if it does not exist
    if key not in accumulators:
        accumulators[key] = {
            "sum": 0.0,
            "count": 0,
            "unit": unit
        }

    # Update the accumulated values
    accumulators[key]["sum"] += value
    accumulators[key]["count"] += 1


def save_closed_windows(current_window_start):
    # Find windows that are older than the current one
    closed_windows = [
        key for key in accumulators
        if key[2] < current_window_start
    ]

    for key in closed_windows:
        patient, sign, window_start = key
        data = accumulators[key]

        # Calculate the average
        average = data["sum"] / data["count"]

        # Save the average in SQLite
        db.execute(
            """
            INSERT OR REPLACE INTO averages
            (patient, sign, window_start, average, unit)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                patient,
                sign,
                window_start,
                average,
                data["unit"]
            )
        )

        db.commit()

        print(
            f"Average saved: {patient} - {sign} - "
            f"{average:.2f} {data['unit']}"
        )

        # Remove the processed window from memory
        del accumulators[key]


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

        # Identify the time window of the reading
        current_window_start = get_window_start(
            reading["timestamp"]
        )

        # Accumulate the new reading
        accumulate_reading(reading)

        # Save averages from previous windows
        save_closed_windows(current_window_start)

        # Commit the Kafka offset
        consumer.commit(message=msg)

except KeyboardInterrupt:
    print("\nStopping average calculator...")

finally:
    consumer.close()
    db.close()
    print("Average calculator stopped")

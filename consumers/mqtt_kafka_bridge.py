
import json
import paho.mqtt.client as mqtt
from confluent_kafka import Producer

# MQTT Configuration
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "hospital/+/+/+"

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "hospital-readings"

# Create Kafka producer
kafka_producer = Producer({
    "bootstrap.servers": KAFKA_BROKER,
    "acks": "all",
    "enable.idempotence": True,
    "partitioner": "murmur2_random"
})


# Callback executed when Kafka confirms message delivery
def on_delivery(err, msg):
    if err is not None:
        print(f"Kafka delivery failed: {err}")
    else:
        print(
            f"Delivered to Kafka: "
            f"topic={msg.topic()}, "
            f"partition={msg.partition()}, "
            f"key={msg.key().decode('utf-8')}"
        )


# Callback executed whenever an MQTT message arrives
def on_message(client, userdata, msg):
    try:
        # Decode the received MQTT message
        message = json.loads(msg.payload.decode("utf-8"))

        # Get the patient identifier
        patient = message["patient"]

        # Send the original message to Kafka
        kafka_producer.produce(
            topic=KAFKA_TOPIC,
            key=patient,
            value=msg.payload,
            callback=on_delivery
        )

        kafka_producer.poll(0)

        print(
            f"Forwarded {message['sign']} reading "
            f"from {patient} to Kafka"
        )

    except (ValueError, KeyError, UnicodeDecodeError) as error:
        print(f"Invalid MQTT message: {error}")

    except BufferError as error:
        print(f"Kafka producer queue is full: {error}")


# Callback executed when MQTT connection is established
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("Connected to MQTT broker")
        client.subscribe(MQTT_TOPIC, qos=1)
        print(f"Subscribed to {MQTT_TOPIC}")
    else:
        print(f"MQTT connection failed: {reason_code}")


# Create MQTT client
client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="cloud-kafka-bridge",
    clean_session=False
)

# Register callback functions
client.on_connect = on_connect
client.on_message = on_message

try:
    # Connect to Mosquitto
    client.connect(MQTT_BROKER, MQTT_PORT, 60)

    print("Starting MQTT to Kafka bridge...")
    client.loop_forever()

except KeyboardInterrupt:
    print("\nStopping MQTT to Kafka bridge...")

finally:
    client.disconnect()
    client.loop_stop()

    # Wait for pending Kafka messages
    remaining = kafka_producer.flush(10)

    if remaining:
        print(f"Warning: {remaining} Kafka messages were not delivered")

    print("Bridge stopped")

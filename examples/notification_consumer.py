"""Notification Service — consumes payments and order updates, sends notifications."""

import json
import os
import time
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def main():
    print(f"Notification Service connecting to {BOOTSTRAP}...")
    consumer = KafkaConsumer(
        "payments.processed", "orders.updated",
        bootstrap_servers=BOOTSTRAP,
        group_id="notification-service",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        client_id="notification-service-01",
    )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        client_id="notification-service-prod-01",
    )

    print("Notification Service started — consuming payments.processed, orders.updated")

    for message in consumer:
        data = message.value
        notification = {
            "type": "email" if message.topic == "payments.processed" else "push",
            "source_topic": message.topic,
            "ref_id": data.get("payment_id") or data.get("order_id"),
            "message": f"Event from {message.topic}",
            "timestamp": time.time(),
        }
        producer.send("notifications.sent", value=notification)


if __name__ == "__main__":
    main()

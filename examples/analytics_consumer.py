"""Analytics Service — consumes from multiple topics for aggregation."""

import json
import os
from kafka import KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def main():
    print(f"Analytics Service connecting to {BOOTSTRAP}...")
    consumer = KafkaConsumer(
        "orders.created", "payments.processed", "notifications.sent",
        bootstrap_servers=BOOTSTRAP,
        group_id="analytics-service",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        client_id="analytics-service-01",
    )

    print("Analytics Service started — consuming orders, payments, notifications")

    counts = {}
    for message in consumer:
        topic = message.topic
        counts[topic] = counts.get(topic, 0) + 1
        if counts[topic] % 10 == 0:
            print(f"[Analytics] {topic}: {counts[topic]} events processed")


if __name__ == "__main__":
    main()

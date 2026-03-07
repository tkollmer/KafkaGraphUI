"""Inventory Service — produces inventory events."""

import json
import os
import random
import time
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
WAREHOUSES = ["warehouse-east", "warehouse-west", "warehouse-central"]
PRODUCTS = ["laptop", "phone", "tablet", "headphones", "keyboard", "monitor"]


def main():
    print(f"Inventory Service connecting to {BOOTSTRAP}...")
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        client_id="inventory-service-prod-01",
    )
    print("Inventory Service started — producing to inventory.updated")

    while True:
        event = {
            "product": random.choice(PRODUCTS),
            "warehouse": random.choice(WAREHOUSES),
            "quantity_change": random.randint(-10, 50),
            "stock_level": random.randint(0, 500),
            "timestamp": time.time(),
        }
        producer.send("inventory.updated", value=event)

        # Low stock alerts
        if event["stock_level"] < 20:
            alert = {
                "product": event["product"],
                "warehouse": event["warehouse"],
                "stock_level": event["stock_level"],
                "severity": "critical" if event["stock_level"] < 5 else "warning",
                "timestamp": time.time(),
            }
            producer.send("inventory.alerts", value=alert)

        time.sleep(random.uniform(1.0, 4.0))


if __name__ == "__main__":
    main()

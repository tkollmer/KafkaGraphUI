"""Order Service — produces order events to 'orders.created' topic."""

import json
import os
import random
import time
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
PRODUCTS = ["laptop", "phone", "tablet", "headphones", "keyboard", "monitor", "mouse", "webcam"]
CUSTOMERS = ["alice", "bob", "charlie", "diana", "eve", "frank"]

def main():
    print(f"Order Service connecting to {BOOTSTRAP}...")
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        client_id="order-service-prod-01",
    )
    print("Order Service started — producing to orders.created")

    order_id = 1000
    while True:
        order = {
            "order_id": f"ORD-{order_id}",
            "customer": random.choice(CUSTOMERS),
            "product": random.choice(PRODUCTS),
            "quantity": random.randint(1, 5),
            "price": round(random.uniform(9.99, 999.99), 2),
            "timestamp": time.time(),
        }
        producer.send("orders.created", value=order, key=order["order_id"].encode())
        order_id += 1

        # Also produce to orders.updated occasionally
        if random.random() < 0.3:
            update = {
                "order_id": f"ORD-{random.randint(1000, order_id)}",
                "status": random.choice(["confirmed", "shipped", "delivered"]),
                "timestamp": time.time(),
            }
            producer.send("orders.updated", value=update)

        time.sleep(random.uniform(0.5, 2.0))


if __name__ == "__main__":
    main()

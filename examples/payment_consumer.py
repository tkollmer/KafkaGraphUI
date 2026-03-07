"""Payment Service — consumes orders, produces payment events."""

import json
import os
import random
import time
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def main():
    print(f"Payment Service connecting to {BOOTSTRAP}...")
    consumer = KafkaConsumer(
        "orders.created",
        bootstrap_servers=BOOTSTRAP,
        group_id="payment-service",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        client_id="payment-service-01",
    )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        client_id="payment-service-prod-01",
    )

    print("Payment Service started — consuming orders.created, producing payments.processed")

    for message in consumer:
        order = message.value
        # Simulate payment processing
        time.sleep(random.uniform(0.1, 0.5))

        payment = {
            "payment_id": f"PAY-{random.randint(10000, 99999)}",
            "order_id": order.get("order_id"),
            "amount": order.get("price", 0) * order.get("quantity", 1),
            "status": random.choice(["approved", "approved", "approved", "declined"]),
            "method": random.choice(["credit_card", "debit_card", "paypal", "bank_transfer"]),
            "timestamp": time.time(),
        }
        producer.send("payments.processed", value=payment)

        # Produce to payments.failed occasionally
        if payment["status"] == "declined":
            producer.send("payments.failed", value=payment)


if __name__ == "__main__":
    main()

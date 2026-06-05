from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "orders.cdc")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda value: str(value).encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    events = [
        {"order_id": 1, "customer_id": 101, "product_id": "P1", "status": "NEW", "amount": 100.00},
        {"order_id": 2, "customer_id": 102, "product_id": "P2", "status": "NEW", "amount": 50.00},
        {"order_id": 1, "customer_id": 101, "product_id": "P1", "status": "UPDATED", "amount": 120.00},
        {"order_id": 4, "customer_id": 101, "product_id": "P2", "status": "NEW", "amount": 80.00},
    ]

    for event in events:
        event["event_time"] = utc_now()
        producer.send(TOPIC, key=event["order_id"], value=event)
        print(f"sent {TOPIC}: {event}")
        time.sleep(0.5)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Day 2 Kafka producer: publish order events to an event bus topic.

This example uses kafka-python because it is small enough for a beginner lab:

    python -m pip install kafka-python
    python producer.py

What this script teaches:
  - KafkaProducer is the application-side client.
  - value_serializer turns a Python dict into bytes.
  - key_serializer turns the business key into bytes.
  - producer.send(...) publishes an event record to a topic.
  - producer.flush() waits for queued records before the script exits.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaProducer
from kafka.producer.future import FutureRecordMetadata


TOPIC_NAME = "order-events"


def json_serializer(data: Any) -> bytes:
    """Convert Python objects to JSON bytes for Kafka."""
    return json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")


def key_serializer(key: str) -> bytes:
    """Convert the business key to bytes."""
    return key.encode("utf-8")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def build_order_event(order_id: str, status: str, amount: float) -> dict[str, Any]:
    """Create one event-bus message payload."""
    return {
        "event_type": "OrderStatusChanged",
        "event_time": utc_now(),
        "order_id": order_id,
        "status": status,
        "amount": amount,
        "currency": "INR",
        "source_system": "checkout-service",
    }


def report_delivery(future: FutureRecordMetadata, order_id: str) -> None:
    """Block for lab visibility and print where Kafka stored the event."""
    metadata = future.get(timeout=10)
    print(
        "published",
        f"order_id={order_id}",
        f"topic={metadata.topic}",
        f"partition={metadata.partition}",
        f"offset={metadata.offset}",
    )


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        client_id="checkout-service-producer",
        key_serializer=key_serializer,
        value_serializer=json_serializer,
        acks="all",
        linger_ms=10,
        batch_size=32_768,
        compression_type="gzip",
        retries=5,
        retry_backoff_ms=200,
        request_timeout_ms=30_000,
    )

    events = [
        build_order_event("ORD-1001", "PAID", 1299.00),
        build_order_event("ORD-1002", "PAID", 799.00),
        build_order_event("ORD-1001", "PACKED", 1299.00),
        build_order_event("ORD-1003", "PAYMENT_FAILED", 1499.00),
        build_order_event("ORD-1001", "SHIPPED", 1299.00),
    ]

    try:
        for event in events:
            order_id = event["order_id"]
            future = producer.send(
                TOPIC_NAME,
                key=order_id,
                value=event,
                headers=[("event_type", event["event_type"].encode("utf-8"))],
            )
            report_delivery(future, order_id)
            time.sleep(0.2)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()

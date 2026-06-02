#!/usr/bin/env python3
"""
Day 2 Kafka consumer: read order events from the event bus topic.

Run this in one terminal before running producer.py in another terminal:

    python -m pip install kafka-python
    python consumer.py

What this script teaches:
  - KafkaConsumer subscribes to a topic.
  - value_deserializer turns Kafka bytes back into a Python dict.
  - key_deserializer turns the Kafka key back into a string.
  - message.value is business data.
  - message.topic, partition and offset are Kafka delivery metadata.
"""

from __future__ import annotations

import json
import time
from json import JSONDecodeError
from typing import Any

from kafka import KafkaConsumer


TOPIC_NAME = "order-events"


def json_deserializer(data: bytes | None) -> dict[str, Any] | None:
    """Convert JSON bytes from Kafka into a Python dict."""
    if not data:
        return None
    try:
        return json.loads(data.decode("utf-8"))
    except JSONDecodeError:
        return None


def key_deserializer(data: bytes | None) -> str | None:
    """Convert Kafka key bytes into a Python string."""
    if data is None:
        return None
    return data.decode("utf-8")


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=["localhost:9092"],
        client_id="order-notification-client",
        group_id="order-notification-service",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=key_deserializer,
        value_deserializer=json_deserializer,
    )

    print(f"Listening for events on topic '{TOPIC_NAME}'. Press Ctrl+C to stop.\n")
    try:
        stop_after_seconds = 15
        deadline = time.monotonic() + stop_after_seconds

        while time.monotonic() < deadline:
            records = consumer.poll(timeout_ms=1_000)
            if not records:
                continue

            deadline = time.monotonic() + stop_after_seconds
            for messages in records.values():
                for message in messages:
                    event = message.value
                    if event is None:
                        print(
                            "skipped",
                            f"key={message.key}",
                            f"topic={message.topic}",
                            f"partition={message.partition}",
                            f"offset={message.offset}",
                            "reason=empty-or-invalid-json",
                        )
                        continue

                    print(
                        "received",
                        f"event_type={event['event_type']}",
                        f"order_id={event['order_id']}",
                        f"status={event['status']}",
                        f"key={message.key}",
                        f"topic={message.topic}",
                        f"partition={message.partition}",
                        f"offset={message.offset}",
                    )
        print("\nNo more records arrived within 15 seconds; consumer stopped.")
    except KeyboardInterrupt:
        print("\nStopping consumer.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Consume Day 5 Avro logistics records through Schema Registry."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from uuid import uuid4

from confluent_kafka import TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.deserializing_consumer import DeserializingConsumer


ROOT = Path(__file__).resolve().parent
TOPIC = "northstar.logistics.avro"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--schema-registry-url", default="http://localhost:8081")
    parser.add_argument("--group-id", default=None)
    parser.add_argument("--run-id", default=None, help="Only print/write records from this producer run id")
    parser.add_argument("--max-records", type=int, default=20)
    parser.add_argument("--output", default=str(ROOT / "outputs" / "bronze" / "logistics_avro.json"))
    args = parser.parse_args()

    schema_registry = SchemaRegistryClient({"url": args.schema_registry_url})
    avro_deserializer = AvroDeserializer(schema_registry)
    group_id = args.group_id or f"day5-avro-logistics-consumer-{uuid4()}"

    consumer = DeserializingConsumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": avro_deserializer,
        }
    )

    metadata = consumer.list_topics(TOPIC, timeout=10)
    if TOPIC not in metadata.topics or metadata.topics[TOPIC].error is not None:
        raise RuntimeError(f"Topic {TOPIC} is not available")

    partitions = [TopicPartition(TOPIC, partition_id) for partition_id in metadata.topics[TOPIC].partitions]
    consumer.assign(partitions)
    low_high = {partition: consumer.get_watermark_offsets(partition, timeout=10) for partition in partitions}
    for partition, (low, _high) in low_high.items():
        partition.offset = low
    consumer.assign(partitions)

    consumed = []
    idle_polls = 0
    try:
        while len(consumed) < args.max_records:
            msg = consumer.poll(1.0)
            if msg is None:
                idle_polls += 1
                if idle_polls >= 5:
                    break
                continue
            idle_polls = 0
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            value = msg.value()
            if args.run_id and value.get("_lab_run_id") != args.run_id:
                continue
            record = {
                "_kafka_topic": msg.topic(),
                "_kafka_partition": msg.partition(),
                "_kafka_offset": msg.offset(),
                "_kafka_key": msg.key(),
                **value,
            }
            consumed.append(record)
            print(f"Consumed Avro logistics record: {record}")
    finally:
        consumer.close()

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(consumed, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"group_id": group_id, "topic": TOPIC, "consumed": len(consumed), "output": str(output)}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

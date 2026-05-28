"""
Practical Schema Registry producer for Day 5.

This producer reads the same logistics.csv file as the JSON CSV producer,
validates rows, and publishes valid shipment records as Avro to Kafka using
Confluent Schema Registry.
"""

from __future__ import annotations

import csv
from datetime import datetime

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer


TOPIC = "northstar.logistics.avro"

SCHEMA = """
{
  "type": "record",
  "name": "LogisticsUpdate",
  "namespace": "northstar.logistics",
  "fields": [
    {"name": "shipment_id", "type": "string"},
    {"name": "shipment_status", "type": "string"},
    {"name": "updated_at", "type": "string"}
  ]
}
"""


def validate_row(row: dict[str, str]) -> dict[str, str] | None:
    shipment_id = row.get("shipment_id")
    status = row.get("shipment_status")
    if not shipment_id or not status:
        return None
    return {
        "shipment_id": shipment_id,
        "shipment_status": status,
        "updated_at": row.get("updated_at") or datetime.utcnow().isoformat() + "Z",
    }


def main() -> None:
    schema_registry = SchemaRegistryClient({"url": "http://localhost:8081"})
    avro_serializer = AvroSerializer(schema_registry, SCHEMA)

    producer = SerializingProducer(
        {
            "bootstrap.servers": "localhost:9092",
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
        }
    )

    with open("logistics.csv", "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            record = validate_row(row)
            if record is None:
                print(f"Skipped invalid row for Avro topic: {row}")
                continue
            producer.produce(TOPIC, key=record["shipment_id"], value=record)
            print(f"Produced Avro record: {record}")

    producer.flush()
    print(f"Finished producing Avro records to {TOPIC}.")


if __name__ == "__main__":
    main()

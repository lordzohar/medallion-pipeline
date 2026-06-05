#!/usr/bin/env python3
"""Produce Day 5 logistics records as Avro through Schema Registry."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from confluent_kafka import Producer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer


ROOT = Path(__file__).resolve().parent
AVRO_TOPIC = "northstar.logistics.avro"
DLQ_TOPIC = "northstar.ingestion.dlq"

SCHEMA = """
{
  "type": "record",
  "name": "LogisticsUpdate",
  "namespace": "northstar.logistics",
  "fields": [
    {"name": "shipment_id", "type": "string"},
    {"name": "order_id", "type": "string"},
    {"name": "carrier", "type": "string"},
    {"name": "status", "type": "string"},
    {"name": "event_ts", "type": "string"},
    {"name": "_lab_run_id", "type": ["null", "string"], "default": null}
  ]
}
"""


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_row(row: dict[str, str], run_id: str) -> dict[str, str | None]:
    return {
        "shipment_id": (row.get("shipment_id") or "").strip(),
        "order_id": (row.get("order_id") or "").strip(),
        "carrier": (row.get("carrier") or "").strip(),
        "status": (row.get("status") or row.get("shipment_status") or "").strip(),
        "event_ts": (row.get("event_ts") or row.get("updated_at") or now_utc()).strip(),
        "_lab_run_id": run_id,
    }


def validate_record(record: dict[str, str | None]) -> list[str]:
    errors = []
    for field in ["shipment_id", "order_id", "carrier", "status", "event_ts"]:
        if not record.get(field):
            errors.append(f"missing {field}")
    if record.get("status") and record["status"] not in {"CREATED", "IN_TRANSIT", "DELIVERED"}:
        errors.append("invalid shipment status")
    return errors


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed for {msg.topic()} [{msg.partition()}]: {err}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(ROOT / "input" / "logistics_batch.csv"))
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--schema-registry-url", default="http://localhost:8081")
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    run_id = args.run_id or f"avro-logistics-{uuid4()}"
    schema_registry = SchemaRegistryClient({"url": args.schema_registry_url})
    avro_serializer = AvroSerializer(schema_registry, SCHEMA)

    avro_producer = SerializingProducer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
        }
    )
    dlq_producer = Producer({"bootstrap.servers": args.bootstrap_servers})

    produced = 0
    dlq = 0
    with Path(args.input).open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            record = normalize_row(row, run_id)
            errors = validate_record(record)
            if errors:
                dlq_record = {"source": "avro_logistics_producer", "errors": errors, "record": row, "run_id": run_id}
                key = f"avro_logistics_producer:{','.join(errors)}"
                dlq_producer.produce(
                    DLQ_TOPIC,
                    key=key.encode("utf-8"),
                    value=json.dumps(dlq_record, sort_keys=True).encode("utf-8"),
                    callback=delivery_report,
                )
                dlq += 1
                print(f"Sent invalid logistics row to {DLQ_TOPIC}: {dlq_record}")
                continue

            avro_producer.produce(AVRO_TOPIC, key=str(record["shipment_id"]), value=record, on_delivery=delivery_report)
            produced += 1
            print(f"Produced Avro logistics record to {AVRO_TOPIC}: {record}")

    avro_producer.flush()
    dlq_producer.flush()
    print(json.dumps({"run_id": run_id, "avro_topic": AVRO_TOPIC, "produced_avro": produced, "dlq_topic": DLQ_TOPIC, "produced_dlq": dlq}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

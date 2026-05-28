"""
CSV Producer
------------
This script reads a CSV file containing logistics updates and publishes
each row as a JSON message to the `northstar.logistics.csv` topic.
It performs basic validation to illustrate bad data handling.  Rows
missing a shipment_id or status will be considered invalid and sent
to the DLQ topic (`northstar.ingestion.dlq`) instead of the main topic.

Prerequisites:
  pip install kafka-python pandas
  Provide a `logistics.csv` file in the same directory with columns:
  shipment_id,shipment_status,updated_at
"""

import csv
import json
from datetime import datetime
from kafka import KafkaProducer


DLQ_TOPIC = "northstar.ingestion.dlq"
MAIN_TOPIC = "northstar.logistics.csv"


def validate_row(row):
    """Return a tuple (is_valid, message_dict)."""
    shipment_id = row.get("shipment_id")
    status = row.get("shipment_status")
    updated_at = row.get("updated_at") or datetime.utcnow().isoformat() + "Z"
    if not shipment_id or not status:
        return False, {
            "error": "Missing required fields",
            "row": row,
            "timestamp": updated_at
        }
    return True, {
        "shipment_id": shipment_id,
        "shipment_status": status,
        "updated_at": updated_at
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    filename = "logistics.csv"
    with open(filename, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            valid, message = validate_row(row)
            if valid:
                key = message["shipment_id"]
                producer.send(MAIN_TOPIC, key=key, value=message)
                print(f"Sent valid record: {message}")
            else:
                # For invalid rows, publish to DLQ
                producer.send(DLQ_TOPIC, value=message)
                print(f"Sent to DLQ: {message}")
        producer.flush()
        print("Finished sending CSV records.")
    producer.close()


if __name__ == "__main__":
    main()

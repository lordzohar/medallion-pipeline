#!/usr/bin/env python3
"""Register Day 5 event contracts in Confluent Schema Registry.

The main CDC producers use JSON payloads, but Schema Registry still matters as
the catalog of expected contracts. This script registers JSON Schema subjects so
learners can see concrete subjects in the UI and inspect compatibility.
"""

from __future__ import annotations

import argparse
import json
import urllib.request


SCHEMAS = {
    "northstar.orders.public.orders-value": {
        "type": "object",
        "additionalProperties": True,
        "required": ["op"],
        "properties": {
            "before": {"type": ["object", "null"]},
            "after": {"type": ["object", "null"]},
            "op": {"type": "string", "enum": ["r", "c", "u", "d"]},
            "source": {"type": "object"},
            "ts_ms": {"type": ["integer", "null"]},
        },
    },
    "northstar.inventory.inventorydb.inventory-value": {
        "type": "object",
        "additionalProperties": True,
        "required": ["op"],
        "properties": {
            "before": {"type": ["object", "null"]},
            "after": {"type": ["object", "null"]},
            "op": {"type": "string", "enum": ["r", "c", "u", "d"]},
            "source": {"type": "object"},
            "ts_ms": {"type": ["integer", "null"]},
        },
    },
    "northstar.clickstream.events-value": {
        "type": "object",
        "additionalProperties": False,
        "required": ["event_id", "event_ts", "event_type", "session_id", "user_id"],
        "properties": {
            "event_id": {"type": "string"},
            "event_ts": {"type": "string", "format": "date-time"},
            "event_type": {"type": "string"},
            "order_id": {"type": ["integer", "null"]},
            "session_id": {"type": "string"},
            "user_id": {"type": "string"},
            "_lab_run_id": {"type": "string"},
        },
    },
    "northstar.logistics.csv-value": {
        "type": "object",
        "additionalProperties": False,
        "required": ["shipment_id", "order_id", "carrier", "status", "event_ts"],
        "properties": {
            "shipment_id": {"type": "string"},
            "order_id": {"type": "string"},
            "carrier": {"type": "string"},
            "status": {"type": "string", "enum": ["CREATED", "IN_TRANSIT", "DELIVERED"]},
            "event_ts": {"type": "string", "format": "date-time"},
            "_lab_run_id": {"type": "string"},
        },
    },
    "northstar.ingestion.dlq-value": {
        "type": "object",
        "additionalProperties": True,
        "required": ["source", "errors", "record"],
        "properties": {
            "source": {"type": "string"},
            "errors": {"type": "array", "items": {"type": "string"}},
            "record": {"type": "object"},
        },
    },
}


def post_json(url: str, payload: dict) -> dict:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def get_json(url: str) -> list | dict:
    request = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema-registry-url", default="http://localhost:8081")
    args = parser.parse_args()

    base_url = args.schema_registry_url.rstrip("/")
    registered = {}
    for subject, schema in SCHEMAS.items():
        result = post_json(
            f"{base_url}/subjects/{subject}/versions",
            {"schemaType": "JSON", "schema": json.dumps(schema, sort_keys=True)},
        )
        registered[subject] = result

    print(json.dumps({"registered": registered, "subjects": get_json(f"{base_url}/subjects")}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()


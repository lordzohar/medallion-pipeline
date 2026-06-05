#!/usr/bin/env python3
"""Check Day 5 schema proposals with Schema Registry compatibility APIs."""

from __future__ import annotations

import argparse
import json
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parent
INPUT = ROOT / "input"
OUTPUT = ROOT / "outputs" / "silver"
SUBJECT = "northstar.orders.contract-value"

BASELINE_SCHEMA = {
    "type": "record",
    "name": "OrderEvent",
    "namespace": "northstar.orders",
    "fields": [
        {"name": "order_id", "type": "long"},
        {"name": "customer_id", "type": "long"},
        {"name": "status", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "order_ts", "type": "string"},
        {"name": "coupon_code", "type": ["null", "string"], "default": None},
    ],
}

PROPOSED_SCHEMAS = {
    "Add optional field customer_tier with default UNKNOWN": {
        **BASELINE_SCHEMA,
        "fields": [
            *BASELINE_SCHEMA["fields"],
            {"name": "customer_tier", "type": "string", "default": "UNKNOWN"},
        ],
    },
    "Rename order_id to id": {
        **BASELINE_SCHEMA,
        "fields": [
            {"name": "id", "type": "long"},
            *[field for field in BASELINE_SCHEMA["fields"] if field["name"] != "order_id"],
        ],
    },
    "Remove optional field coupon_code": {
        **BASELINE_SCHEMA,
        "fields": [field for field in BASELINE_SCHEMA["fields"] if field["name"] != "coupon_code"],
    },
}


def request_json(method: str, url: str, payload: dict | None = None) -> dict | list:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        body = response.read().decode("utf-8")
        return json.loads(body) if body else {}


def schema_payload(schema: dict) -> dict:
    return {"schemaType": "AVRO", "schema": json.dumps(schema, sort_keys=True)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema-registry-url", default="http://localhost:8081")
    parser.add_argument("--subject", default=SUBJECT)
    args = parser.parse_args()

    base_url = args.schema_registry_url.rstrip("/")
    subject = args.subject

    request_json("PUT", f"{base_url}/config/{subject}", {"compatibility": "FULL_TRANSITIVE"})
    baseline_result = request_json("POST", f"{base_url}/subjects/{subject}/versions", schema_payload(BASELINE_SCHEMA))

    proposals = json.loads((INPUT / "schema_change_proposals.json").read_text(encoding="utf-8"))
    results = []
    for proposal in proposals:
        proposed_schema = PROPOSED_SCHEMAS[proposal["change"]]
        compatibility = request_json(
            "POST",
            f"{base_url}/compatibility/subjects/{subject}/versions/latest",
            schema_payload(proposed_schema),
        )
        actual_decision = "ACCEPT" if compatibility.get("is_compatible") else "REJECT"
        results.append(
            {
                **proposal,
                "subject": subject,
                "registry_compatibility": compatibility,
                "actual_decision": actual_decision,
                "checked_by": "Schema Registry FULL_TRANSITIVE compatibility",
            }
        )

    OUTPUT.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT / "schema_registry_compatibility_checks.json"
    output_path.write_text(json.dumps(results, indent=2, sort_keys=True), encoding="utf-8")
    print(
        json.dumps(
            {
                "baseline_subject": subject,
                "baseline_registration": baseline_result,
                "output": str(output_path),
                "decisions": [item["actual_decision"] for item in results],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

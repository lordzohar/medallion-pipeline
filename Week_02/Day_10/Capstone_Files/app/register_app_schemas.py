#!/usr/bin/env python
"""Register Avro schemas for the 4 stream subjects with Schema Registry.

Idempotent — the registry returns the existing schema id if content matches.
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import requests

SR_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
HERE = Path(__file__).resolve().parent
SCHEMAS_DIR = HERE / "schemas"

SUBJECTS = {
    "ogn.aircraft.positions-value": "ogn_position.avsc",
    "noaa.observations-value":      "noaa_observation.avsc",
    "noaa.alerts-value":            "noaa_alert.avsc",
    "seismic.events-value":         "seismic_event.avsc",
}


def wait_for_sr(timeout: int = 60) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{SR_URL}/subjects", timeout=2)
            if r.ok:
                print(f"[ok] Schema Registry reachable at {SR_URL}"); return
        except requests.RequestException:
            pass
        print(f"[wait] {SR_URL} not ready yet...")
        time.sleep(2)
    sys.exit(f"[err] Schema Registry not reachable: {SR_URL}")


def register(subject: str, schema_path: Path) -> int:
    body = {"schema": schema_path.read_text(), "schemaType": "AVRO"}
    r = requests.post(
        f"{SR_URL}/subjects/{subject}/versions",
        data=json.dumps(body),
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=10,
    )
    if not r.ok:
        sys.exit(f"[err] register {subject}: {r.status_code} {r.text}")
    sid = r.json()["id"]
    print(f"[ok] {subject:40s} -> schema id={sid} (file={schema_path.name})")
    return sid


def main() -> None:
    wait_for_sr()
    for subject, fname in SUBJECTS.items():
        register(subject, SCHEMAS_DIR / fname)


if __name__ == "__main__":
    main()

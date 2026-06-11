#!/usr/bin/env python
"""Register all Kafka Connect connectors for the Day 10 capstone.

Idempotent: PUTs each connector config to /connectors/<name>/config so re-runs
update in place instead of erroring on 409 Conflict. Use --restart to also
issue a restart on every task.

Usage (host):
    python debezium/register_all_connectors.py
    python debezium/register_all_connectors.py --restart
    python debezium/register_all_connectors.py --delete    # tear down
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import requests

CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL", "http://localhost:8083")
HERE = Path(__file__).resolve().parent
CONNECTORS_DIR = HERE / "connectors"


def wait_for_connect(timeout: int = 120) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{CONNECT_URL}/", timeout=2)
            if r.ok:
                version = r.json().get("version", "?")
                print(f"[ok] Kafka Connect reachable (version {version})")
                return
        except requests.RequestException:
            pass
        print(f"[wait] {CONNECT_URL} not ready yet...")
        time.sleep(3)
    sys.exit(f"[err] Kafka Connect not reachable at {CONNECT_URL} after {timeout}s")


def list_connectors() -> list[str]:
    r = requests.get(f"{CONNECT_URL}/connectors", timeout=5)
    r.raise_for_status()
    return r.json()


def put_connector(name: str, config: dict) -> None:
    url = f"{CONNECT_URL}/connectors/{name}/config"
    r = requests.put(url, json=config, headers={"Content-Type": "application/json"}, timeout=30)
    if r.status_code not in (200, 201):
        print(f"[err] PUT {name} -> {r.status_code} {r.text}")
        r.raise_for_status()
    print(f"[ok] upserted connector '{name}'")


def status(name: str) -> dict:
    r = requests.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=10)
    r.raise_for_status()
    return r.json()


def restart_connector(name: str) -> None:
    requests.post(f"{CONNECT_URL}/connectors/{name}/restart?includeTasks=true&onlyFailed=false", timeout=10)
    print(f"[ok] restart issued for '{name}'")


def delete_connector(name: str) -> None:
    r = requests.delete(f"{CONNECT_URL}/connectors/{name}", timeout=10)
    if r.status_code == 404:
        print(f"[skip] '{name}' did not exist")
    else:
        r.raise_for_status()
        print(f"[ok] deleted '{name}'")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--restart", action="store_true")
    ap.add_argument("--delete", action="store_true")
    args = ap.parse_args()

    wait_for_connect()

    files = sorted(CONNECTORS_DIR.glob("*.json"))
    if not files:
        sys.exit(f"[err] no connector JSON files in {CONNECTORS_DIR}")

    if args.delete:
        for f in files:
            cfg = json.loads(f.read_text())
            delete_connector(cfg["name"])
        return

    for f in files:
        cfg = json.loads(f.read_text())
        name = cfg["name"]
        put_connector(name, cfg["config"])
        if args.restart:
            time.sleep(1)
            restart_connector(name)

    # Print status summary.
    print("\n=== Connector status ===")
    for name in list_connectors():
        s = status(name)
        connector_state = s.get("connector", {}).get("state", "?")
        task_states = [t.get("state", "?") for t in s.get("tasks", [])]
        print(f"  {name:35s}  connector={connector_state:10s}  tasks={task_states}")


if __name__ == "__main__":
    main()

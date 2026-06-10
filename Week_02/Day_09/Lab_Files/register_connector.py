"""
register_connector.py
---------------------
POSTs the Debezium PostgreSQL connector config to Kafka Connect.
Idempotent: if a connector with the same name exists it just reports it.

Run once after `docker compose up` and after `db_seeder.py`:
    python register_connector.py
"""
import json
import sys
import time

import requests

CONNECT_URL = "http://localhost:8083"
CONFIG_FILE = "debezium-postgres.json"


def wait_for_connect():
    for _ in range(40):
        try:
            r = requests.get(f"{CONNECT_URL}/connectors", timeout=2)
            if r.ok:
                return
        except requests.RequestException:
            pass
        print("  waiting for Kafka Connect...")
        time.sleep(3)
    sys.exit("Kafka Connect not reachable")


def main():
    wait_for_connect()
    with open(CONFIG_FILE) as f:
        cfg = json.load(f)
    name = cfg["name"]

    r = requests.get(f"{CONNECT_URL}/connectors/{name}", timeout=5)
    if r.status_code == 200:
        print(f"connector '{name}' already registered")
        return

    r = requests.post(
        f"{CONNECT_URL}/connectors",
        headers={"Content-Type": "application/json"},
        data=json.dumps(cfg),
        timeout=10,
    )
    if not r.ok:
        sys.exit(f"register failed: {r.status_code} {r.text}")
    print(f"connector '{name}' registered")

    # poll status briefly
    for _ in range(10):
        time.sleep(2)
        s = requests.get(f"{CONNECT_URL}/connectors/{name}/status").json()
        state = s.get("connector", {}).get("state", "UNKNOWN")
        tasks = [t.get("state") for t in s.get("tasks", [])]
        print(f"  connector={state}  tasks={tasks}")
        if state == "RUNNING" and all(t == "RUNNING" for t in tasks):
            return


if __name__ == "__main__":
    main()

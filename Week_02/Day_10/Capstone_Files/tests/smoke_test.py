#!/usr/bin/env python
"""End-to-end smoke test for the Day 10 capstone (real-time streams build).

Asserts (within reasonable timeouts):
  1. All Connect connectors are RUNNING (no FAILED tasks).
  2. Each stream + CDC topic has > 0 messages.
  3. MinIO bronze/, silver/, gold/ each contain >= 1 object.
  4. Quality + business dashboards return 200 on /health.

Exit non-zero on first hard failure.
"""
from __future__ import annotations

import sys
import time

import requests

CONNECT = "http://localhost:8083"
SR      = "http://localhost:8081"
KAFKA   = "localhost:9092"
MINIO_ENDPOINT = "localhost:9000"
QD = "http://localhost:5001"
BD = "http://localhost:5002"
LM = "http://localhost:5003"

STREAM_TOPICS = [
    "ogn.aircraft.positions",
    "noaa.observations",
    "noaa.alerts",
    "seismic.events",
]
CDC_TOPICS = [
    "config.public.regions",
    "config.public.alert_thresholds",
    "config.public.subscriber_watchlist",
]

OK   = "\033[32mOK\033[0m"
FAIL = "\033[31mFAIL\033[0m"


def step(name: str, ok: bool, detail: str = "") -> None:
    tag = OK if ok else FAIL
    print(f"[{tag}] {name} {('— ' + detail) if detail else ''}")
    if not ok:
        sys.exit(1)


def check_connectors():
    r = requests.get(f"{CONNECT}/connectors", timeout=10); r.raise_for_status()
    names = r.json()
    step("connectors discovered", len(names) >= 3, f"{len(names)} connectors")
    for n in names:
        s = requests.get(f"{CONNECT}/connectors/{n}/status", timeout=10).json()
        connector_state = s.get("connector", {}).get("state", "?")
        task_states = [t.get("state", "?") for t in s.get("tasks", [])]
        ok = (connector_state == "RUNNING" and all(ts == "RUNNING" for ts in task_states))
        step(f"connector {n}", ok, f"connector={connector_state} tasks={task_states}")


def check_topics():
    from confluent_kafka.admin import AdminClient
    from confluent_kafka import Consumer, TopicPartition
    admin = AdminClient({"bootstrap.servers": KAFKA})
    md = admin.list_topics(timeout=10)
    found = set(md.topics)
    targets = STREAM_TOPICS + CDC_TOPICS
    for t in targets:
        step(f"topic exists: {t}", t in found)
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": "smoke", "enable.auto.commit": False})
    for t in targets:
        total = 0
        for p in md.topics[t].partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(t, p), timeout=5)
            total += (high - low)
        # noaa.alerts may legitimately be empty if no US weather alerts are active;
        # do not hard-fail that one.
        if t == "noaa.alerts":
            print(f"  · noaa.alerts has {total} message(s) (informational; may be 0).")
            continue
        step(f"topic {t} has messages", total > 0, f"{total} msgs")
    consumer.close()


def check_minio():
    import boto3
    from botocore.config import Config
    s3 = boto3.client(
        "s3", endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin",
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    for b in ["bronze", "silver", "gold"]:
        try:
            resp = s3.list_objects_v2(Bucket=b, MaxKeys=1)
        except Exception as e:
            step(f"bucket {b}", False, f"{e}"); continue
        count = resp.get("KeyCount", 0)
        step(f"bucket {b} non-empty", count >= 1, f"{count} object(s) sampled")


def check_dashboards():
    for name, url in [("quality dashboard", f"{QD}/health"),
                      ("business dashboard", f"{BD}/health"),
                      ("live map dashboard", f"{LM}/health")]:
        try:
            r = requests.get(url, timeout=5)
            step(name, r.status_code == 200, f"GET /health -> {r.status_code}")
        except Exception as e:
            step(name, False, str(e))


def main() -> None:
    print("=== Day 10 capstone smoke test (real-time streams) ===")
    print("Waiting 30s for first DAG runs and ingestors to push data...")
    time.sleep(30)

    check_connectors()
    check_topics()
    check_minio()
    check_dashboards()
    print(f"\n[{OK}] all checks passed")


if __name__ == "__main__":
    main()

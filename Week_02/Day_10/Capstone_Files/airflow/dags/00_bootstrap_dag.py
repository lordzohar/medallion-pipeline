"""00_bootstrap — manually triggered (or by bootstrap.ps1).

Creates Kafka topics, registers Avro schemas, creates MinIO buckets, and
registers Debezium + S3 sink connectors. Idempotent.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
SR_URL          = os.environ["SCHEMA_REGISTRY_URL"]
CONNECT_URL     = os.environ["KAFKA_CONNECT_URL"]
MINIO_ENDPOINT  = os.environ["MINIO_ENDPOINT"]
MINIO_KEY       = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET    = os.environ["MINIO_SECRET_KEY"]
BUCKETS         = [os.environ["BRONZE_BUCKET"], os.environ["SILVER_BUCKET"], os.environ["GOLD_BUCKET"]]

TOPICS = [
    # Live streams
    ("ogn.aircraft.positions", 6),
    ("noaa.observations",      3),
    ("noaa.alerts",            3),
    ("seismic.events",         3),
    # CDC (config reference tables)
    ("config.public.regions",              1),
    ("config.public.alert_thresholds",     1),
    ("config.public.subscriber_watchlist", 1),
    ("config.heartbeat",                   1),
    # DLQ
    ("dlq.config-source",         1),
    ("dlq.s3-sink-bronze-cdc",    1),
    ("dlq.s3-sink-bronze-streams",1),
]


def create_topics(**_):
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    existing = set(admin.list_topics(timeout=10).topics.keys())
    new_topics = [
        NewTopic(name, num_partitions=p, replication_factor=1)
        for (name, p) in TOPICS if name not in existing
    ]
    if not new_topics:
        print("[ok] all topics already exist"); return
    futs = admin.create_topics(new_topics)
    for name, fut in futs.items():
        try:
            fut.result(10); print(f"[ok] created {name}")
        except Exception as e:
            print(f"[warn] {name}: {e}")


def create_buckets(**_):
    from minio import Minio
    client = Minio(MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
                   access_key=MINIO_KEY, secret_key=MINIO_SECRET, secure=False)
    for b in BUCKETS:
        if not client.bucket_exists(b):
            client.make_bucket(b); print(f"[ok] created bucket {b}")
        else:
            print(f"[skip] bucket exists: {b}")


def register_app_schemas(**_):
    schemas = {
        "ogn.aircraft.positions-value": "/opt/app/schemas/ogn_position.avsc",
        "noaa.observations-value":      "/opt/app/schemas/noaa_observation.avsc",
        "noaa.alerts-value":            "/opt/app/schemas/noaa_alert.avsc",
        "seismic.events-value":         "/opt/app/schemas/seismic_event.avsc",
    }
    for subject, path in schemas.items():
        p = Path(path)
        if not p.exists():
            print(f"[skip] schema file missing on this mount: {p}"); continue
        body = {"schema": p.read_text(), "schemaType": "AVRO"}
        r = requests.post(
            f"{SR_URL}/subjects/{subject}/versions",
            data=json.dumps(body),
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            timeout=10,
        )
        if not r.ok:
            print(f"[warn] {subject}: {r.status_code} {r.text}")
        else:
            print(f"[ok] {subject} -> {r.json()}")


def register_connectors(**_):
    connectors_dir = Path("/opt/debezium/connectors")
    for f in sorted(connectors_dir.glob("*.json")):
        cfg = json.loads(f.read_text())
        name = cfg["name"]
        r = requests.put(
            f"{CONNECT_URL}/connectors/{name}/config",
            json=cfg["config"],
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        if r.status_code in (200, 201):
            print(f"[ok] connector {name}")
        else:
            print(f"[warn] {name}: {r.status_code} {r.text}")


with DAG(
    "00_bootstrap",
    description="One-time topic+schema+bucket+connector setup.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["capstone", "bootstrap"],
) as dag:
    t_topics  = PythonOperator(task_id="create_topics",        python_callable=create_topics)
    t_buckets = PythonOperator(task_id="create_minio_buckets", python_callable=create_buckets)
    t_schemas = PythonOperator(task_id="register_app_schemas", python_callable=register_app_schemas)
    t_connect = PythonOperator(task_id="register_connectors",  python_callable=register_connectors)

    t_topics >> t_buckets >> t_schemas >> t_connect

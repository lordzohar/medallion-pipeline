"""40_data_quality — every 5 min run a rule pack against silver and push
findings to the quality dashboard via HTTP webhook.

Rules cover the live streams (OGN / NOAA / Seismic) + the CDC config tables.
"""
from __future__ import annotations

import io
import os
import time
from datetime import datetime, timedelta

import boto3
import fastavro
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from botocore.config import Config

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
SILVER_BUCKET  = os.environ["SILVER_BUCKET"]
DASH_URL       = os.environ["QUALITY_DASHBOARD_URL"]


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _read_avro_all(prefix: str) -> list[dict]:
    s = _s3()
    rows: list[dict] = []
    token = None
    while True:
        kw = {"Bucket": SILVER_BUCKET, "Prefix": prefix, "MaxKeys": 1000}
        if token: kw["ContinuationToken"] = token
        resp = s.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            if "/_quality/" in o["Key"]: continue
            try:
                body = s.get_object(Bucket=SILVER_BUCKET, Key=o["Key"])["Body"].read()
                rows.extend(list(fastavro.reader(io.BytesIO(body))))
            except Exception:
                continue
        if not resp.get("IsTruncated"): return rows
        token = resp.get("NextContinuationToken")


# ---------- rule pack ----------

def rule_seismic_has_magnitude(rows: list[dict]) -> tuple[bool, str]:
    bad = [r for r in rows if r.get("magnitude") in (None, 0)]
    return (len(bad) == 0, f"{len(bad)}/{len(rows)} seismic rows with null/zero magnitude")


def rule_noaa_obs_coords(rows: list[dict]) -> tuple[bool, str]:
    bad = [r for r in rows if r.get("lat") is None or r.get("lon") is None]
    rate = (len(bad)/len(rows)*100) if rows else 0
    return (rate < 5.0, f"NOAA observation null-coord rate {rate:.2f}% (threshold 5%)")


def rule_ogn_positions_non_null_pk(rows: list[dict]) -> tuple[bool, str]:
    bad = [r for r in rows if not (r.get("address") or r.get("callsign"))]
    rate = (len(bad)/len(rows)*100) if rows else 0
    return (rate < 10.0, f"OGN positions without address+callsign {rate:.2f}% (threshold 10%)")


def rule_late_events_under_1pct(rows: list[dict], horizon_ms: int = 24*3600*1000) -> tuple[bool, str]:
    if not rows: return (True, "no rows")
    max_ts = max((r.get("ts_ms") or 0) for r in rows)
    late = [r for r in rows if (r.get("ts_ms") or 0) < max_ts - horizon_ms]
    rate = (len(late)/len(rows)*100)
    return (rate < 1.0, f"late event rate {rate:.2f}% (threshold 1%)")


def rule_ingest_freshness(rows: list[dict], max_age_sec: int = 600) -> tuple[bool, str]:
    """Latest ingested_ms must be within max_age_sec — catches stuck ingestors."""
    if not rows: return (False, "no rows in last 2h")
    now_ms = int(time.time() * 1000)
    latest = max((r.get("ingested_ms") or 0) for r in rows)
    age = (now_ms - latest) / 1000
    return (age <= max_age_sec, f"freshest record is {age:.0f}s old (threshold {max_age_sec}s)")


def rule_regions_active_count(rows: list[dict]) -> tuple[bool, str]:
    active = [r for r in rows if r.get("is_active") is True]
    return (len(active) >= 50, f"{len(active)} active regions (need >= 50)")


RULES = [
    ("seismic_events/",        "seismic.has_magnitude",     rule_seismic_has_magnitude),
    ("seismic_events/",        "seismic.freshness",         rule_ingest_freshness),
    ("noaa_observations/",     "noaa.obs_coords_present",   rule_noaa_obs_coords),
    ("noaa_observations/",     "noaa.freshness",            lambda r: rule_ingest_freshness(r, 1800)),
    ("ogn_positions/",         "ogn.positions_have_pk",     rule_ogn_positions_non_null_pk),
    ("ogn_positions/",         "ogn.late_events_lt_1pct",   rule_late_events_under_1pct),
    ("regions/",               "config.regions_active_min", rule_regions_active_count),
]


def run_rules(**_):
    results = []
    cache: dict[str, list[dict]] = {}
    for prefix, name, rule in RULES:
        rows = cache.setdefault(prefix, _read_avro_all(prefix))
        ok, msg = rule(rows)
        status = "PASS" if ok else "FAIL"
        results.append({
            "rule": name, "status": status, "detail": msg,
            "rows_scanned": len(rows),
            "checked_at": datetime.utcnow().isoformat() + "Z",
        })
        print(f"[{status}] {name}: {msg}")

    try:
        r = requests.post(f"{DASH_URL}/api/quality/results", json={"results": results}, timeout=5)
        print(f"[push] dashboard responded {r.status_code}")
    except Exception as e:
        print(f"[warn] could not POST to dashboard: {e}")


with DAG(
    "40_data_quality",
    description="Run quality rules against silver and push to quality dashboard.",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    tags=["capstone", "quality"],
) as dag:
    PythonOperator(task_id="run_rule_pack", python_callable=run_rules)

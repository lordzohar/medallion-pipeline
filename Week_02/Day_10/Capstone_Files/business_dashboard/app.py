"""Business dashboard — reads `latest.parquet` for each gold mart from MinIO.

POST /api/refresh forces a cache reload (called every 5 min by Airflow DAG
50_business_kpis). Browser GET / renders a static HTML view.
"""
from __future__ import annotations

import io
import os
import threading
from datetime import datetime, timezone

import boto3
import pyarrow.parquet as pq
from botocore.config import Config
from flask import Flask, render_template, jsonify
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST

app = Flask(__name__)

ENDPOINT = os.environ["MINIO_ENDPOINT"]
ACCESS   = os.environ["MINIO_ACCESS_KEY"]
SECRET   = os.environ["MINIO_SECRET_KEY"]
GOLD     = os.environ["GOLD_BUCKET"]

MARTS = [
    "aircraft_density_by_region",
    "weather_snapshot",
    "seismic_24h_summary",
    "region_alert_correlation",
]

_cache: dict[str, list[dict]] = {}
_cache_at: datetime = datetime.now(timezone.utc)
_lock = threading.Lock()

M_REFRESH   = Counter("business_dashboard_refresh_total", "Cache refreshes.")
M_MART_ROWS = Gauge("business_dashboard_mart_rows", "Rows per mart in cache.", labelnames=("mart",))


def _s3():
    return boto3.client(
        "s3", endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS, aws_secret_access_key=SECRET,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _read_latest(mart: str) -> list[dict]:
    try:
        body = _s3().get_object(Bucket=GOLD, Key=f"{mart}/latest.parquet")["Body"].read()
        table = pq.read_table(io.BytesIO(body))
        return table.to_pylist()
    except Exception as e:
        print(f"[warn] {mart}: {e}")
        return []


def refresh() -> dict[str, int]:
    global _cache_at
    out = {}
    with _lock:
        for m in MARTS:
            rows = _read_latest(m)
            _cache[m] = rows
            out[m] = len(rows)
            M_MART_ROWS.labels(mart=m).set(len(rows))
        _cache_at = datetime.now(timezone.utc)
    M_REFRESH.inc()
    return out


try:
    refresh()
except Exception as e:
    print(f"[warn] initial refresh failed: {e}")


@app.route("/")
def index():
    with _lock:
        seismic = _cache.get("seismic_24h_summary", [])
        return render_template(
            "dashboard.html",
            aircraft=_cache.get("aircraft_density_by_region", [])[:50],
            weather=_cache.get("weather_snapshot", [])[:50],
            seismic=seismic[:100],
            seismic_total=sum((r.get("events") or 0) for r in seismic),
            correlation=_cache.get("region_alert_correlation", [])[:30],
            cache_at=_cache_at.isoformat(),
        )


@app.route("/health")
def health(): return {"status": "ok"}, 200


@app.route("/metrics")
def metrics(): return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    return jsonify(refresh()), 200


@app.route("/api/mart/<name>")
def api_mart(name: str):
    with _lock:
        return jsonify(_cache.get(name, [])), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002, debug=False)

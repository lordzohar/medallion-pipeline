"""50_business_kpis — every 5 min, ping the business dashboard so it picks
up the freshly-written gold parquet snapshot. The dashboard reads from
s3://gold/<mart>/latest.parquet directly; this DAG just nudges it to
refresh its in-memory cache.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

DASH_URL = os.environ["BUSINESS_DASHBOARD_URL"]


def nudge(**_):
    try:
        r = requests.post(f"{DASH_URL}/api/refresh", timeout=10)
        print(f"[push] {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"[warn] cannot reach dashboard: {e}")


with DAG(
    "50_business_kpis",
    description="Nudge the business dashboard to refresh its KPI cache.",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    tags=["capstone", "serving"],
) as dag:
    PythonOperator(task_id="refresh_cache", python_callable=nudge)

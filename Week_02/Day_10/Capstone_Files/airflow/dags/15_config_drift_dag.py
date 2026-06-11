"""15_config_drift — every 2 min, push a handful of mutations to the
reference tables so Debezium produces continuous CDC events.

Mimics ops occasionally tweaking regions / thresholds / watchlist rows.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "15_config_drift",
    description="Mutate config tables to keep CDC topics warm.",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=2),
    catchup=False,
    max_active_runs=1,
    tags=["capstone", "cdc"],
) as dag:
    BashOperator(
        task_id="mutate_config",
        bash_command="docker exec app python /app/config_drift.py --mutations=20",
    )

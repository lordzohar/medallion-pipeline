"""30_hop_medallion — every 5 min, run bronze->silver->gold via the HopOperator.

Defaults to mode="python" (runs the working Python reference transforms inside
the `hop` container). Switch to mode="hop-run" once the Hop project is opened
in the GUI and tested.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
from plugins.hop_operator import HopOperator  # noqa: E402

from airflow import DAG  # noqa: E402

STREAM_ENTITIES = ["ogn_positions", "noaa_observations", "noaa_alerts", "seismic_events"]
CDC_ENTITIES    = ["regions", "alert_thresholds", "subscriber_watchlist"]
GOLD_MARTS      = [
    "aircraft_density_by_region",
    "weather_snapshot",
    "seismic_24h_summary",
    "region_alert_correlation",
]


with DAG(
    "30_hop_medallion",
    description="Bronze -> Silver -> Gold refresh via Apache Hop / Python ref.",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    tags=["capstone", "transform"],
) as dag:

    silver_streams = [
        HopOperator(
            task_id=f"silver_stream_{e}",
            mode="python",
            script="bronze_to_silver.py",
            args=[f"--stream={e}"],
        )
        for e in STREAM_ENTITIES
    ]

    silver_cdc = [
        HopOperator(
            task_id=f"silver_cdc_{e}",
            mode="python",
            script="bronze_to_silver.py",
            args=[f"--cdc={e}"],
        )
        for e in CDC_ENTITIES
    ]

    gold = [
        HopOperator(
            task_id=f"gold_{m}",
            mode="python",
            script="silver_to_gold.py",
            args=[f"--mart={m}"],
        )
        for m in GOLD_MARTS
    ]

    # All silver in parallel, then gold (which depends on silver fan-out)
    for g in gold:
        for upstream in silver_streams + silver_cdc:
            upstream >> g

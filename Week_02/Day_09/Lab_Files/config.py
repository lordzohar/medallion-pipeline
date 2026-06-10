"""
config.py
---------
Single source of truth for runtime configuration.

Reads:
  * config.json  - business / data parameters (zones, fares, quality bounds)
  * environment variables - infrastructure addresses (Kafka, Postgres)

Every service in this lab imports its settings from here so nothing is
hard-coded inside the application code.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent
with open(ROOT / "config.json", "r", encoding="utf-8") as fh:
    _CFG = json.load(fh)

# ---------------------------------------------------------------------------
# Infrastructure (overridable per-environment via env vars) -----------------
KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP",
    "localhost:9092,localhost:9094,localhost:9095",
)
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN",
    "postgresql://taxi:taxi@localhost:5432/taxi",
)

# Topics ---------------------------------------------------------------------
TOPIC_TRIPS_RAW       = "taxi-trips"
TOPIC_GPS_PINGS       = "gps-pings"
TOPIC_TRIPS_CLEAN     = "trips-clean"
TOPIC_TRIPS_ENRICHED  = "trips-enriched"
TOPIC_TRIPS_DLQ       = "trips-dlq"
TOPIC_SURGE_EVENTS    = "surge-events"
TOPIC_DRIVERS_CDC     = "cdc.public.drivers"   # written by Debezium

# Business / data parameters -------------------------------------------------
ZONES         = _CFG["zones"]
PAYMENT_TYPES = _CFG["payment_types"]
FARE          = _CFG["fare"]
QUALITY       = _CFG["quality"]
CORRUPTION_RATE = _CFG["corruption_rate"]

ZONES_BY_NAME = {z["name"]: z for z in ZONES}


def zone_names() -> list[str]:
    return [z["name"] for z in ZONES]


def zone_center(name: str) -> tuple[float, float]:
    z = ZONES_BY_NAME[name]
    return (z["lat"], z["lon"])

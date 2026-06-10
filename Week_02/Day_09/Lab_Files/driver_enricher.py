"""
driver_enricher.py
------------------
Stream-table join.

Builds an in-memory KTable from the CDC topic `cdc.public.drivers`
(populated by Debezium from Postgres), then enriches every record on
`trips-clean` with the driver's full_name / vehicle / rating and
publishes to `trips-enriched`.

Demonstrates:
  - Two consumers in one process (changelog + fact stream)
  - Stream-table join pattern (Kafka Streams' canonical operation)
  - Materialized view kept up to date by CDC
"""
from __future__ import annotations

import json
import logging
import signal
import threading
from typing import Any

from confluent_kafka import Consumer, Producer
from prometheus_client import Counter, Gauge, start_http_server

from config import (
    KAFKA_BOOTSTRAP,
    TOPIC_DRIVERS_CDC,
    TOPIC_TRIPS_CLEAN,
    TOPIC_TRIPS_ENRICHED,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [enricher] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

ENRICHED = Counter("trips_enriched_total", "Trips successfully enriched")
MISSING_DRIVER = Counter("trips_missing_driver_total", "Trips referencing unknown driver")
DRIVERS_LOADED = Gauge("drivers_table_size", "Current size of in-memory drivers KTable")

# Materialized view: driver_id -> dict
drivers_table: dict[str, dict[str, Any]] = {}
table_lock = threading.Lock()
stop = {"f": False}


# ---------------------------------------------------------------------------
def cdc_loop():
    """Continuously hydrates the in-memory drivers table from CDC topic."""
    c = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "driver-enricher-cdc",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    c.subscribe([TOPIC_DRIVERS_CDC])
    log.info("CDC consumer started on %s", TOPIC_DRIVERS_CDC)
    while not stop["f"]:
        msg = c.poll(1.0)
        if not msg or msg.error():
            continue
        try:
            payload = json.loads(msg.value()) if msg.value() else None
        except Exception:
            continue

        if msg.key():
            try:
                key = json.loads(msg.key())
                driver_id = key.get("driver_id") if isinstance(key, dict) else str(key)
            except Exception:
                driver_id = msg.key().decode("utf-8", "ignore")
        elif payload:
            driver_id = payload.get("driver_id")
        else:
            continue

        with table_lock:
            if payload is None:                # tombstone -> delete
                drivers_table.pop(driver_id, None)
            else:
                drivers_table[driver_id] = payload
            DRIVERS_LOADED.set(len(drivers_table))


# ---------------------------------------------------------------------------
def enrich_loop():
    c = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "driver-enricher-trips",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "client.id": "driver-enricher"})
    c.subscribe([TOPIC_TRIPS_CLEAN])
    log.info("trip consumer started on %s", TOPIC_TRIPS_CLEAN)

    while not stop["f"]:
        msg = c.poll(1.0)
        if not msg or msg.error():
            continue
        try:
            trip = json.loads(msg.value())
        except Exception:
            continue

        with table_lock:
            driver = drivers_table.get(trip.get("driver_id"))

        if not driver:
            MISSING_DRIVER.inc()
            continue

        enriched = {
            **trip,
            "driver_name":    driver.get("full_name"),
            "driver_rating":  float(driver["rating"]) if driver.get("rating") is not None else None,
            "vehicle":        f"{driver.get('vehicle_year')} {driver.get('vehicle_make')} {driver.get('vehicle_model')}",
            "license_number": driver.get("license_number"),
        }
        p.produce(
            TOPIC_TRIPS_ENRICHED,
            key=trip["driver_id"].encode(),
            value=json.dumps(enriched).encode(),
        )
        ENRICHED.inc()
        p.poll(0)
        c.commit(msg, asynchronous=True)


# ---------------------------------------------------------------------------
def main():
    start_http_server(8005)
    signal.signal(signal.SIGINT, lambda *_: stop.update(f=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(f=True))

    t = threading.Thread(target=cdc_loop, daemon=True)
    t.start()
    enrich_loop()


if __name__ == "__main__":
    main()

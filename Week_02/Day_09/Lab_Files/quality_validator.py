"""
quality_validator.py
====================
Runs Great Expectations against micro-batches read from `trips-clean`.

Why this script exists:
  * Demonstrates ALL the same expectation types used in batch analytics
    but on a streaming source (mini-batches of 25 records).
  * Bounds and rule definitions are loaded from config.json so changing
    a threshold needs zero code edits.
  * Failures route to `trips-dlq` with the expectation name preserved
    so the DLQ tool can show *why* each record was rejected.

Quality rules (read from config.json -> "quality" block):
  | column          | rule                       | bounds                 |
  | --------------- | -------------------------- | ---------------------- |
  | trip_id         | not null                   | -                      |
  | driver_id       | not null                   | -                      |
  | fare_amount     | between                    | 0 .. 1000              |
  | distance_miles  | between                    | 0 .. 150               |
  | passenger_count | between                    | 1 .. 8                 |
  | payment_type    | in set                     | credit_card,cash,...   |
  | pickup_lat      | between                    | 40.4 .. 41.0           |
  | pickup_lon      | between                    | -74.3 .. -73.5         |
"""
from __future__ import annotations

import argparse
import json
import logging
import signal

import great_expectations as gx
import pandas as pd
from confluent_kafka import Consumer, Producer
from prometheus_client import Counter, Gauge, start_http_server

from config import (
    KAFKA_BOOTSTRAP,
    PAYMENT_TYPES,
    QUALITY,
    TOPIC_TRIPS_CLEAN,
    TOPIC_TRIPS_DLQ,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [quality] %(levelname)s %(message)s")
log = logging.getLogger(__name__)

QUALITY_SCORE = Gauge("data_quality_score", "Percent of expectations passing in last batch")
PASSED = Counter("gx_expectations_passed_total", "Expectations passed", ["expectation"])
FAILED = Counter("gx_expectations_failed_total", "Expectations failed", ["expectation"])
RECORDS = Counter("gx_records_validated_total", "Records validated")


# ---------------------------------------------------------------------------
# Build the expectation suite by RUNNING each expectation on the dataset.
# This is the simple, stable GX 0.18 PandasDataset API.
# ---------------------------------------------------------------------------
def validate_batch(df: pd.DataFrame) -> list[tuple[str, bool, list]]:
    """Return list of (expectation_name, success, unexpected_values_sample)."""
    ds = gx.from_pandas(df)
    checks = []

    def _record(name, result):
        success = bool(result.success)
        unexpected = result.result.get("partial_unexpected_list", [])[:5] if not success else []
        checks.append((name, success, unexpected))

    _record("trip_id_not_null",        ds.expect_column_values_to_not_be_null("trip_id"))
    _record("driver_id_not_null",      ds.expect_column_values_to_not_be_null("driver_id"))
    _record("fare_amount_in_range",    ds.expect_column_values_to_be_between(
        "fare_amount", min_value=QUALITY["fare_min"], max_value=QUALITY["fare_max"]))
    _record("distance_in_range",       ds.expect_column_values_to_be_between(
        "distance_miles", min_value=QUALITY["distance_min"], max_value=QUALITY["distance_max"]))
    _record("passengers_in_range",     ds.expect_column_values_to_be_between(
        "passenger_count", min_value=QUALITY["passenger_min"], max_value=QUALITY["passenger_max"]))
    _record("payment_type_in_set",     ds.expect_column_values_to_be_in_set(
        "payment_type", value_set=PAYMENT_TYPES))
    _record("pickup_lat_in_nyc",       ds.expect_column_values_to_be_between(
        "pickup_lat", min_value=QUALITY["lat_min"], max_value=QUALITY["lat_max"]))
    _record("pickup_lon_in_nyc",       ds.expect_column_values_to_be_between(
        "pickup_lon", min_value=QUALITY["lon_min"], max_value=QUALITY["lon_max"]))

    return checks


# ---------------------------------------------------------------------------
def run(batch_size: int):
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "gx-quality-v1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "client.id": "gx"})
    consumer.subscribe([TOPIC_TRIPS_CLEAN])

    buffer: list[dict] = []
    stop = {"f": False}
    signal.signal(signal.SIGINT,  lambda *_: stop.update(f=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(f=True))

    log.info("GX validator running. batch_size=%d", batch_size)
    while not stop["f"]:
        msg = consumer.poll(1.0)
        if msg and not msg.error():
            try:
                buffer.append(json.loads(msg.value()))
            except Exception as e:
                log.warning("parse err: %s", e)

        if len(buffer) >= batch_size:
            df = pd.DataFrame(buffer)
            checks = validate_batch(df)
            RECORDS.inc(len(df))

            passed = 0
            for name, success, unexpected in checks:
                if success:
                    PASSED.labels(expectation=name).inc()
                    passed += 1
                else:
                    FAILED.labels(expectation=name).inc()
                    log.warning("FAIL %-30s sample=%s", name, unexpected)
                    # Route the offending rows to DLQ with rule name preserved
                    for row in df.to_dict("records"):
                        bad = {**row, "_dlq_reason": name}
                        producer.produce(
                            TOPIC_TRIPS_DLQ,
                            key=str(row.get("trip_id", "x")).encode(),
                            value=json.dumps(bad).encode(),
                        )

            score = 100.0 * passed / max(len(checks), 1)
            QUALITY_SCORE.set(score)
            log.info("batch=%d score=%.1f%% passed=%d/%d", len(df), score, passed, len(checks))
            producer.poll(0)
            buffer.clear()

    consumer.close()
    producer.flush(5)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch-size",   type=int, default=25)
    ap.add_argument("--metrics-port", type=int, default=8004)
    args = ap.parse_args()
    start_http_server(args.metrics_port)
    run(args.batch_size)


if __name__ == "__main__":
    main()

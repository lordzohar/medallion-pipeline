"""
quality_validator.py
====================
Streaming data-quality validator.

Reads micro-batches from `trips-clean`, runs a fixed suite of
expectations against each batch, increments per-rule Prometheus
counters, and routes any failing row into `trips-dlq` with the
expectation name preserved in `_dlq_reason`.

The expectation API is deliberately tiny (~30 lines) so students can
read it end-to-end. It mirrors what Great Expectations / Soda / Deequ
do conceptually:

    Expectation(name, predicate(row) -> bool)

All bounds are pulled from config.json -> "quality" block so changing
a threshold needs zero code edits.

Quality rules:
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
from dataclasses import dataclass
from typing import Callable

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
@dataclass
class Expectation:
    name: str
    predicate: Callable[[dict], bool]


def _not_null(col: str) -> Callable[[dict], bool]:
    return lambda r: r.get(col) not in (None, "")


def _between(col: str, lo: float, hi: float) -> Callable[[dict], bool]:
    def _check(r: dict) -> bool:
        v = r.get(col)
        try:
            return v is not None and lo <= float(v) <= hi
        except (TypeError, ValueError):
            return False
    return _check


def _in_set(col: str, allowed: set) -> Callable[[dict], bool]:
    return lambda r: r.get(col) in allowed


SUITE: list[Expectation] = [
    Expectation("trip_id_not_null",     _not_null("trip_id")),
    Expectation("driver_id_not_null",   _not_null("driver_id")),
    Expectation("fare_amount_in_range", _between("fare_amount",
                                                 QUALITY["fare_min"], QUALITY["fare_max"])),
    Expectation("distance_in_range",    _between("distance_miles",
                                                 QUALITY["distance_min"], QUALITY["distance_max"])),
    Expectation("passengers_in_range",  _between("passenger_count",
                                                 QUALITY["passenger_min"], QUALITY["passenger_max"])),
    Expectation("payment_type_in_set",  _in_set("payment_type", set(PAYMENT_TYPES))),
    Expectation("pickup_lat_in_nyc",    _between("pickup_lat",
                                                 QUALITY["lat_min"], QUALITY["lat_max"])),
    Expectation("pickup_lon_in_nyc",    _between("pickup_lon",
                                                 QUALITY["lon_min"], QUALITY["lon_max"])),
]


def validate_batch(batch: list[dict]) -> tuple[int, int, dict[str, list[dict]]]:
    """Return (passed_count, failed_count, failures_by_rule)."""
    passed = 0
    failed = 0
    failures: dict[str, list[dict]] = {}
    for row in batch:
        row_ok = True
        for exp in SUITE:
            if not exp.predicate(row):
                failures.setdefault(exp.name, []).append(row)
                row_ok = False
        if row_ok:
            passed += 1
        else:
            failed += 1
    return passed, failed, failures


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

    log.info("validator running. batch_size=%d  suite_size=%d", batch_size, len(SUITE))
    while not stop["f"]:
        msg = consumer.poll(1.0)
        if msg and not msg.error():
            try:
                buffer.append(json.loads(msg.value()))
            except Exception as e:
                log.warning("parse err: %s", e)

        if len(buffer) >= batch_size:
            RECORDS.inc(len(buffer))

            # Count per-expectation pass/fail across the batch
            rule_fail_counts: dict[str, int] = {e.name: 0 for e in SUITE}
            for row in buffer:
                for exp in SUITE:
                    if not exp.predicate(row):
                        rule_fail_counts[exp.name] += 1

            for name, fails in rule_fail_counts.items():
                passes = len(buffer) - fails
                if passes > 0:
                    PASSED.labels(expectation=name).inc(passes)
                if fails > 0:
                    FAILED.labels(expectation=name).inc(fails)

            # Per-row failure handling -> DLQ
            passed, failed, failures = validate_batch(buffer)
            for rule, rows in failures.items():
                for row in rows:
                    bad = {**row, "_dlq_reason": rule}
                    producer.produce(
                        TOPIC_TRIPS_DLQ,
                        key=str(row.get("trip_id", "x")).encode(),
                        value=json.dumps(bad).encode(),
                    )

            total_checks = len(buffer) * len(SUITE)
            total_passed = total_checks - sum(rule_fail_counts.values())
            score = 100.0 * total_passed / max(total_checks, 1)
            QUALITY_SCORE.set(score)
            log.info("batch=%d rows passed=%d failed=%d score=%.1f%%",
                     len(buffer), passed, failed, score)
            if failed:
                worst = sorted(rule_fail_counts.items(), key=lambda kv: -kv[1])[:3]
                log.info("  top failing rules: %s", worst)

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
    log.info("prometheus metrics on :%d", args.metrics_port)
    run(args.batch_size)


if __name__ == "__main__":
    main()

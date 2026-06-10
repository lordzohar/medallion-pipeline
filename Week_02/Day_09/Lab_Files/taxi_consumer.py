"""
Trip Consumer & Aggregator
==========================
Consumes from `taxi-trips`, computes rolling metrics, writes the
clean stream to `trips-clean` and routes invalid records to `trips-dlq`.

Demonstrates:
  - Consumer groups & manual offset commit
  - At-least-once delivery semantics
  - Light validation in-stream (heavy validation is in quality_validator.py)
  - Prometheus instrumentation of a Kafka consumer
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import time
from collections import defaultdict, deque

from confluent_kafka import Consumer, Producer, KafkaError
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from config import KAFKA_BOOTSTRAP, TOPIC_TRIPS_RAW, TOPIC_TRIPS_CLEAN, TOPIC_TRIPS_DLQ

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [consumer] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ---- Prometheus ------------------------------------------------------------
TRIPS_IN = Counter("taxi_trips_consumed_total", "Trips consumed", ["zone"])
TRIPS_VALID = Counter("taxi_trips_valid_total", "Valid trips", ["zone"])
TRIPS_INVALID = Counter("taxi_trips_invalid_total", "Invalid trips", ["reason"])
PROCESS_TIME = Histogram("taxi_consumer_process_seconds", "Per-record processing")
REVENUE = Counter("taxi_revenue_dollars_total", "Total revenue", ["zone"])
LAST_FARE = Gauge("taxi_last_fare", "Last seen fare amount")

WINDOW_SECS = 60
recent_trips: deque = deque()  # (ts, zone, fare)


def light_validate(trip: dict) -> tuple[bool, str | None]:
    if "_corruption" in trip:
        return False, trip["_corruption"]
    required = ["trip_id", "driver_id", "fare_amount", "distance_miles", "pickup_zone"]
    for f in required:
        if trip.get(f) in (None, ""):
            return False, f"missing_{f}"
    if trip["fare_amount"] < 0:
        return False, "negative_fare"
    if not (0 < trip["distance_miles"] < 200):
        return False, "distance_out_of_range"
    return True, None


def rolling_summary():
    now = time.time()
    while recent_trips and now - recent_trips[0][0] > WINDOW_SECS:
        recent_trips.popleft()
    by_zone: dict[str, list[float]] = defaultdict(list)
    for _, zone, fare in recent_trips:
        by_zone[zone].append(fare)
    return {z: {"count": len(v), "revenue": round(sum(v), 2)} for z, v in by_zone.items()}


def run(bootstrap: str, group: str):
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
    })
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "trip-consumer",
        "compression.type": "snappy",
    })
    consumer.subscribe([TOPIC_TRIPS_RAW])

    stop = {"f": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(f=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(f=True))

    last_report = time.time()
    log.info("consumer started, group=%s", group)

    while not stop["f"]:
        msg = consumer.poll(1.0)
        if msg is None:
            if time.time() - last_report > 10:
                log.info("rolling 60s window: %s", rolling_summary())
                last_report = time.time()
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            log.error(msg.error()); continue

        with PROCESS_TIME.time():
            try:
                trip = json.loads(msg.value())
                zone = trip.get("pickup_zone", "UNKNOWN")
                TRIPS_IN.labels(zone=zone).inc()

                ok, reason = light_validate(trip)
                if ok:
                    TRIPS_VALID.labels(zone=zone).inc()
                    REVENUE.labels(zone=zone).inc(float(trip["total_amount"]))
                    LAST_FARE.set(float(trip["fare_amount"]))
                    recent_trips.append((time.time(), zone, float(trip["total_amount"])))
                    producer.produce(TOPIC_TRIPS_CLEAN,
                                     key=trip["driver_id"].encode(),
                                     value=json.dumps(trip).encode())
                else:
                    TRIPS_INVALID.labels(reason=reason).inc()
                    bad = {**trip, "_dlq_reason": reason, "_dlq_ts": time.time()}
                    producer.produce(TOPIC_TRIPS_DLQ,
                                     key=str(trip.get("trip_id", "x")).encode(),
                                     value=json.dumps(bad).encode())

                producer.poll(0)
                consumer.commit(msg, asynchronous=True)
            except Exception as e:
                log.exception("processing error: %s", e)
                TRIPS_INVALID.labels(reason="exception").inc()

    consumer.close()
    producer.flush(5)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    p.add_argument("--group", default="trip-processor-v1")
    p.add_argument("--metrics-port", type=int, default=8002)
    args = p.parse_args()
    start_http_server(args.metrics_port)
    log.info("metrics on :%d", args.metrics_port)
    run(args.bootstrap, args.group)


if __name__ == "__main__":
    main()

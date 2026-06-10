"""
taxi_simulator.py
=================
NYC Uber-style taxi simulator. Produces TWO Kafka topics:

  * gps-pings   - high-volume, one ping per driver per tick
  * taxi-trips  - one event per completed trip (the "fact" stream)

Driver IDs are pulled from the Postgres `drivers` table (seeded by
db_seeder.py). That way every trip event references a real row that
Debezium replicates into Kafka as `cdc.public.drivers`, which the
driver_enricher.py service joins back in. End-to-end realistic.

Every business-data parameter (zones, fare schedule, quality bounds,
corruption rate) lives in config.json. No hard-coded magic numbers.

Run:
    python taxi_simulator.py --drivers 50 --rate 1.0
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import random
import signal
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum

import psycopg2
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from config import (
    CORRUPTION_RATE,
    FARE,
    KAFKA_BOOTSTRAP,
    PAYMENT_TYPES,
    POSTGRES_DSN,
    TOPIC_GPS_PINGS,
    TOPIC_TRIPS_RAW,
    ZONES,
    ZONES_BY_NAME,
    zone_center,
    zone_names,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [taxi-sim] %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
TRIPS_STARTED   = Counter("taxi_trips_started_total",  "Trips started",   ["zone"])
TRIPS_COMPLETED = Counter("taxi_trips_completed_total","Trips completed", ["zone"])
GPS_PINGS       = Counter("taxi_gps_pings_total",      "GPS pings emitted")
BAD_RECORDS     = Counter("taxi_bad_records_injected_total", "Bad records injected")
ACTIVE_DRIVERS  = Gauge  ("taxi_active_drivers",       "Drivers in each state", ["state"])
PRODUCE_LATENCY = Histogram("taxi_produce_latency_seconds", "Produce ack latency")


class DriverState(str, Enum):
    IDLE = "IDLE"
    EN_ROUTE_PICKUP = "EN_ROUTE_PICKUP"
    ON_TRIP = "ON_TRIP"


@dataclass
class Driver:
    driver_id: str
    lat: float
    lon: float
    state: DriverState = DriverState.IDLE
    trip_id:        str | None  = None
    pickup:         tuple | None = None
    dropoff:        tuple | None = None
    pickup_zone:    str | None = None
    dropoff_zone:   str | None = None
    trip_started_at: float | None = None
    speed_mph: float = 25.0


# ---------------------------------------------------------------------------
# Geometry helpers (haversine + straight-line interpolation)
# ---------------------------------------------------------------------------
def haversine_miles(p1: tuple, p2: tuple) -> float:
    R = 3958.8
    lat1, lon1, lat2, lon2 = map(math.radians, [p1[0], p1[1], p2[0], p2[1]])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def move_toward(driver: Driver, target: tuple, tick_seconds: float) -> bool:
    dist = haversine_miles((driver.lat, driver.lon), target)
    if dist < 0.05:
        driver.lat, driver.lon = target
        return True
    step = (driver.speed_mph / 3600.0) * tick_seconds
    frac = min(1.0, step / dist)
    driver.lat += (target[0] - driver.lat) * frac
    driver.lon += (target[1] - driver.lon) * frac
    return False


def pick_zone() -> str:
    weights = [z["base_demand"] for z in ZONES]
    return random.choices(zone_names(), weights=weights, k=1)[0]


def spawn_in_zone(zone: str) -> tuple:
    lat, lon = zone_center(zone)
    return (lat + random.uniform(-0.005, 0.005), lon + random.uniform(-0.005, 0.005))


# ---------------------------------------------------------------------------
# Fare schedule (Uber-like). All coefficients from config.json
# ---------------------------------------------------------------------------
def calculate_fare(distance_miles: float, duration_min: float, surge: float = 1.0) -> dict:
    sub = FARE["base"] + FARE["per_mile"] * distance_miles + FARE["per_minute"] * duration_min
    fare = round(sub * surge, 2)
    tip = round(fare * random.uniform(0, FARE["max_tip_pct"]), 2)
    return {
        "base_fare":        FARE["base"],
        "distance_charge":  round(FARE["per_mile"]   * distance_miles, 2),
        "time_charge":      round(FARE["per_minute"] * duration_min, 2),
        "surge_multiplier": surge,
        "fare_amount":      fare,
        "tip_amount":       tip,
        "total_amount":     round(fare + tip, 2),
    }


# ---------------------------------------------------------------------------
# Bad-data injector - keeps the data-quality lab honest
# ---------------------------------------------------------------------------
def maybe_corrupt(record: dict) -> dict:
    if random.random() > CORRUPTION_RATE:
        return record
    BAD_RECORDS.inc()
    choice = random.choice(["neg_fare", "missing", "outlier_dist", "bad_coord", "future_ts"])
    if choice == "neg_fare":      record["fare_amount"] = -abs(record.get("fare_amount", 10))
    elif choice == "missing":     record.pop("driver_id", None)
    elif choice == "outlier_dist":record["distance_miles"] = 9999.0
    elif choice == "bad_coord":   record["pickup_lat"] = 999.0
    elif choice == "future_ts":   record["pickup_time"] = "2099-01-01T00:00:00"
    record["_corruption"] = choice
    return record


# ---------------------------------------------------------------------------
def load_driver_ids(limit: int) -> list[str]:
    """Pull real driver IDs from Postgres so trip events reference real rows."""
    with psycopg2.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT driver_id FROM drivers WHERE is_active=TRUE ORDER BY driver_id LIMIT %s",
                    (limit,))
        ids = [r[0] for r in cur.fetchall()]
    if not ids:
        raise SystemExit("No drivers in Postgres. Run db_seeder.py first.")
    return ids


# ---------------------------------------------------------------------------
def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "taxi-simulator",
        "compression.type": "snappy",
        "acks": "all",
        "linger.ms": 50,
        "batch.size": 32 * 1024,
        "enable.idempotence": True,
    })


def emit(producer: Producer, topic: str, key: str, value: dict):
    t0 = time.time()
    producer.produce(topic, key=key.encode(), value=json.dumps(value).encode())
    PRODUCE_LATENCY.observe(time.time() - t0)
    producer.poll(0)


def update_state_gauges(drivers: list[Driver]):
    counts = {s.value: 0 for s in DriverState}
    for d in drivers:
        counts[d.state.value] += 1
    for state, c in counts.items():
        ACTIVE_DRIVERS.labels(state=state).set(c)


# ---------------------------------------------------------------------------
def run(n_drivers: int, tick: float):
    log.info("loading %d driver ids from postgres...", n_drivers)
    driver_ids = load_driver_ids(n_drivers)
    log.info("starting simulator: drivers=%d tick=%ss", len(driver_ids), tick)

    producer = build_producer()
    drivers: list[Driver] = []
    for did in driver_ids:
        lat, lon = spawn_in_zone(pick_zone())
        drivers.append(Driver(driver_id=did, lat=lat, lon=lon))

    stop = {"f": False}
    signal.signal(signal.SIGINT,  lambda *_: stop.update(f=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(f=True))

    while not stop["f"]:
        t0 = time.time()
        for driver in drivers:
            if driver.state == DriverState.IDLE:
                if random.random() < 0.30:
                    p, d = pick_zone(), pick_zone()
                    driver.pickup, driver.dropoff = spawn_in_zone(p), spawn_in_zone(d)
                    driver.pickup_zone, driver.dropoff_zone = p, d
                    driver.trip_id = f"TRIP-{uuid.uuid4().hex[:10].upper()}"
                    driver.state = DriverState.EN_ROUTE_PICKUP
                    TRIPS_STARTED.labels(zone=p).inc()

            elif driver.state == DriverState.EN_ROUTE_PICKUP:
                if move_toward(driver, driver.pickup, tick):
                    driver.trip_started_at = time.time()
                    driver.state = DriverState.ON_TRIP

            elif driver.state == DriverState.ON_TRIP:
                if move_toward(driver, driver.dropoff, tick):
                    duration_min = (time.time() - driver.trip_started_at) / 60.0
                    distance = haversine_miles(driver.pickup, driver.dropoff)
                    surge_prob = ZONES_BY_NAME[driver.pickup_zone]["surge_prob"]
                    surge = round(random.uniform(1.2, 2.5), 2) if random.random() < surge_prob else 1.0
                    fare = calculate_fare(distance, duration_min, surge)

                    trip = {
                        "trip_id":         driver.trip_id,
                        "driver_id":       driver.driver_id,
                        "pickup_zone":     driver.pickup_zone,
                        "dropoff_zone":    driver.dropoff_zone,
                        "pickup_lat":      driver.pickup[0],
                        "pickup_lon":      driver.pickup[1],
                        "dropoff_lat":     driver.dropoff[0],
                        "dropoff_lon":     driver.dropoff[1],
                        "distance_miles":  round(distance, 2),
                        "duration_minutes":round(duration_min, 2),
                        "passenger_count": random.randint(1, 4),
                        "payment_type":    random.choice(PAYMENT_TYPES),
                        "pickup_time":     time.strftime("%Y-%m-%dT%H:%M:%S"),
                        **fare,
                    }
                    trip = maybe_corrupt(trip)
                    emit(producer, TOPIC_TRIPS_RAW, driver.driver_id, trip)
                    TRIPS_COMPLETED.labels(zone=driver.dropoff_zone).inc()
                    driver.state = DriverState.IDLE
                    driver.trip_id = driver.pickup = driver.dropoff = None

            # GPS ping every tick
            ping = {
                "driver_id": driver.driver_id,
                "lat": round(driver.lat, 6),
                "lon": round(driver.lon, 6),
                "state": driver.state.value,
                "trip_id": driver.trip_id,
                "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
            }
            emit(producer, TOPIC_GPS_PINGS, driver.driver_id, ping)
            GPS_PINGS.inc()

        update_state_gauges(drivers)
        producer.poll(0)
        time.sleep(max(0, tick - (time.time() - t0)))

    log.info("flushing...")
    producer.flush(10)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--drivers", type=int, default=50)
    ap.add_argument("--rate",    type=float, default=1.0, help="seconds per tick")
    ap.add_argument("--metrics-port", type=int, default=8001)
    args = ap.parse_args()
    start_http_server(args.metrics_port)
    log.info("prometheus metrics on :%d", args.metrics_port)
    try:
        run(args.drivers, args.rate)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

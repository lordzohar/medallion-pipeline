"""
Surge Pricing Detector (Tumbling Windows)
==========================================
Reads `gps-pings` to track active drivers per zone,
reads `taxi-trips` to track demand per zone,
computes surge_multiplier per zone every 30s and publishes to `surge-events`.

This is essentially what Kafka Streams `.windowedBy(TimeWindows)` does,
implemented in pure Python so students can SEE the windowing logic.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import time
from collections import defaultdict, deque

from confluent_kafka import Consumer, Producer
from prometheus_client import Gauge, start_http_server

from config import (
    KAFKA_BOOTSTRAP,
    TOPIC_GPS_PINGS,
    TOPIC_SURGE_EVENTS,
    TOPIC_TRIPS_RAW,
    ZONES,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [surge] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

SURGE_GAUGE = Gauge("surge_multiplier", "Active surge multiplier", ["zone"])
DEMAND_GAUGE = Gauge("zone_demand_trips_per_min", "Trips/min in zone", ["zone"])
SUPPLY_GAUGE = Gauge("zone_supply_idle_drivers", "Idle drivers in zone", ["zone"])

WINDOW = 30  # seconds


def run(bootstrap: str):
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "surge-detector-v1",
        "auto.offset.reset": "latest",
    })
    producer = Producer({"bootstrap.servers": bootstrap, "client.id": "surge"})
    consumer.subscribe([TOPIC_GPS_PINGS, TOPIC_TRIPS_RAW])

    demand: dict[str, deque] = defaultdict(deque)   # zone -> deque[ts]
    supply: dict[str, set] = defaultdict(set)       # zone -> {driver_id}

    stop = {"f": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(f=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(f=True))

    last_emit = time.time()
    log.info("surge detector running. window=%ss", WINDOW)

    while not stop["f"]:
        msg = consumer.poll(1.0)
        if msg and not msg.error():
            try:
                rec = json.loads(msg.value())
                topic = msg.topic()
                if topic == TOPIC_TRIPS_RAW:
                    z = rec.get("pickup_zone")
                    if z:
                        demand[z].append(time.time())
                elif topic == TOPIC_GPS_PINGS:
                    z = nearest_zone(rec.get("lat"), rec.get("lon"))
                    if rec.get("state") == "IDLE":
                        supply[z].add(rec["driver_id"])
                    else:
                        supply[z].discard(rec["driver_id"])
            except Exception as e:
                log.warning("parse err: %s", e)

        # Window emit
        now = time.time()
        if now - last_emit >= WINDOW:
            for zone, dq in demand.items():
                while dq and now - dq[0] > 60:
                    dq.popleft()
                trips_per_min = len(dq)
                idle = len(supply.get(zone, set()))
                DEMAND_GAUGE.labels(zone=zone).set(trips_per_min)
                SUPPLY_GAUGE.labels(zone=zone).set(idle)
                multiplier = compute_surge(trips_per_min, idle)
                SURGE_GAUGE.labels(zone=zone).set(multiplier)

                evt = {
                    "zone": zone,
                    "window_ts": now,
                    "trips_per_min": trips_per_min,
                    "idle_drivers": idle,
                    "surge_multiplier": multiplier,
                }
                producer.produce(TOPIC_SURGE_EVENTS, key=zone.encode(), value=json.dumps(evt).encode())
                log.info("zone=%-13s demand=%2d supply=%2d surge=%.2fx", zone, trips_per_min, idle, multiplier)
            producer.poll(0)
            last_emit = now

    consumer.close()
    producer.flush(5)


def compute_surge(demand: int, supply: int) -> float:
    if supply == 0 and demand > 0:
        return 3.0
    ratio = demand / max(supply, 1)
    if ratio < 0.5:
        return 1.0
    if ratio < 1.0:
        return 1.2
    if ratio < 2.0:
        return 1.6
    if ratio < 3.0:
        return 2.0
    return 2.5


_CENTERS = [(z["name"], z["lat"], z["lon"]) for z in ZONES]


def nearest_zone(lat: float, lon: float) -> str:
    if lat is None or lon is None:
        return "UNKNOWN"
    best, best_d = "UNKNOWN", 1e9
    for name, la, lo in _CENTERS:
        d = (la - lat) ** 2 + (lo - lon) ** 2
        if d < best_d:
            best_d, best = d, name
    return best


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    p.add_argument("--metrics-port", type=int, default=8003)
    args = p.parse_args()
    start_http_server(args.metrics_port)
    run(args.bootstrap)


if __name__ == "__main__":
    main()

#!/usr/bin/env python
"""OGN ingestor — Open Glider Network APRS feed -> Kafka topic ogn.aircraft.positions.

Uses the python `ogn-client` library to connect to APRS-IS over TCP, parse
aircraft beacons and emit them as Avro to Kafka. Long-running; intended to
be a `docker compose` service.

Env:
  KAFKA_BOOTSTRAP            kafka:29092
  SCHEMA_REGISTRY_URL        http://schema-registry:8081
  OGN_TOPIC                  ogn.aircraft.positions
  OGN_APRS_USER              N0CALL (default; APRS-IS expects a callsign)
  OGN_APRS_FILTER            optional; e.g. 'r/48.0/9.0/200' (radius filter)
"""
from __future__ import annotations

import json
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from ogn.client import AprsClient
from ogn.parser import parse, ParseError

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
SR_URL          = os.environ["SCHEMA_REGISTRY_URL"]
TOPIC           = os.environ.get("OGN_TOPIC", "ogn.aircraft.positions")
APRS_USER       = os.environ.get("OGN_APRS_USER", "N0CALL")

# APRS-IS server-side filter. Priority:
#   1. explicit OGN_APRS_FILTER (e.g. 'r/40/-105/250' or 'm/300')
#   2. build a radius filter from CITY_LAT/CITY_LON/CITY_RADIUS_KM
#   3. empty -> global feed (heavy!)
_explicit = os.environ.get("OGN_APRS_FILTER", "").strip()
if _explicit:
    APRS_FILTER = _explicit
elif os.environ.get("CITY_LAT") and os.environ.get("CITY_LON"):
    APRS_FILTER = f"r/{os.environ['CITY_LAT']}/{os.environ['CITY_LON']}/{os.environ.get('CITY_RADIUS_KM', '250')}"
else:
    APRS_FILTER = ""

print(f"[ogn] APRS user={APRS_USER!r} filter={APRS_FILTER!r}", flush=True)

SCHEMA_FILE = Path(__file__).resolve().parent.parent / "schemas" / "ogn_position.avsc"

_running = True


def _stop(*_):
    global _running
    _running = False


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _to_ms(dt) -> int:
    if dt is None:
        return _now_ms()
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    try:
        return int(dt)
    except Exception:
        return _now_ms()


def _build_producer() -> SerializingProducer:
    sr = SchemaRegistryClient({"url": SR_URL})
    schema_str = SCHEMA_FILE.read_text()
    return SerializingProducer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "key.serializer":   StringSerializer("utf_8"),
        "value.serializer": AvroSerializer(sr, schema_str),
        "compression.type": "snappy",
        "linger.ms": 200,
        "enable.idempotence": True,
        "client.id": "ogn-ingestor",
    })


def _on_delivery(err, msg):
    if err:
        print(f"[delivery] {err}", file=sys.stderr)


def main() -> None:
    producer = _build_producer()
    print(f"[ogn] connecting as {APRS_USER!r}, filter={APRS_FILTER!r}, topic={TOPIC}")

    def handle(raw_message: str) -> None:
        if not _running:
            return
        try:
            beacon = parse(raw_message)
        except ParseError:
            return
        except Exception as e:
            print(f"[parse-err] {e}", file=sys.stderr)
            return
        if not beacon:
            return

        beacon_type = beacon.get("beacon_type") or beacon.get("aprs_type") or "unknown"
        # Keep only aircraft beacons in the primary topic; receivers etc. dropped.
        if beacon_type not in ("aprs_aircraft", "flarm", "ogn_tracker", "fanet"):
            return

        record = {
            "event_id":         str(uuid.uuid4()),
            "beacon_type":      str(beacon_type),
            "callsign":         beacon.get("name") or beacon.get("callsign"),
            "address":          beacon.get("address"),
            "address_type":     beacon.get("address_type"),
            "aircraft_type":    beacon.get("aircraft_type"),
            "ts_ms":            _to_ms(beacon.get("timestamp")),
            "ingested_ms":      _now_ms(),
            "lat":              beacon.get("latitude"),
            "lon":              beacon.get("longitude"),
            "altitude_m":       beacon.get("altitude"),
            "ground_speed_kmh": beacon.get("ground_speed"),
            "track_deg":        beacon.get("track"),
            "climb_rate_mps":   beacon.get("climb_rate"),
            "turn_rate_dps":    beacon.get("turn_rate"),
            "receiver_name":    beacon.get("receiver_name"),
            "raw":              raw_message[:300],
        }
        try:
            producer.produce(
                topic=TOPIC,
                key=record["address"] or record["callsign"] or record["event_id"],
                value=record,
                on_delivery=_on_delivery,
            )
            producer.poll(0)
        except BufferError:
            producer.flush(2)
        except Exception as e:
            print(f"[serialize-err] {e}", file=sys.stderr)

    client = AprsClient(aprs_user=APRS_USER, aprs_filter=APRS_FILTER or None)
    while _running:
        try:
            client.connect()
            print("[ogn] APRS connected")
            client.run(callback=handle, autoreconnect=True)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"[ogn] client error: {e}; reconnecting in 5s")
            time.sleep(5)
        finally:
            try: client.disconnect()
            except Exception: pass

    producer.flush(10)
    print("[ogn] stopped cleanly")


if __name__ == "__main__":
    main()

#!/usr/bin/env python
"""Seismic Portal ingestor — wss://www.seismicportal.eu real-time websocket -> Kafka.

The EMSC websocket pushes a JSON envelope like:
    {"action": "create"|"update", "data": { ... GeoJSON Feature ... }}

We unwrap, flatten and emit each event to topic `seismic.events`.
Long-running service. Auto-reconnects with exponential backoff.
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

import websockets
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
SR_URL          = os.environ["SCHEMA_REGISTRY_URL"]
TOPIC           = os.environ.get("SEISMIC_TOPIC", "seismic.events")
WS_URL          = os.environ.get("SEISMIC_WS_URL", "wss://www.seismicportal.eu/standing_order/websocket")

SCHEMA_FILE = Path(__file__).resolve().parent.parent / "schemas" / "seismic_event.avsc"

_running = True
def _stop(*_):
    global _running; _running = False
signal.signal(signal.SIGINT,  _stop)
signal.signal(signal.SIGTERM, _stop)


def _now_ms() -> int: return int(time.time() * 1000)

def _iso_ms(s):
    if not s: return None
    try:
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return None


def _producer() -> SerializingProducer:
    sr = SchemaRegistryClient({"url": SR_URL})
    schema_str = SCHEMA_FILE.read_text()
    return SerializingProducer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "key.serializer":   StringSerializer("utf_8"),
        "value.serializer": AvroSerializer(sr, schema_str),
        "compression.type": "snappy",
        "linger.ms": 100,
        "enable.idempotence": True,
        "client.id": "seismic-ingestor",
    })


def _delivery(err, msg):
    if err: print(f"[delivery] {err}", file=sys.stderr)


def _to_record(envelope: dict) -> dict | None:
    action = envelope.get("action") or "create"
    data = envelope.get("data") or {}
    props = data.get("properties") or {}
    geom  = data.get("geometry") or {}
    coords = geom.get("coordinates") or [None, None, None]
    lon = coords[0] if len(coords) > 0 else None
    lat = coords[1] if len(coords) > 1 else None
    depth = coords[2] if len(coords) > 2 else None  # km

    event_id = data.get("id") or props.get("unid") or str(uuid.uuid4())
    mag = props.get("mag")
    if mag is None or lat is None or lon is None:
        return None

    return {
        "event_id":       str(event_id),
        "action":         str(action),
        "ts_ms":          _iso_ms(props.get("time")) or _now_ms(),
        "ingested_ms":    _now_ms(),
        "lat":            float(lat),
        "lon":            float(lon),
        "depth_km":       float(depth) if depth is not None else None,
        "magnitude":      float(mag),
        "magnitude_type": props.get("magtype"),
        "region":         props.get("flynn_region"),
        "source_id":      props.get("source_id"),
        "source_catalog": props.get("source_catalog"),
        "auth":           props.get("auth"),
        "evtype":         props.get("evtype"),
        "lastupdate_ms":  _iso_ms(props.get("lastupdate")),
    }


async def stream(producer: SerializingProducer) -> None:
    backoff = 2
    while _running:
        try:
            print(f"[seismic] connecting {WS_URL}")
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                print("[seismic] connected")
                backoff = 2
                async for raw in ws:
                    if not _running: break
                    try:
                        envelope = json.loads(raw)
                    except Exception:
                        continue
                    rec = _to_record(envelope)
                    if not rec:
                        continue
                    try:
                        producer.produce(TOPIC, key=rec["event_id"], value=rec, on_delivery=_delivery)
                        producer.poll(0)
                    except BufferError:
                        producer.flush(2)
                    except Exception as e:
                        print(f"[serialize-err] {e}", file=sys.stderr)
        except (websockets.ConnectionClosed, OSError) as e:
            print(f"[seismic] connection lost: {e}")
        except Exception as e:
            print(f"[seismic] error: {type(e).__name__} {e}")
        if not _running: break
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)


def main() -> None:
    producer = _producer()
    try:
        asyncio.run(stream(producer))
    finally:
        producer.flush(10)
        print("[seismic] stopped cleanly")


if __name__ == "__main__":
    main()

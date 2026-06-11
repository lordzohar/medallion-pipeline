#!/usr/bin/env python
"""NOAA ingestor — api.weather.gov polling -> Kafka.

  - Observations: polls `/stations/{id}/observations/latest` for each station
    in NOAA_STATIONS at POLL_INTERVAL_SEC. Topic: noaa.observations.
  - Alerts: polls `/alerts/active` and emits new alerts. Topic: noaa.alerts.

The endpoint isn't a push stream, but observations refresh every ~15 min and
alerts every ~minute, so polling at 60 s captures everything new in near-real-
time. We dedupe by station+ts (obs) and alert_id (alerts).
"""
from __future__ import annotations

import os
import signal
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

import requests
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
SR_URL          = os.environ["SCHEMA_REGISTRY_URL"]
USER_AGENT      = os.environ.get("NOAA_USER_AGENT", "(day10-capstone, ops@example.org)")

OBS_TOPIC       = os.environ.get("NOAA_OBS_TOPIC",   "noaa.observations")
ALERT_TOPIC     = os.environ.get("NOAA_ALERT_TOPIC", "noaa.alerts")
POLL_SEC        = int(os.environ.get("NOAA_POLL_SEC", "60"))
STATIONS        = [s.strip() for s in os.environ.get(
    "NOAA_STATIONS",
    "KJFK,KLAX,KSFO,KORD,KBOS,KDEN,KATL,KMIA,KSEA,KIAD"
).split(",") if s.strip()]
ALERT_AREA      = os.environ.get("NOAA_ALERT_AREA", "")   # blank = whole US; or e.g. "NY,CA"

SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "schemas"

_running = True
def _stop(*_):
    global _running; _running = False
signal.signal(signal.SIGINT,  _stop)
signal.signal(signal.SIGTERM, _stop)

def _now_ms() -> int: return int(time.time() * 1000)

def _iso_to_ms(s: str | None) -> int | None:
    if not s: return None
    try:
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return None


def _producer(schema_file: str, client_id: str) -> SerializingProducer:
    sr = SchemaRegistryClient({"url": SR_URL})
    schema_str = (SCHEMAS_DIR / schema_file).read_text()
    return SerializingProducer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "key.serializer":   StringSerializer("utf_8"),
        "value.serializer": AvroSerializer(sr, schema_str),
        "compression.type": "snappy",
        "linger.ms": 200,
        "enable.idempotence": True,
        "client.id": client_id,
    })


def _delivery(err, msg):
    if err: print(f"[delivery] {err}", file=sys.stderr)


# ---------- observations ----------

def _convert_pressure(p):
    """NOAA gives pressure in Pa already in unitCode 'wmoUnit:Pa'."""
    return p

def _convert_speed_kmh(qv):
    """NOAA returns wind speed in km_h-1 (km/h). Pass through."""
    return qv

def fetch_observation(session: requests.Session, station: str) -> dict | None:
    url = f"https://api.weather.gov/stations/{station}/observations/latest"
    r = session.get(url, timeout=10)
    if r.status_code == 404:
        print(f"[noaa-obs] {station} 404"); return None
    if not r.ok:
        print(f"[noaa-obs] {station} {r.status_code}"); return None
    feat = r.json()
    props = feat.get("properties") or {}
    geom  = feat.get("geometry") or {}
    coords = geom.get("coordinates") or [None, None, None]
    def val(k):
        node = props.get(k) or {}
        return node.get("value") if isinstance(node, dict) else node

    return {
        "event_id":         str(uuid.uuid4()),
        "station_id":       station,
        "ts_ms":            _iso_to_ms(props.get("timestamp")) or _now_ms(),
        "ingested_ms":      _now_ms(),
        "lat":              coords[1] if len(coords) > 1 else None,
        "lon":              coords[0] if coords else None,
        "elevation_m":      val("elevation"),
        "temperature_c":    val("temperature"),
        "dewpoint_c":       val("dewpoint"),
        "humidity_pct":     val("relativeHumidity"),
        "wind_speed_kmh":   _convert_speed_kmh(val("windSpeed")),
        "wind_dir_deg":     val("windDirection"),
        "wind_gust_kmh":    _convert_speed_kmh(val("windGust")),
        "pressure_pa":      _convert_pressure(val("barometricPressure")),
        "visibility_m":     val("visibility"),
        "precip_last_1h_mm": val("precipitationLastHour"),
        "text_description": props.get("textDescription"),
    }


# ---------- alerts ----------

def fetch_alerts(session: requests.Session, seen: set[str]) -> list[dict]:
    url = "https://api.weather.gov/alerts/active"
    params = {"area": ALERT_AREA} if ALERT_AREA else None
    r = session.get(url, params=params, timeout=15)
    if not r.ok:
        print(f"[noaa-alerts] {r.status_code}"); return []
    out: list[dict] = []
    for f in r.json().get("features") or []:
        p = f.get("properties") or {}
        alert_id = p.get("id") or f.get("id")
        if not alert_id or alert_id in seen:
            continue
        seen.add(alert_id)
        geo = (p.get("geocode") or {})
        states = ",".join(sorted({c[:2] for c in (geo.get("SAME") or []) if isinstance(c, str)}))
        out.append({
            "event_id":     str(uuid.uuid4()),
            "alert_id":     alert_id,
            "ts_ms":        _iso_to_ms(p.get("sent")) or _now_ms(),
            "ingested_ms":  _now_ms(),
            "effective_ms": _iso_to_ms(p.get("effective")),
            "expires_ms":   _iso_to_ms(p.get("expires")),
            "event":        p.get("event")        or "Unknown",
            "severity":     p.get("severity")     or "Unknown",
            "certainty":    p.get("certainty")    or "Unknown",
            "urgency":      p.get("urgency")      or "Unknown",
            "status":       p.get("status")       or "Unknown",
            "message_type": p.get("messageType")  or "Unknown",
            "category":     p.get("category")     or "Unknown",
            "headline":     p.get("headline"),
            "area_desc":    p.get("areaDesc"),
            "sender":       p.get("senderName"),
            "states":       states or None,
        })
    return out


# ---------- main loop ----------

def main() -> None:
    p_obs   = _producer("noaa_observation.avsc", "noaa-obs")
    p_alert = _producer("noaa_alert.avsc",       "noaa-alerts")
    seen_alerts: set[str] = set()

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/geo+json"})

    print(f"[noaa] stations={STATIONS} poll={POLL_SEC}s obs={OBS_TOPIC} alerts={ALERT_TOPIC}")
    while _running:
        # observations
        for st in STATIONS:
            if not _running: break
            try:
                rec = fetch_observation(session, st)
                if rec:
                    p_obs.produce(OBS_TOPIC, key=st, value=rec, on_delivery=_delivery)
                    p_obs.poll(0)
            except Exception as e:
                print(f"[noaa-obs] {st}: {type(e).__name__} {e}")
        # alerts
        try:
            for rec in fetch_alerts(session, seen_alerts):
                p_alert.produce(ALERT_TOPIC, key=rec["alert_id"], value=rec, on_delivery=_delivery)
            p_alert.poll(0)
        except Exception as e:
            print(f"[noaa-alerts] {type(e).__name__} {e}")
        # cap seen set so it doesn't grow forever
        if len(seen_alerts) > 5000:
            seen_alerts.clear()

        for _ in range(POLL_SEC):
            if not _running: break
            time.sleep(1)

    for p in (p_obs, p_alert):
        p.flush(10)
    print("[noaa] stopped cleanly")


if __name__ == "__main__":
    main()

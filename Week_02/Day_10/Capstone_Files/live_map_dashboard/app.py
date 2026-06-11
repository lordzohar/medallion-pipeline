"""
Live Map Dashboard
==================

Glidernet-style real-time map that overlays:
  * Aircraft positions from `ogn.aircraft.positions` (with rotated plane
    icon + trail of the last N positions)
  * NOAA weather observations from `noaa.observations`
    (temperature + wind on hover)
  * EMSC seismic events from `seismic.events` (pulsing circle marker
    sized + coloured by magnitude)

Open:  http://localhost:5003

Three Kafka consumer threads keep an in-memory state. Browsers connect via
Socket.IO and receive:
  - 'snapshot' on connect (full current state)
  - 'aircraft', 'weather', 'seismic' for each incoming event

State is naturally bounded:
  - aircraft: keep last `MAX_TRAIL` positions per callsign; expire after 15 min
  - weather:  one entry per station, replaced on each obs
  - seismic:  last 24h, capped at `MAX_QUAKES` (newest wins)
"""
from __future__ import annotations

import io
import json
import logging
import os
import struct
import threading
import time
from collections import OrderedDict, deque
from datetime import datetime, timezone

import fastavro
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from flask import Flask, jsonify, render_template
from flask_socketio import SocketIO
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

logging.basicConfig(level=logging.INFO, format="%(asctime)s [livemap] %(message)s")
log = logging.getLogger(__name__)

BOOTSTRAP   = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
SR_URL      = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
GROUP_ID    = os.environ.get("LIVEMAP_GROUP_ID", "livemap-dashboard")

OGN_TOPIC     = os.environ.get("OGN_TOPIC", "ogn.aircraft.positions")
NOAA_TOPIC    = os.environ.get("NOAA_OBS_TOPIC", "noaa.observations")
SEISMIC_TOPIC = os.environ.get("SEISMIC_TOPIC", "seismic.events")

MAX_TRAIL        = int(os.environ.get("LIVEMAP_MAX_TRAIL", "40"))
MAX_AIRCRAFT     = int(os.environ.get("LIVEMAP_MAX_AIRCRAFT", "100"))
MAX_QUAKES       = int(os.environ.get("LIVEMAP_MAX_QUAKES", "300"))
AIRCRAFT_TTL_SEC = int(os.environ.get("LIVEMAP_AIRCRAFT_TTL_SEC", "600"))   # 10 min
QUAKE_TTL_SEC    = int(os.environ.get("LIVEMAP_QUAKE_TTL_SEC", "86400"))    # 24 h

CITY = {
    "name":      os.environ.get("CITY_NAME", "Denver"),
    "lat":       float(os.environ.get("CITY_LAT", "39.74")),
    "lon":       float(os.environ.get("CITY_LON", "-104.99")),
    "radius_km": float(os.environ.get("CITY_RADIUS_KM", "250")),
}

app = Flask(__name__)
app.config["SECRET_KEY"] = "livemap-dev"
sio = SocketIO(app, cors_allowed_origins="*", async_mode="threading", logger=False, engineio_logger=False)

# --- in-memory state -------------------------------------------------------

_state_lock = threading.Lock()
_aircraft: "OrderedDict[str, dict]" = OrderedDict()   # key -> {callsign, lat, lon, ts_ms, ..., trail: deque}
_weather: dict[str, dict] = {}                         # station_id -> latest obs
_quakes: "deque[dict]" = deque(maxlen=MAX_QUAKES)      # newest at right

# --- prom metrics ----------------------------------------------------------

M_MSGS  = Counter("livemap_messages_total", "Kafka messages consumed.", labelnames=("topic",))
M_DROP  = Counter("livemap_dropped_total",  "Messages dropped (parse error / missing geo).", labelnames=("topic",))
M_AIRC  = Gauge("livemap_aircraft_active", "Aircraft currently tracked.")
M_STAT  = Gauge("livemap_stations_active", "Weather stations with a fresh obs.")
M_QUAKE = Gauge("livemap_quakes_24h",      "Seismic events kept in memory.")

# --- Confluent Avro deserialization (manual; avoids serializer churn) ------

_sr = SchemaRegistryClient({"url": SR_URL})
_schema_cache: dict[int, dict] = {}


def _decode_avro(buf: bytes) -> dict | None:
    """Strip the 5-byte Confluent framing then parse the Avro body."""
    if not buf or len(buf) < 5 or buf[0] != 0:
        return None
    schema_id = struct.unpack(">I", buf[1:5])[0]
    schema = _schema_cache.get(schema_id)
    if schema is None:
        try:
            raw = _sr.get_schema(schema_id).schema_str
            schema = json.loads(raw)
            _schema_cache[schema_id] = schema
        except Exception as exc:
            log.warning("failed to fetch schema %s: %s", schema_id, exc)
            return None
    try:
        return fastavro.schemaless_reader(io.BytesIO(buf[5:]), schema)
    except Exception as exc:
        log.warning("avro decode error sid=%s: %s", schema_id, exc)
        return None


# --- consumer loops --------------------------------------------------------

def _make_consumer(name: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"{GROUP_ID}-{name}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "session.timeout.ms": 30000,
    })


def _consume_loop(topic: str, handler):
    while True:
        try:
            c = _make_consumer(topic)
            c.subscribe([topic])
            log.info("subscribed to %s", topic)
            while True:
                msg = c.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    log.warning("%s consumer error: %s", topic, msg.error())
                    continue
                rec = _decode_avro(msg.value())
                if rec is None:
                    M_DROP.labels(topic=topic).inc()
                    continue
                M_MSGS.labels(topic=topic).inc()
                try:
                    handler(rec)
                except Exception as exc:
                    log.exception("handler error on %s: %s", topic, exc)
        except KafkaException as exc:
            log.warning("kafka loop crash on %s: %s — reconnecting in 5s", topic, exc)
            time.sleep(5)
        except Exception as exc:
            log.exception("loop crash on %s: %s — reconnecting in 5s", topic, exc)
            time.sleep(5)


# --- per-topic handlers ----------------------------------------------------

def _ac_key(rec: dict) -> str | None:
    return rec.get("callsign") or rec.get("address") or rec.get("event_id")


def _handle_aircraft(rec: dict):
    lat, lon = rec.get("lat"), rec.get("lon")
    if lat is None or lon is None:
        return
    key = _ac_key(rec)
    if not key:
        return
    point = {
        "lat": lat, "lon": lon,
        "alt": rec.get("altitude_m"),
        "spd": rec.get("ground_speed_kmh"),
        "trk": rec.get("track_deg"),
        "clr": rec.get("climb_rate_mps"),
        "ts":  rec.get("ts_ms"),
    }
    with _state_lock:
        cur = _aircraft.get(key)
        if cur is None:
            cur = {
                "key": key,
                "callsign": rec.get("callsign"),
                "address": rec.get("address"),
                "aircraft_type": rec.get("aircraft_type"),
                "receiver": rec.get("receiver_name"),
                "trail": deque(maxlen=MAX_TRAIL),
            }
            _aircraft[key] = cur
        cur.update({
            "lat": lat, "lon": lon,
            "alt": point["alt"], "spd": point["spd"], "trk": point["trk"],
            "clr": point["clr"], "ts": point["ts"],
        })
        cur["trail"].append({"lat": lat, "lon": lon, "ts": point["ts"]})
        _aircraft.move_to_end(key)
        # LRU evict: drop oldest-seen aircraft once we hit the cap.
        while len(_aircraft) > MAX_AIRCRAFT:
            evict_key, _ = _aircraft.popitem(last=False)
            sio.emit("aircraft_gone", {"key": evict_key})
        M_AIRC.set(len(_aircraft))
    sio.emit("aircraft", _ac_payload(cur))


def _ac_payload(cur: dict) -> dict:
    return {
        "key": cur["key"],
        "callsign": cur.get("callsign") or cur["key"],
        "aircraft_type": cur.get("aircraft_type"),
        "lat": cur["lat"], "lon": cur["lon"],
        "alt": cur.get("alt"), "spd": cur.get("spd"),
        "trk": cur.get("trk"), "clr": cur.get("clr"),
        "ts":  cur.get("ts"),
        "receiver": cur.get("receiver"),
        "trail": list(cur.get("trail", [])),
    }


def _handle_weather(rec: dict):
    lat, lon = rec.get("lat"), rec.get("lon")
    sid = rec.get("station_id")
    if not sid or lat is None or lon is None:
        return
    payload = {
        "station_id": sid,
        "lat": lat, "lon": lon,
        "temp_c": rec.get("temperature_c"),
        "wind_kmh": rec.get("wind_speed_kmh"),
        "wind_dir": rec.get("wind_dir_deg"),
        "gust_kmh": rec.get("wind_gust_kmh"),
        "humidity_pct": rec.get("humidity_pct"),
        "pressure_pa": rec.get("pressure_pa"),
        "visibility_m": rec.get("visibility_m"),
        "description": rec.get("text_description"),
        "ts": rec.get("ts_ms"),
    }
    with _state_lock:
        _weather[sid] = payload
        M_STAT.set(len(_weather))
    sio.emit("weather", payload)


def _handle_seismic(rec: dict):
    lat, lon = rec.get("lat"), rec.get("lon")
    if lat is None or lon is None:
        return
    payload = {
        "event_id": rec.get("event_id"),
        "lat": lat, "lon": lon,
        "depth_km": rec.get("depth_km"),
        "magnitude": rec.get("magnitude"),
        "magnitude_type": rec.get("magnitude_type"),
        "region": rec.get("region"),
        "ts": rec.get("ts_ms"),
        "action": rec.get("action"),
    }
    with _state_lock:
        # On update, replace existing event with same id.
        for i, q in enumerate(_quakes):
            if q.get("event_id") == payload["event_id"]:
                _quakes[i] = payload
                break
        else:
            _quakes.append(payload)
        M_QUAKE.set(len(_quakes))
    sio.emit("seismic", payload)


# --- janitor: TTL eviction --------------------------------------------------

def _janitor():
    while True:
        time.sleep(60)
        now_ms = int(time.time() * 1000)
        with _state_lock:
            stale = [k for k, c in _aircraft.items()
                     if (c.get("ts") or 0) < now_ms - AIRCRAFT_TTL_SEC * 1000]
            for k in stale:
                _aircraft.pop(k, None)
                sio.emit("aircraft_gone", {"key": k})
            cutoff = now_ms - QUAKE_TTL_SEC * 1000
            while _quakes and (_quakes[0].get("ts") or 0) < cutoff:
                _quakes.popleft()
            M_AIRC.set(len(_aircraft))
            M_QUAKE.set(len(_quakes))


# --- HTTP / Socket.IO -------------------------------------------------------

@app.route("/")
def index():
    return render_template("map.html")


@app.route("/health")
def health():
    return {"status": "ok"}, 200


@app.route("/metrics")
def metrics():
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}


@app.route("/api/snapshot")
def api_snapshot():
    return jsonify(_snapshot())


@app.route("/api/config")
def api_config():
    return jsonify({"city": CITY})


def _snapshot() -> dict:
    with _state_lock:
        return {
            "aircraft": [_ac_payload(c) for c in _aircraft.values()],
            "weather":  list(_weather.values()),
            "seismic":  list(_quakes),
            "city":     CITY,
            "now_ms":   int(time.time() * 1000),
        }


@sio.on("connect")
def on_connect():
    sio.emit("snapshot", _snapshot())


# --- bootstrap --------------------------------------------------------------

def _start_consumers():
    threading.Thread(target=_consume_loop, args=(OGN_TOPIC,     _handle_aircraft), daemon=True, name="kc-ogn").start()
    threading.Thread(target=_consume_loop, args=(NOAA_TOPIC,    _handle_weather),  daemon=True, name="kc-noaa").start()
    threading.Thread(target=_consume_loop, args=(SEISMIC_TOPIC, _handle_seismic),  daemon=True, name="kc-seismic").start()
    threading.Thread(target=_janitor, daemon=True, name="janitor").start()


_start_consumers()


if __name__ == "__main__":
    sio.run(app, host="0.0.0.0", port=5003, allow_unsafe_werkzeug=True)

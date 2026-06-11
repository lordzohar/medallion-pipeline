"""Bronze -> Silver transforms (Python reference for Hop).

Reads recent Avro from bronze/ for each stream and CDC entity, unwraps,
dedupes, late-filters, then writes silver Avro under silver/<entity>/...

Run from CLI:
    python bronze_to_silver.py --all
    python bronze_to_silver.py --stream ogn_positions
    python bronze_to_silver.py --cdc regions
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone

from lib_minio import (BRONZE, SILVER, ensure_bucket, read_avro,
                       recent_keys, write_avro)


# topic in bronze/ -> (silver_entity, pk, ts_field)
STREAMS: dict[str, tuple[str, str, str]] = {
    "ogn.aircraft.positions": ("ogn_positions",     "event_id", "ts_ms"),
    "noaa.observations":      ("noaa_observations", "event_id", "ts_ms"),
    "noaa.alerts":            ("noaa_alerts",       "alert_id", "ts_ms"),
    "seismic.events":         ("seismic_events",    "event_id", "ts_ms"),
}

# config table topic -> (silver_entity, pk)
CDC_TABLES: dict[str, tuple[str, str]] = {
    "config.public.regions":              ("regions",              "region_id"),
    "config.public.alert_thresholds":     ("alert_thresholds",     "threshold_id"),
    "config.public.subscriber_watchlist": ("subscriber_watchlist", "watchlist_id"),
}

LATE_THRESHOLD_MS = 24 * 60 * 60 * 1000  # 24h


# ---------- helpers ----------

def _silver_key(entity: str, when: datetime, payload_hash: str) -> str:
    return (f"{entity}/year={when.strftime('%Y')}/month={when.strftime('%m')}/"
            f"day={when.strftime('%d')}/part-{payload_hash}.avro")


def _hash(records: list[dict]) -> str:
    h = hashlib.sha256()
    for r in records:
        h.update(json.dumps(r, default=str, sort_keys=True).encode())
    return h.hexdigest()[:12]


def _flat_schema(sample: dict, name: str) -> dict:
    def avro_for(v):
        if isinstance(v, bool):  return "boolean"
        if isinstance(v, int):   return "long"
        if isinstance(v, float): return "double"
        return "string"
    fields = []
    for k, v in sample.items():
        t = avro_for(v) if v is not None else "string"
        fields.append({"name": k, "type": ["null", t], "default": None})
    return {"type": "record", "name": name, "namespace": "org.pipeline.silver", "fields": fields}


def _dedupe(records: list[dict], pk: str, ts: str = "ts_ms") -> list[dict]:
    keep: dict[str, dict] = {}
    for r in records:
        k = r.get(pk)
        if k is None:
            continue
        prev = keep.get(k)
        if prev is None or (r.get(ts) or 0) >= (prev.get(ts) or 0):
            keep[k] = r
    return list(keep.values())


def _late_filter(records: list[dict], ts: str = "ts_ms") -> tuple[list[dict], int]:
    if not records:
        return [], 0
    max_ts = max((r.get(ts) or 0) for r in records)
    cutoff = max_ts - LATE_THRESHOLD_MS
    kept = [r for r in records if (r.get(ts) or 0) >= cutoff]
    return kept, len(records) - len(kept)


def _emit_quality(entity: str, rows_in: int, rows_out: int,
                  late_dropped: int, dedup_dropped: int) -> None:
    payload = {
        "entity": entity, "ran_at": datetime.now(timezone.utc).isoformat(),
        "rows_in": rows_in, "rows_out": rows_out,
        "late_dropped": late_dropped, "dedup_dropped": dedup_dropped,
    }
    schema = {
        "type": "record", "name": "QualityRecord",
        "namespace": "org.pipeline.quality",
        "fields": [
            {"name": "entity",       "type": "string"},
            {"name": "ran_at",       "type": "string"},
            {"name": "rows_in",      "type": "long"},
            {"name": "rows_out",     "type": "long"},
            {"name": "late_dropped", "type": "long"},
            {"name": "dedup_dropped","type": "long"},
        ],
    }
    ts = datetime.now(timezone.utc)
    key = f"_quality/{entity}/{ts.strftime('%Y-%m-%dT%H-%M-%S')}.avro"
    write_avro(SILVER, key, schema, [payload])


def _read_bronze(prefix: str, hours: int = 2) -> list[dict]:
    rows: list[dict] = []
    for k in recent_keys(BRONZE, prefix, hours=hours):
        try:
            recs, _ = read_avro(BRONZE, k)
            rows.extend(recs)
        except Exception as e:
            print(f"[warn] {k}: {e}")
    return rows


# ---------- core transforms ----------

def stream(topic: str) -> None:
    entity, pk, ts_field = STREAMS[topic]
    prefix = f"{topic}/"
    rows = _read_bronze(prefix)
    print(f"[stream:{entity}] {len(rows)} bronze rows")
    rows_in = len(rows)
    if rows_in == 0:
        _emit_quality(entity, 0, 0, 0, 0); return

    deduped = _dedupe(rows, pk, ts_field)
    deduped, late_dropped = _late_filter(deduped, ts_field)
    if not deduped:
        _emit_quality(entity, rows_in, 0, late_dropped, 0); return

    schema = _flat_schema(deduped[0], f"{entity}_silver")
    when = datetime.now(timezone.utc)
    key = _silver_key(entity, when, _hash(deduped))
    n = write_avro(SILVER, key, schema, deduped)
    print(f"[stream:{entity}] wrote {n} -> s3://{SILVER}/{key}")
    _emit_quality(entity, rows_in, n, late_dropped, rows_in - len(deduped) - late_dropped)


def cdc(topic: str) -> None:
    entity, pk = CDC_TABLES[topic]
    prefix = f"{topic}/"
    rows = _read_bronze(prefix)
    print(f"[cdc:{entity}] {len(rows)} bronze rows")
    rows_in = len(rows)
    if rows_in == 0:
        _emit_quality(entity, 0, 0, 0, 0); return

    # Debezium ExtractNewRecordState adds __ts_ms; normalize.
    for r in rows:
        if "ts_ms" not in r and "__ts_ms" in r:
            r["ts_ms"] = r.pop("__ts_ms")

    deduped = _dedupe(rows, pk, "ts_ms")
    deduped, late_dropped = _late_filter(deduped, "ts_ms")
    if not deduped:
        _emit_quality(entity, rows_in, 0, late_dropped, 0); return

    schema = _flat_schema(deduped[0], f"{entity}_silver")
    when = datetime.now(timezone.utc)
    key = _silver_key(entity, when, _hash(deduped))
    n = write_avro(SILVER, key, schema, deduped)
    print(f"[cdc:{entity}] wrote {n} -> s3://{SILVER}/{key}")
    _emit_quality(entity, rows_in, n, late_dropped, rows_in - len(deduped) - late_dropped)


# ---------- entry point ----------

ENTITY_TO_STREAM = {v[0]: k for k, v in STREAMS.items()}
ENTITY_TO_CDC    = {v[0]: k for k, v in CDC_TABLES.items()}


def run_all() -> None:
    ensure_bucket(SILVER)
    for t in STREAMS:
        try: stream(t)
        except Exception as e: print(f"[err] stream {t}: {type(e).__name__}: {e}")
    for t in CDC_TABLES:
        try: cdc(t)
        except Exception as e: print(f"[err] cdc {t}: {type(e).__name__}: {e}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--stream", action="append", default=[],
                    help="silver entity name (ogn_positions | noaa_observations | noaa_alerts | seismic_events)")
    ap.add_argument("--cdc",    action="append", default=[],
                    help="silver entity name (regions | alert_thresholds | subscriber_watchlist)")
    args = ap.parse_args()

    ensure_bucket(SILVER)
    if args.all:
        run_all(); return
    for name in args.stream:
        if name in ENTITY_TO_STREAM:    stream(ENTITY_TO_STREAM[name])
        else: print(f"[skip] unknown stream entity: {name}")
    for name in args.cdc:
        if name in ENTITY_TO_CDC:       cdc(ENTITY_TO_CDC[name])
        else: print(f"[skip] unknown cdc entity: {name}")


if __name__ == "__main__":
    main()

"""Silver -> Gold transforms (Python reference for Hop).

Reads silver Avro via DuckDB (avro extension), with a fastavro fallback.
Writes Parquet snapshots to gold/<mart>/snapshot=<utc>/part-0.parquet and
overwrites gold/<mart>/latest.parquet for the business dashboard.

Marts:
  aircraft_density_by_region : OGN positions joined to regions bbox (last 1h)
  weather_snapshot           : latest NOAA observation per station + region
  seismic_24h_summary        : EMSC events by magnitude bucket + region (last 24h)
  region_alert_correlation   : cross-stream — quakes >= threshold near subscribed regions
"""
from __future__ import annotations

import argparse
import io
from datetime import datetime, timezone

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from lib_minio import GOLD, SILVER, ensure_bucket, s3 as _s3


# ---------- DuckDB / fastavro plumbing ----------

def _duck() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    try:
        con.execute("INSTALL avro; LOAD avro;")
    except Exception:
        pass
    con.execute("SET s3_endpoint='minio:9000';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    con.execute("SET s3_access_key_id='minioadmin';")
    con.execute("SET s3_secret_access_key='minioadmin';")
    con.execute("SET s3_region='us-east-1';")
    return con


def _read_via_fastavro(prefix: str) -> pa.Table | None:
    import fastavro
    rows: list[dict] = []
    client = _s3()
    token = None
    while True:
        kw = {"Bucket": SILVER, "Prefix": prefix, "MaxKeys": 1000}
        if token: kw["ContinuationToken"] = token
        resp = client.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            if o["Key"].startswith(f"{prefix}_quality"):
                continue
            try:
                body = client.get_object(Bucket=SILVER, Key=o["Key"])["Body"].read()
                rows.extend(list(fastavro.reader(io.BytesIO(body))))
            except Exception:
                continue
        if not resp.get("IsTruncated"): break
        token = resp.get("NextContinuationToken")
    return pa.Table.from_pylist(rows) if rows else None


def _scan(con: duckdb.DuckDBPyConnection, alias: str, prefix: str) -> bool:
    try:
        con.execute(
            f"CREATE OR REPLACE TEMP VIEW {alias} AS "
            f"SELECT * FROM read_avro('s3://{SILVER}/{prefix}**/*.avro')"
        )
        con.execute(f"SELECT count(*) FROM {alias}").fetchone()
        return True
    except Exception as e:
        print(f"[fallback fastavro] {alias}: {e}")
        tbl = _read_via_fastavro(prefix)
        if tbl is None: return False
        con.register(alias, tbl)
        return True


def _write_gold(mart: str, table: pa.Table) -> None:
    snap = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snap_key   = f"{mart}/snapshot={snap}/part-0.parquet"
    latest_key = f"{mart}/latest.parquet"
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    body = buf.getvalue()
    cli = _s3()
    cli.put_object(Bucket=GOLD, Key=snap_key,   Body=body)
    cli.put_object(Bucket=GOLD, Key=latest_key, Body=body)
    print(f"[gold] {table.num_rows} rows -> s3://{GOLD}/{snap_key} (+latest)")


# ---------- marts ----------

def aircraft_density_by_region() -> None:
    con = _duck()
    if not _scan(con, "ogn",     "ogn_positions/"): return
    if not _scan(con, "regions", "regions/"):       return
    sql = """
    WITH recent AS (
        SELECT * FROM ogn
         WHERE ts_ms >= (epoch_ms(now()) - 60*60*1000)
           AND lat IS NOT NULL AND lon IS NOT NULL
    )
    SELECT
        r.name                                                   AS region,
        r.country                                                AS country,
        r.kind                                                   AS kind,
        COUNT(*)                                                 AS positions_last_1h,
        COUNT(DISTINCT COALESCE(p.address, p.callsign))          AS unique_aircraft,
        AVG(p.altitude_m)                                        AS avg_altitude_m,
        AVG(p.ground_speed_kmh)                                  AS avg_speed_kmh,
        MAX(p.altitude_m)                                        AS max_altitude_m,
        MAX(p.ts_ms)                                             AS last_seen_ms
    FROM regions r
    JOIN recent p
      ON p.lat BETWEEN r.min_lat AND r.max_lat
     AND p.lon BETWEEN r.min_lon AND r.max_lon
    WHERE r.is_active = true
    GROUP BY r.name, r.country, r.kind
    HAVING COUNT(*) >= 1
    ORDER BY positions_last_1h DESC
    LIMIT 200
    """
    _write_gold("aircraft_density_by_region", con.execute(sql).fetch_arrow_table())


def weather_snapshot() -> None:
    con = _duck()
    if not _scan(con, "obs",     "noaa_observations/"): return
    if not _scan(con, "regions", "regions/"):            return
    sql = """
    WITH latest_per_station AS (
        SELECT station_id, max(ts_ms) AS ts_ms
          FROM obs
         GROUP BY station_id
    )
    SELECT
        o.station_id,
        r.name                AS region,
        r.country             AS country,
        o.ts_ms,
        o.lat, o.lon,
        o.temperature_c,
        o.humidity_pct,
        o.wind_speed_kmh,
        o.wind_gust_kmh,
        o.wind_dir_deg,
        o.pressure_pa,
        o.visibility_m,
        o.precip_last_1h_mm,
        o.text_description
    FROM obs o
    JOIN latest_per_station l
      ON l.station_id = o.station_id AND l.ts_ms = o.ts_ms
    LEFT JOIN regions r
      ON r.station_code = o.station_id
    ORDER BY o.ts_ms DESC
    LIMIT 500
    """
    _write_gold("weather_snapshot", con.execute(sql).fetch_arrow_table())


def seismic_24h_summary() -> None:
    con = _duck()
    if not _scan(con, "quakes", "seismic_events/"): return
    sql = """
    WITH recent AS (
        SELECT *,
               CASE
                 WHEN magnitude <  3.0 THEN '<3'
                 WHEN magnitude <  4.5 THEN '3-4.5'
                 WHEN magnitude <  5.5 THEN '4.5-5.5'
                 WHEN magnitude <  6.5 THEN '5.5-6.5'
                 ELSE '6.5+'
               END AS mag_bucket
          FROM quakes
         WHERE ts_ms >= (epoch_ms(now()) - 24*60*60*1000)
    )
    SELECT
        COALESCE(region, 'Unknown')   AS region,
        mag_bucket,
        COUNT(*)                      AS events,
        AVG(magnitude)                AS avg_magnitude,
        AVG(depth_km)                 AS avg_depth_km,
        MIN(magnitude)                AS min_mag,
        MAX(magnitude)                AS max_mag,
        MAX(ts_ms)                    AS last_event_ms
    FROM recent
    GROUP BY region, mag_bucket
    ORDER BY events DESC
    LIMIT 500
    """
    _write_gold("seismic_24h_summary", con.execute(sql).fetch_arrow_table())


def region_alert_correlation() -> None:
    """Quakes >= seismic.magnitude threshold whose epicentre falls in a subscribed region."""
    con = _duck()
    if not _scan(con, "quakes",     "seismic_events/"):        return
    if not _scan(con, "regions",    "regions/"):               return
    if not _scan(con, "thresholds", "alert_thresholds/"):      return
    if not _scan(con, "watchlist",  "subscriber_watchlist/"):  return
    sql = """
    WITH active_threshold AS (
        SELECT MIN(threshold) AS min_mag
          FROM thresholds
         WHERE source='seismic' AND metric='magnitude'
           AND op IN ('>','>=') AND is_active=true
    ),
    recent_q AS (
        SELECT * FROM quakes
         WHERE ts_ms >= (epoch_ms(now()) - 24*60*60*1000)
           AND magnitude >= COALESCE((SELECT min_mag FROM active_threshold), 5.5)
    )
    SELECT
        r.name                  AS region,
        r.country               AS country,
        COUNT(DISTINCT q.event_id) AS quake_count_24h,
        AVG(q.magnitude)        AS avg_magnitude,
        MAX(q.magnitude)        AS max_magnitude,
        COUNT(DISTINCT w.recipient) AS subscribers_notified,
        MAX(q.ts_ms)            AS last_event_ms
    FROM regions r
    JOIN recent_q q
      ON q.lat BETWEEN r.min_lat AND r.max_lat
     AND q.lon BETWEEN r.min_lon AND r.max_lon
    LEFT JOIN watchlist w
      ON w.region_id = r.region_id
     AND w.source IN ('seismic','*')
     AND w.is_active = true
    WHERE r.is_active = true
    GROUP BY r.name, r.country
    ORDER BY max_magnitude DESC, quake_count_24h DESC
    LIMIT 100
    """
    _write_gold("region_alert_correlation", con.execute(sql).fetch_arrow_table())


# ---------- entry point ----------

ALL_MARTS = {
    "aircraft_density_by_region": aircraft_density_by_region,
    "weather_snapshot":           weather_snapshot,
    "seismic_24h_summary":        seismic_24h_summary,
    "region_alert_correlation":   region_alert_correlation,
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--all",  action="store_true")
    ap.add_argument("--mart", action="append", default=[], choices=list(ALL_MARTS))
    args = ap.parse_args()

    ensure_bucket(GOLD)
    targets = list(ALL_MARTS) if args.all else args.mart
    for m in targets:
        try:
            print(f"=== {m} ===")
            ALL_MARTS[m]()
        except Exception as e:
            print(f"[err] {m}: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()

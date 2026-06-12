# Day 10 Capstone &mdash; Run Guide

> Companion to the original `Capstone.docx` design brief. This file is the
> **operator's manual** for the implemented stack under
> [`Week_02/Day_10/Capstone_Files/`](../Capstone_Files/).
>
> The capstone ingests **3 real-time public streams** plus Postgres CDC
> through a medallion pipeline (Kafka → MinIO bronze → silver → gold) with
> Airflow + Apache Hop orchestration, Prometheus / Grafana observability,
> and three Flask dashboards (quality, business, live map).
>
> Default demo city: **Denver / Boulder, CO**.

---

## 0. Prerequisites

| Need | Why | Verify |
|---|---|---|
| Docker Desktop, ≥ 8 GB RAM, ≥ 20 GB free disk | the stack runs ~24 containers | `docker --version`, `docker compose version` |
| Windows PowerShell 5+ *(or* bash on macOS/Linux*)* | `bootstrap.ps1` / `bootstrap.sh` | `pwsh --version` |
| Python 3.11 on the host *(optional)* | runs `tests/smoke_test.py`; the bootstrap can fall back to `docker exec` | `python --version` |
| Outbound internet | OGN APRS-IS, api.weather.gov, EMSC websocket, Docker Hub pulls, Confluent Hub plugin downloads | `curl https://api.weather.gov` |
| Port 8080, 8081, 8083, 8088, 8089, 9000–9001, 5001–5003, 9090, 9093, 3000, 5432–5433 free | dashboards + brokers + UIs | `Get-NetTCPConnection -LocalPort 8080` |

> On Windows, run PowerShell **as your normal user**, not as Administrator — Docker Desktop owns the docker socket either way.

---

## 1. First boot (one command)

```powershell
cd Week_02\Day_10\Capstone_Files
.\bootstrap.ps1                   # idempotent — safe to re-run
```

(Mac/Linux: `cp .env.example .env && bash bootstrap.sh`)

`bootstrap.ps1` does, in order:

1. Confirms `.env` exists (copies from `.env.example` if not).
2. Downloads Kafka Connect plugins + the JMX Prometheus agent (skips if already cached under [debezium/plugins/](../Capstone_Files/debezium/plugins/) and [monitoring/jmx_exporter/](../Capstone_Files/monitoring/jmx_exporter/)).
3. `docker compose --env-file .env up -d` (~24 containers, see [docker-compose.yml](../Capstone_Files/docker-compose.yml)).
4. Polls MinIO, Schema Registry, Kafka Connect, Airflow until each is healthy.
5. Creates the three MinIO buckets (`bronze`, `silver`, `gold`).
6. Registers the 4 Avro schemas with Schema Registry ([app/register_app_schemas.py](../Capstone_Files/app/register_app_schemas.py)).
7. Registers the Debezium source + two S3 sink connectors ([debezium/register_all_connectors.py](../Capstone_Files/debezium/register_all_connectors.py)).
8. Unpauses the 4 continuous DAGs.
9. Prints the URL list.

**Expected first-run time:** ~3–6 min after plugin download finishes (cold pull of Kafka, Postgres, Airflow images can add several minutes).

---

## 2. Smoke test

```powershell
python tests\smoke_test.py
```

Validates:
- all Connect connectors are RUNNING (no FAILED tasks),
- every topic exists and has ≥1 message *(except `noaa.alerts` which is informational — it can legitimately be empty if no US weather alerts are active),*
- `bronze/`, `silver/`, `gold/` MinIO buckets each contain ≥1 object,
- quality / business / live-map dashboards return HTTP 200 on `/health`.

Exits non-zero on the first hard failure.

---

## 3. URLs after bring-up

| What | URL | Login |
|---|---|---|
| **Live map (gliders/weather/quakes)** | http://localhost:5003 | — |
| Business dashboard (gold marts) | http://localhost:5002 | — |
| Quality dashboard (DataOps) | http://localhost:5001 | — |
| Airflow | http://localhost:8080 | airflow / airflow |
| Kafka UI | http://localhost:8088 | — |
| Schema Registry | http://localhost:8081 | — |
| Kafka Connect REST | http://localhost:8083 | — |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Apache Hop Web | http://localhost:8089 | — |
| Prometheus | http://localhost:9090 | — |
| Alertmanager | http://localhost:9093 | — |
| Grafana (provisioned dashboards) | http://localhost:3000 | admin / admin |

---

## 4. The "wow" demo &mdash; **live map** (5–7 min walk-through)

Open http://localhost:5003 (the page on http://localhost:5003 is the
glidertracker.org-style live tracker built on top of the real Kafka topics
this stack is ingesting).

1. **Wait ~30 s after first boot.** OGN beacons start appearing as black
   plane silhouettes rotated by heading, with their callsign in a yellow
   tag (just like glidertracker.org).
   - The dashed magenta circle around **Denver** shows the 250 km
     ingestion radius set in `.env`.
   - The right-side **OnLine** panel lists every active aircraft (CN /
     altitude / speed / climb arrow).
2. **Click a plane** (on the map or in the list) → it turns magenta and
   draws its trail polyline in real time as new beacons arrive.
3. **Click the *Weather* tab.** NOAA chips (`KDEN 22°C` etc.) light up at
   the 6 Front-Range stations once the first poll completes (~120 s after
   boot). Click a station → popup shows full obs (temp / wind / humidity /
   pressure / visibility).
4. **Click the *Seismic* tab.** Earthquakes from the EMSC websocket render
   as filled circles, sized & coloured by magnitude (green ≤ M3 →
   purple > M6.5). Anything inside the 250 km Denver circle gets a
   magenta outline + a permanent pulse and increments the *(nearby N)*
   counter in the header.
5. **Toggle *Auto-zoom new quakes*** in the top-left panel — when a quake
   pushes inside the Denver radius, the map flies to it.
6. **Switch basemap** to *Esri satellite* for the eye-candy reveal during
   the demo.

The page works on a phone too — the side panel collapses below the map.

---

## 5. Show the rest of the pipeline (4–5 min)

### 5a. Bronze landing

- **Kafka UI** → `Topics`: `ogn.aircraft.positions`,
  `noaa.observations`, `seismic.events` populate continuously;
  `config.public.*` topics show Debezium snapshot rows.
- **MinIO console** → bucket `bronze` →
  `ogn.aircraft.positions/year=YYYY/month=MM/day=DD/hour=HH/` — Avro files
  written by the Confluent S3 Sink Connector every flush interval (~60–180 s).

### 5b. Silver + gold via Hop / Airflow

- **Airflow UI** → DAG `30_hop_medallion` → runs every 5 min. Fans out
  bronze → silver (4 stream entities + 3 CDC entities), then silver → gold
  (4 marts).
- **MinIO** → bucket `silver` → see deduped Avro per entity, plus
  `_quality/<entity>/` audit files.
- **MinIO** → bucket `gold` → 4 marts. Each has `latest.parquet`
  (overwritten) + `snapshot=…/part-0.parquet` (immutable per run).

### 5c. Business dashboard

- **http://localhost:5002** shows the four gold marts:
  - **Aircraft density by region** — last-1h positions joined to region bbox.
  - **Weather snapshot** — latest observation per station, joined to region by station_code.
  - **Seismic 24h summary** — events bucketed by magnitude × region.
  - **Region / alert correlation** — quakes over live threshold near subscribed regions (cross-stream join with Postgres CDC subscriber_watchlist).

### 5d. Quality dashboard

- **http://localhost:5001** lists the latest rule results from DAG
  `40_data_quality` (e.g. `seismic.freshness`,
  `noaa.obs_coords_present`, `ogn.late_events_lt_1pct`), DLQ topic sizes,
  and any firing Alertmanager alerts received via webhook.

### 5e. Trigger a config change (Debezium → silver flow)

In PowerShell:

```powershell
docker exec config-db psql -U postgres -d config -c `
  "UPDATE alert_thresholds SET threshold=5.0 WHERE source='seismic' AND metric='magnitude' AND op='>=';"
```

Within ~5 min the change flows through:
`Postgres WAL → Debezium → config.public.alert_thresholds → bronze (Avro) → silver/alert_thresholds/ → region_alert_correlation` mart.
The business dashboard's "Region alert correlation" table will reflect the new minimum.

(The DAG `15_config_drift` runs every 2 min and produces small random
mutations on the same tables so the CDC stream is always warm during the
demo — disable it in Airflow UI to keep the topic quiet.)

### 5f. Cause a DLQ event

```powershell
docker exec config-db psql -U postgres -d config -c "ALTER TABLE regions ADD COLUMN broken_col TEXT;"
# (demo, then revert)
docker exec config-db psql -U postgres -d config -c "ALTER TABLE regions DROP COLUMN broken_col;"
```

The DLQ panel on the quality dashboard (and Prometheus rule
`DLQGrowing`) will fire.

### 5g. Stop an ingestor to show freshness alerting

```powershell
docker stop ingestor-seismic
```

In ~5–10 min the `seismic.freshness` rule turns FAIL on the quality
dashboard and the `IngestorStale` alert appears in Alertmanager
(http://localhost:9093). Re-start with `docker start ingestor-seismic`.

### 5h. Grafana

http://localhost:3000 → the provisioned dashboard shows Kafka throughput
(JMX), Postgres replication lag (`pg_replication_slots`), MinIO bucket
size, and the live-map dashboard's own counters
(`livemap_aircraft_active`, `livemap_stations_active`,
`livemap_quakes_24h`).

---

## 6. Changing the demo city

Open [.env](../Capstone_Files/.env) and edit the **DEMO CITY** block:

```ini
CITY_NAME=Denver
CITY_LAT=39.74
CITY_LON=-104.99
CITY_RADIUS_KM=250
NOAA_STATIONS=KDEN,KBJC,KAPA,KCOS,KFNL,KGJT
```

Presets pre-written in `.env.example` comments:

| City | CITY_* | NOAA_STATIONS |
|---|---|---|
| Denver / Boulder (default) | `39.74, -104.99, 250` | `KDEN,KBJC,KAPA,KCOS,KFNL,KGJT` |
| New York | `40.71, -74.01, 200` | `KJFK,KLGA,KEWR,KTEB,KHPN,KISP` |
| Reno / Minden NV (heavy glider) | `39.10, -119.78, 250` | `KRNO,KMEV,KCXP,KSPK` |

Then apply only the affected services (no full rebuild needed):

```powershell
docker compose --env-file .env up -d --force-recreate `
  ingestor-ogn ingestor-noaa live-map-dashboard
```

Refresh the live map browser tab — it auto-fetches `/api/config` and
re-centres on the new city.

> NOAA is **US-only**. If you change to a non-US city, leave a US
> `NOAA_STATIONS` list and the weather chips will simply appear at their
> US locations. OGN works worldwide.

---

## 7. Common operations

| Need | Command |
|---|---|
| Tail an ingestor | `docker logs -f ingestor-ogn` (or `ingestor-noaa`, `ingestor-seismic`) |
| Force-rebuild a gold mart | `docker exec hop python3 /files/project/transforms/silver_to_gold.py --mart=seismic_24h_summary` |
| Force-rerun bronze→silver for one stream | `docker exec hop python3 /files/project/transforms/bronze_to_silver.py --stream=seismic_events` |
| Re-register one connector | `docker exec connect curl -X POST -H 'Content-Type: application/json' --data @/etc/debezium/connectors/postgres-source-config.json http://localhost:8083/connectors` |
| Restart all ingestors | `docker compose restart ingestor-ogn ingestor-noaa ingestor-seismic` |
| Inspect a silver Avro file | `docker exec hop python3 -c "import fastavro, boto3, io; ..."` — see [hop/transforms/lib_minio.py](../Capstone_Files/hop/transforms/lib_minio.py) |
| Watch Postgres replication lag | `docker exec config-db psql -U postgres -d config -c "SELECT slot_name, active, restart_lsn FROM pg_replication_slots;"` |
| Manually trigger a DAG | Airflow UI → DAG → `▶` Trigger |

---

## 8. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `bootstrap.ps1` halts at `[wait] kafka-connect …` | Plugin pull still running or Connect crashing | `docker logs connect`; re-run `.\debezium\download_plugins.ps1` |
| `ingestor-ogn` log shows `connection lost / reconnecting` repeatedly | APRS-IS server-side rate-limit | Increase the radius or use a different APRS-IS server; the client auto-reconnects |
| `noaa.observations` empty after 5+ min | NOAA blocked the User-Agent | Set a real contact in `NOAA_USER_AGENT` (your email) and recreate `ingestor-noaa` |
| `seismic.events` empty for hours | Genuine — quiet seismic period | Look at the EMSC live page; nothing to fix |
| Live map shows "disconnected" badge | Browser blocked websocket / dashboard restarting | Refresh; check `docker logs live-map-dashboard` |
| Connect tasks FAILED | Schema Registry unreachable / bad plugin path | `docker logs connect`; verify `debezium/plugins/` contents |
| `gold/` empty for a long time | Bronze hasn't flushed (low msg volume) or 30_hop_medallion paused | Trigger the DAG manually; S3 Sink rotates every 60–180s |
| Airflow webserver 502 on first hit | First db migrate still running | `docker compose logs airflow-init`; wait a minute and retry |
| Live map shows aircraft but no labels | `Aircraft labels` checkbox is off in the top-left panel | Toggle it back on |

---

## 9. Tear-down

```powershell
docker compose down            # keeps volumes (data persists across runs)
docker compose down -v         # also wipes MinIO + Postgres volumes (clean slate)
```

> Cleaning **only** the demo state (keeping Docker images cached) is
> ~30 seconds with `down -v`. Re-bootstrap takes the same ~3–6 min as a
> first run because plugins are already downloaded.

---

## 10. Architecture & file map (one-screen reference)

```
              REAL-TIME PUBLIC STREAMS                    Postgres (config)
   OGN (APRS)      NOAA (REST)     EMSC (WebSocket)      regions / thresholds
       │                │                │                / subscriber_watchlist
       ▼                ▼                ▼                        │ pgoutput
   ingestor-ogn    ingestor-noaa   ingestor-seismic            Debezium
       └──────────────────┴──────────────┴──┐                    │
                                            ▼                    ▼
                                          Kafka (8 topics + 3 DLQs)
                                            │ Confluent S3 Sink (Avro)
                                            ▼
                                    MinIO s3://bronze/
                                            │ Hop / Python
                                            ▼
                                    MinIO s3://silver/   ──► quality-dashboard :5001
                                            │ Hop / Python
                                            ▼
                                    MinIO s3://gold/     ──► business-dashboard :5002

   live-map-dashboard :5003  ← consumes 3 stream topics directly from Kafka
   prometheus :9090 ← scrapes JMX, postgres-exporter, MinIO, all 3 dashboards
   alertmanager :9093 → webhook → quality-dashboard
   grafana :3000 → provisioned dashboards
```

See [Capstone.docx](Capstone.docx) for the canonical student guide, including
the full architecture reference, topic/subject map, medallion contracts,
gold-mart definitions, and runbook appendix.

# Day 10 Capstone — End-to-end Real-Time Medallion Pipeline

A self-contained Docker stack that ingests **three live public streams**
(Open Glider Network, NOAA weather, EMSC seismic events) plus Postgres
**CDC config drift**, lands them in MinIO as bronze Avro via the Kafka
Connect S3 Sink, refines them through silver/gold using Apache Hop
pipelines (Python reference transforms drive the same logic from
Airflow), and surfaces results in two Flask dashboards plus a Prometheus
/ Grafana / Alertmanager observability stack.

## Architecture at a glance

```
              REAL-TIME PUBLIC STREAMS
                                                       Postgres (config tables)
  Open Glider Net          NOAA weather                regions / thresholds /
  APRS TCP feed   poll REST every 60s                  subscriber_watchlist
       │              │                                          │
       ▼              ▼                ▼                         ▼ pgoutput logical-replication
  ingestor-ogn   ingestor-noaa   ingestor-seismic            Debezium
  (long-running) (long-running)  (websocket)                 (Kafka Connect)
       │              │                │                         │
       └──────────────┴────────────────┴─────────────────────────┘
                                       ▼
                                    Kafka
                       ogn.aircraft.positions | noaa.observations
                       noaa.alerts            | seismic.events
                       config.public.regions  | config.public.alert_thresholds
                       config.public.subscriber_watchlist
                                       │  (Confluent S3 Sink, Avro)
                                       ▼
                              MinIO  s3://bronze/<topic>/year=…/month=…/day=…/hour=…/
                                       │  (Apache Hop / Python reference)
                                       ▼
                              MinIO  s3://silver/<entity>/  (Avro, deduped, late-filtered)
                                       │  (Apache Hop / Python reference)
                                       ▼
                              MinIO  s3://gold/<mart>/      (Parquet, snapshot+latest)
                                       │
                          ┌────────────┴────────────┐
                          ▼                          ▼
                  business-dashboard           quality-dashboard
                  (gold parquet)               (silver + DLQ + alerts)
                                                       ▲
                                                       │ webhook
                              Prometheus → Alertmanager┘
                                  ▲      ▲
                                  │      │
                            kafka-jmx  postgres-exporter  …
                                  Grafana
```

## What lives where

| Folder | Purpose |
| --- | --- |
| [docker-compose.yml](docker-compose.yml)                         | 18 services on a single `pipeline` bridge network |
| [postgres/init/](postgres/init/)                                 | Config-only schema (3 tables, ≥1000 rows) + WAL bootstrap |
| [debezium/](debezium/)                                           | Source (`config.public.*`) + S3 sinks for CDC and streams |
| [app/ingestors/](app/ingestors/)                                 | 3 long-running ingestors: OGN, NOAA, Seismic |
| [app/schemas/](app/schemas/)                                     | 4 Avro schemas for the stream subjects |
| [app/config_drift.py](app/config_drift.py)                       | Periodic mutations on reference tables → keeps CDC topics warm |
| [hop/](hop/)                                                     | Hop pipelines + workflows + Python reference transforms |
| [airflow/dags/](airflow/dags/)                                   | 5 DAGs: bootstrap, config_drift, hop medallion, data quality, business KPIs |
| [quality_dashboard/](quality_dashboard/)                         | Flask UI for DataOps (rule results, alerts, DLQ sizes) |
| [business_dashboard/](business_dashboard/)                       | Flask UI reading gold parquet marts |
| [live_map_dashboard/](live_map_dashboard/)                       | Leaflet + Socket.IO live tracker (gliders, weather, quakes) |
| [monitoring/](monitoring/)                                       | Prometheus rules, JMX exporter, Alertmanager, Grafana dashboards |
| [tests/smoke_test.py](tests/smoke_test.py)                       | End-to-end smoke test |
| [bootstrap.ps1](bootstrap.ps1) / [bootstrap.sh](bootstrap.sh)    | Idempotent one-shot bring-up |

## Data sources (all free, anonymous)

| Source | Protocol | Topic(s) | Cadence |
| --- | --- | --- | --- |
| [Open Glider Network](http://wiki.glidernet.org/) | APRS over TCP (`ogn-client`) | `ogn.aircraft.positions` | continuous (a few per second worldwide) |
| [NOAA api.weather.gov](https://www.weather.gov/documentation/services-web-api) | REST poll | `noaa.observations`, `noaa.alerts` | every `NOAA_POLL_SEC` (default 60s) |
| [EMSC Seismic Portal](https://www.seismicportal.eu/realtime.html) | WebSocket | `seismic.events` | continuous (push) |
| Postgres `config.*` | Debezium pgoutput | `config.public.regions` and 2 more | every CDC commit (driven by `15_config_drift` DAG) |

## Quick start

```powershell
cd Week_02\Day_10\Capstone_Files
Copy-Item .env.example .env
.\bootstrap.ps1
python tests\smoke_test.py
```

URLs after boot:

| | URL | Creds |
| --- | --- | --- |
| Airflow             | http://localhost:8080 | airflow / airflow |
| Kafka UI            | http://localhost:8088 | — |
| Schema Registry     | http://localhost:8081 | — |
| Kafka Connect REST  | http://localhost:8083 | — |
| MinIO console       | http://localhost:9001 | minioadmin / minioadmin |
| Apache Hop Web      | http://localhost:8089 | — |
| Quality dashboard   | http://localhost:5001 | — |
| Business dashboard  | http://localhost:5002 | — |
| Live map (gliders)  | http://localhost:5003 | — |
| Prometheus          | http://localhost:9090 | — |
| Alertmanager        | http://localhost:9093 | — |
| Grafana             | http://localhost:3000 | admin / admin |

## How the medallion lands

Bronze keys (Confluent S3 Sink, hourly TimeBasedPartitioner):

```
s3://bronze/<topic>/year=YYYY/month=MM/day=DD/hour=HH/<topic>+<partition>+<offset>.avro
```

Silver keys (Hop / Python):

```
s3://silver/<entity>/year=YYYY/month=MM/day=DD/part-<hash>.avro
s3://silver/_quality/<entity>/<utc-iso>.avro
```

Gold keys:

```
s3://gold/<mart>/snapshot=YYYYMMDDTHHMMSSZ/part-0.parquet
s3://gold/<mart>/latest.parquet     # overwritten — dashboards read this
```

## Files brought into this repo since the previous synthetic build

This capstone replaces the earlier e-commerce simulator. See
[ARCHITECTURE.md](ARCHITECTURE.md) for the design rationale and
[RUNBOOK.md](RUNBOOK.md) for the demo script.

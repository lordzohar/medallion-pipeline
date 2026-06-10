# Day 9 — Monitoring & Optimization Labs

Production-grade Kafka observability lab kit built around an **NYC taxi pipeline**.
15 hands-on labs, real CDC, real Great Expectations, real surge pricing, on a 3-broker **KRaft** cluster (no Zookeeper).

## Quick start

**Linux / macOS / WSL / Git Bash:**
```bash
chmod +x bootstrap.sh
./bootstrap.sh
source .venv/bin/activate
```

**Windows cmd:**
```cmd
bootstrap.cmd
.venv\Scripts\activate.bat
```

Then in 5 separate terminals (each with the venv activated):

```bash
python taxi_simulator.py --drivers 50    # Bronze: produces gps-pings + taxi-trips
python taxi_consumer.py                  # Bronze→Silver: routes bad → trips-dlq
python driver_enricher.py                # Silver→Gold: joins driver KTable from CDC
python surge_detector.py                 # Windowed surge pricing
python quality_validator.py              # Great Expectations on trips-clean
```

Then open:

| What                          | URL                                   |
| ----------------------------- | ------------------------------------- |
| **Live Taxi Map** (the wow)   | http://localhost:5000                 |
| Kafka UI                      | http://localhost:8080                 |
| Grafana (admin/admin)         | http://localhost:3000                 |
| Prometheus                    | http://localhost:9090                 |
| Alertmanager                  | http://localhost:9093                 |
| Kafka Connect REST            | http://localhost:8083                 |

## Architecture

The pipeline implements the **medallion pattern** end-to-end on streams:

```
                  Postgres.drivers ──► Debezium ──► cdc.public.drivers (CDC)
                                                            │
                                                            ▼ (KTable)
   taxi_simulator.py ─► gps-pings (12p/r3)                  │
                       taxi-trips (6p/r3) ─► taxi_consumer  │
                                                │           │
                                                ▼           ▼
                                        trips-clean ─► driver_enricher ─► trips-enriched
                                          │   │                               │
                                          │   ▼                               │
                                          │  surge_detector ─► surge-events   │
                                          │  quality_validator                │
                                          ▼                                   ▼
                                      trips-dlq  ◄─── dashboard.py @ :5000 ◄──┘
```

| Layer       | Topic            | Purpose                                          |
| ----------- | ---------------- | ------------------------------------------------ |
| **Bronze**  | `taxi-trips`     | Raw events from simulator (with injected bad data) |
| **Silver**  | `trips-clean`    | Schema-validated, light checks passed            |
| **Gold**    | `trips-enriched` | Joined with driver KTable from Postgres CDC      |
| Quarantine  | `trips-dlq`      | Failed records with `_dlq_reason`                |
| Compacted   | `surge-events`   | Latest surge multiplier per zone                 |

## What's special about this stack

- **No Zookeeper.** 3-node KRaft quorum lives inside the Kafka processes.
- **Multi-source.** Trip events come from a Python simulator; driver master data comes from Postgres via Debezium CDC. They are joined in-stream.
- **No magic numbers.** Every business parameter (zones, fare schedule, quality thresholds) lives in [config.json](config.json). Code reads them through [config.py](config.py).
- **Production patterns.** Idempotent producer with `acks=all` + `min.insync.replicas=2`; DLQ with reason metadata; replay tool; Great Expectations validation suite.
- **Full observability.** JMX + lag exporter → Prometheus → Grafana; container logs → Promtail → Loki; metric breaches → Alertmanager.
- **Same logic, two tools.** Surge pricing implemented twice — once in Python (`surge_detector.py`) and once as ksqlDB SQL (`ksql_taxi.sql`) — so the equivalence is visible.

## Files (flat, no nested labs)

```
LABS_GUIDE.md            ← READ THIS — 15 lab walkthrough
README.md                ← this file
config.json              ← all tunable business parameters (zones, fares, quality)
config.py                ← single import point for every Python service
docker-compose.yml       ← the entire stack (KRaft + Postgres + Connect + ksqlDB + monitoring)
bootstrap.sh / .cmd      ← one-shot setup
Dockerfile.app           ← image for the live web dashboard
requirements.txt         ← Python deps (installed into .venv)

# Topic / DB setup
setup_topics.py          ← creates 6 Kafka topics
db_seeder.py             ← seeds Postgres `drivers` table
register_connector.py    ← registers Debezium PostgreSQL connector
debezium-postgres.json   ← connector config
ksql_taxi.sql            ← ksqlDB SQL statements (Lab 12)

# Pipeline services
taxi_simulator.py        ← Bronze: produces taxi-trips + gps-pings
taxi_consumer.py         ← Bronze→Silver: validates, routes bad to DLQ
driver_enricher.py       ← Silver→Gold: stream-table join with CDC
surge_detector.py        ← Tumbling-window surge pricing
quality_validator.py     ← Great Expectations on trips-clean
dashboard.py             ← Flask + Socket.IO + Leaflet live map

# Tools
load_test.py             ← Multi-process load generator (Lab 15)
dlq_tool.py              ← Inspect / replay DLQ (Lab 8)

# Observability config
prometheus.yml           ← scrape targets
alerts.yml               ← alerting rules
alertmanager.yml         ← alert routing
promtail-config.yml      ← log shipping
kafka-lag-exporter.conf  ← consumer-lag exporter config
jmx_exporter/kafka.yml   ← JMX→Prometheus pattern rules
grafana_provisioning/    ← auto-loaded datasources + dashboard JSON
```

## Cleanup

```bash
docker compose down -v
deactivate
```

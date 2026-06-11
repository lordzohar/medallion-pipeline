# Architecture

## Design goals

1. **Real streams, not simulators.** Three independent public feeds — OGN
   (TCP APRS), NOAA (REST poll), EMSC seismic (WebSocket) — so each ingestor
   exercises a different real-world ingestion pattern.
2. **CDC where CDC belongs.** Postgres holds *config / reference* data
   only (regions, alert thresholds, subscriber watchlist). Debezium streams
   changes so downstream consumers reload thresholds without restart.
3. **Medallion in object storage.** Avro at bronze + silver (compact,
   schema-bound), Parquet at gold (analytic / dashboard friendly).
4. **Hop is the canonical engine, Python is the executable spec.**
   `.hpl`/`.hwf` files are openable in Hop GUI for the demo; the Python
   reference transforms run from Airflow so tests do not depend on a GUI
   session.
5. **Two dashboards, two audiences.** Quality dashboard for data ops;
   business dashboard for the people consuming the marts.
6. **Everything observable.** Prometheus scrapes Kafka JMX, Postgres,
   MinIO, Connect, both dashboards. Alertmanager routes firing alerts
   into the quality dashboard via webhook.

## Service inventory

| # | Container | Purpose |
|---|---|---|
| 1 | zookeeper           | Kafka coordination |
| 2 | kafka               | Broker + JMX exporter on :9404 |
| 3 | schema-registry     | Confluent Schema Registry (Avro subjects) |
| 4 | connect             | Kafka Connect (Debezium PG + S3 Sink x2) |
| 5 | kafka-ui            | Provectus Kafka UI |
| 6 | config-db           | Postgres 15 with `wal_level=logical` (reference tables only) |
| 7 | postgres-exporter   | Prometheus exporter for #6 |
| 8 | app                 | Idle utility container (`docker exec` target) |
| 9 | ingestor-ogn        | python-ogn-client → `ogn.aircraft.positions` |
| 10 | ingestor-noaa      | api.weather.gov polling → `noaa.observations`, `noaa.alerts` |
| 11 | ingestor-seismic   | EMSC websocket → `seismic.events` |
| 12 | minio              | S3-compatible storage (bronze/silver/gold) |
| 13 | hop                | Apache Hop GUI + python sidecar |
| 14 | airflow-db         | Postgres 15 (Airflow metadata) |
| 15 | airflow-init       | `airflow db migrate` + admin user (run-once) |
| 16 | airflow-webserver  | Airflow UI |
| 17 | airflow-scheduler  | DAG scheduler |
| 18 | quality-dashboard  | Flask DataOps view, `/metrics`, alert webhook |
| 19 | business-dashboard | Flask reading gold parquet, `/metrics` |
| 20 | prometheus         | Metrics + rule evaluation |
| 21 | alertmanager       | Routes alerts → quality dashboard webhook |
| 22 | grafana            | Provisioned dashboards |

Network: `day10_pipeline` (bridge), all services attached.

## Topic / subject inventory

| Topic | Partitions | Value subject | Source |
| --- | --- | --- | --- |
| `ogn.aircraft.positions`              | 6 | `ogn.aircraft.positions-value` | OGN ingestor |
| `noaa.observations`                   | 3 | `noaa.observations-value`      | NOAA ingestor |
| `noaa.alerts`                         | 3 | `noaa.alerts-value`            | NOAA ingestor |
| `seismic.events`                      | 3 | `seismic.events-value`         | EMSC ingestor |
| `config.public.regions`               | 1 | Debezium-managed Avro          | Postgres CDC |
| `config.public.alert_thresholds`      | 1 | Debezium-managed Avro          | Postgres CDC |
| `config.public.subscriber_watchlist`  | 1 | Debezium-managed Avro          | Postgres CDC |
| `config.heartbeat`                    | 1 | Debezium heartbeat             | Debezium |
| `dlq.config-source`                   | 1 | DLQ | Connect |
| `dlq.s3-sink-bronze-cdc`              | 1 | DLQ | Connect |
| `dlq.s3-sink-bronze-streams`          | 1 | DLQ | Connect |

## Medallion contracts

### Bronze (Avro on MinIO)
Written by the Confluent S3 Sink Connector. Hourly partitions:
```
s3://bronze/<topic>/year=YYYY/month=MM/day=DD/hour=HH/<topic>+<part>+<offset>.avro
```
Schema is whatever Schema Registry currently has for the subject. Two sink
connectors split the load: one for CDC topics, one for stream topics.

### Silver (Avro on MinIO)
Written by `hop/transforms/bronze_to_silver.py` (one entity per invocation).
Each silver entity is:
- deduped on the natural PK preferring the highest `ts_ms`,
- late-filtered (anything older than `max(ts_ms) − 24h` dropped),
- stored as `silver/<entity>/year=YYYY/month=MM/day=DD/part-<hash>.avro`,
- audited via `silver/_quality/<entity>/<utc-iso>.avro`.

### Gold (Parquet on MinIO)
Written by `hop/transforms/silver_to_gold.py`. Each mart uses DuckDB to
query silver Avro (with a fastavro fallback if the DuckDB avro extension
is missing). Each run writes both a snapshot and `latest.parquet`:
```
s3://gold/<mart>/snapshot=YYYYMMDDTHHMMSSZ/part-0.parquet
s3://gold/<mart>/latest.parquet
```

| Mart | Inputs | Shape |
| --- | --- | --- |
| `aircraft_density_by_region`    | `ogn_positions`, `regions`                                | one row per region, last 1h aircraft count + altitude/speed stats |
| `weather_snapshot`              | `noaa_observations`, `regions`                            | latest observation per station, joined to region by station_code |
| `seismic_24h_summary`           | `seismic_events`                                          | event counts bucketed by magnitude × region (last 24h) |
| `region_alert_correlation`      | `seismic_events`, `regions`, `alert_thresholds`, `subscriber_watchlist` | cross-stream — quakes over the live threshold whose epicentre falls inside a subscribed region |

## DAG topology

```
00_bootstrap   (manual)  ─ create topics, buckets, schemas, connectors
15_config_drift (2 min)  ─ mutate config tables → keeps Debezium busy
30_hop_medallion (5 min) ─ silver streams ∥ silver cdc → all 4 gold marts
40_data_quality  (5 min) ─ rule pack → POST quality-dashboard
50_business_kpis (5 min) ─ POST business-dashboard /api/refresh
```

The 3 stream ingestors are **long-running services**, not DAGs — real streams
do not start/stop on a cron.

## Observability map

```
Kafka JMX            :9404 ──┐
postgres-exporter    :9187 ──┤
MinIO /metrics       :9000 ──┤        rule_files: data_quality, pipeline
quality-dashboard    :5001 ──┼──► Prometheus :9090 ──► Alertmanager :9093 ──┐
business-dashboard   :5002 ──┤                                              │
kafka-connect REST   :8083 ──┘                                              │
                                                                            ▼
                                                          quality-dashboard /webhook/alerts
```

Quality dashboard exposes:
- `quality_rule_results_total{rule,status}` (Counter) — every rule result
- `quality_active_alerts` (Gauge) — count of currently firing alerts
- `quality_dlq_size{topic}` (Gauge) — sampled every 30s from Kafka watermarks

Business dashboard exposes:
- `business_dashboard_refresh_total` (Counter)
- `business_dashboard_mart_rows{mart}` (Gauge)

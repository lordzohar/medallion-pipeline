# Runbook

## 1. First boot

```powershell
cd Week_02\Day_10\Capstone_Files
Copy-Item .env.example .env
.\bootstrap.ps1
```

`bootstrap.ps1` does, idempotently:
1. Ensure `.env` exists.
2. Download Kafka Connect plugins + JMX agent (skips if already there).
3. `docker compose up -d` (~22 containers).
4. Poll MinIO, Schema Registry, Connect, Airflow until each reports healthy.
5. Create MinIO buckets, register Avro schemas, register the Debezium + S3
   sink connectors.
6. Unpause the 4 continuous DAGs (`15_config_drift`, `30_hop_medallion`,
   `40_data_quality`, `50_business_kpis`).

> First boot takes ~3–5 min after the plugin download finishes. The
> ingestors begin emitting as soon as `register_app_schemas` finishes —
> watch them in Kafka UI under topics `ogn.aircraft.positions`,
> `noaa.observations`, `seismic.events`.

## 2. Smoke test

```powershell
python tests\smoke_test.py
```

This validates connectors are RUNNING, every topic has messages (except
`noaa.alerts` which is informational — it can legitimately be empty), all
3 buckets contain ≥1 object, and both dashboards respond on `/health`.

## 3. Demo script (~10 min)

1. **Open Kafka UI** ([http://localhost:8088](http://localhost:8088)) — show the live topics filling up. Click
   into `seismic.events` and watch new earthquakes appear in real time.
2. **Open the business dashboard** ([http://localhost:5002](http://localhost:5002)) — show the
   four gold marts populated (aircraft density, weather snapshot, seismic
   24h, region correlation).
3. **Trigger a config change** to demonstrate Debezium → silver propagation:
   ```powershell
   docker exec config-db psql -U postgres -d config -c "UPDATE alert_thresholds SET threshold=5.0 WHERE source='seismic' AND metric='magnitude' AND op='>=';"
   ```
   Within ~5 min the change flows to `config.public.alert_thresholds`,
   then to `silver/alert_thresholds/`, then `region_alert_correlation`
   picks up the new minimum threshold.
4. **Cause a DLQ event** — force a bad message:
   ```powershell
   docker exec config-db psql -U postgres -d config -c "ALTER TABLE regions ADD COLUMN broken_col TEXT;"
   ```
   (Then revert: `... DROP COLUMN broken_col;`.) Watch the quality
   dashboard ([http://localhost:5001](http://localhost:5001)) DLQ panel rise.
5. **Stop an ingestor**:
   ```powershell
   docker stop ingestor-seismic
   ```
   In ~5–10 min `seismic.freshness` rule turns FAIL on the quality
   dashboard and the `IngestorStale` alert fires (visible in Alertmanager
   + the dashboard).
6. **Grafana** ([http://localhost:3000](http://localhost:3000)) — show Kafka throughput, postgres
   replication lag, MinIO bucket size from the provisioned dashboard.

## 4. Common operations

| Need | Command |
| --- | --- |
| Tail any ingestor | `docker logs -f ingestor-ogn` (or noaa / seismic) |
| Force-rerun gold for one mart | `docker exec hop python3 /files/project/transforms/silver_to_gold.py --mart=seismic_24h_summary` |
| Re-register a connector | `python debezium/register_all_connectors.py --restart pipeline-config-source` |
| Inspect a silver Avro file | `docker exec hop python3 -c "import fastavro,boto3,io; ..."` (see lib_minio.py) |
| Clear Kafka offsets & restart from scratch | `docker compose down -v` then bootstrap again |
| See replication lag | `docker exec config-db psql -U postgres -d config -c "SELECT slot_name, active, restart_lsn FROM pg_replication_slots;"` |

## 5. Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `ingestor-ogn` keeps reconnecting | APRS-IS rate-limit / outage | Tighten with `OGN_APRS_FILTER` (regional) or retry — it auto-reconnects |
| `noaa.observations` empty | NOAA blocked the User-Agent | Set a real one in `NOAA_USER_AGENT` (your contact email) |
| `seismic.events` empty for hours | Genuine — quiet period | Look at the EMSC live page; nothing to do |
| Connect tasks FAILED | Plugin missing / SR unreachable | `docker logs connect`; re-run `.\debezium\download_plugins.ps1` |
| `gold/` empty | Bronze hasn’t flushed yet | S3 Sink rotates every 60–180s; wait for the next interval |
| Airflow webserver 502 | Slow first migrate | `docker compose logs airflow-init`; usually self-corrects |

## 6. Tear-down

```powershell
docker compose down            # keeps volumes (data persists)
docker compose down -v         # nukes volumes (clean slate)
```

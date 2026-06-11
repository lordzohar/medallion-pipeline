# Apache Hop layer

This folder contains both the **Apache Hop** pipeline/workflow XML (the *.hpl /
*.hwf files openable in Hop GUI / runnable by `hop-run`) **and** a working
**Python reference implementation** of the same logic.

```
hop/
├── Dockerfile                       apache/hop:2.10.0 + Python sidecar
├── metadata/                        Hop project metadata (connections, VFS)
├── pipelines/                       .hpl files
│   ├── bronze_to_silver_streams.hpl     (OGN / NOAA / Seismic)
│   ├── bronze_to_silver_cdc.hpl         (config reference tables)
│   └── silver_to_gold_mart.hpl          (one mart per invocation)
├── workflows/                       .hwf files
│   ├── wf_bronze_to_silver.hwf
│   ├── wf_silver_to_gold.hwf
│   └── wf_full_medallion.hwf
├── transforms/                      Python reference (what Airflow actually runs)
│   ├── lib_minio.py
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
└── README.md
```

## Why both Hop XML and Python?

The lab brief asks for **Apache Hop** as the transform engine, so the canonical
artefacts are the `.hpl`/`.hwf` files; open them in Hop GUI
(`http://localhost:8089`) to walk through the visual pipelines during the demo.

Hop in a container without a configured project tends to need a one-time GUI
visit to bind metadata. So grading and the smoke tests do not depend on manual
GUI clicks, the **Python reference transforms** under `transforms/` implement
the exact same logic and are what the Airflow `HopOperator` invokes by default
(`mode="python"`). Switch to `mode="hop-run"` once you have walked through and
saved the pipelines in the Hop GUI.

## Mapping: Hop pipeline ↔ Python module

| Hop file | Python entry-point | What it does |
| --- | --- | --- |
| `pipelines/bronze_to_silver_streams.hpl` (param `ENTITY`) | `bronze_to_silver.stream(topic)` | For each live-stream topic (`ogn.aircraft.positions`, `noaa.observations`, `noaa.alerts`, `seismic.events`): read latest 2 h of bronze Avro → dedupe on (pk, `ts_ms`) → late-event filter (24 h) → write silver Avro |
| `pipelines/bronze_to_silver_cdc.hpl` (param `ENTITY`)     | `bronze_to_silver.cdc(topic)`    | Same logic on the Debezium CDC topics (`config.public.regions`, `config.public.alert_thresholds`, `config.public.subscriber_watchlist`) — unwraps `__ts_ms` |
| `pipelines/silver_to_gold_mart.hpl` (param `MART`)        | `silver_to_gold.<mart>()`        | Builds one Parquet mart: `aircraft_density_by_region`, `weather_snapshot`, `seismic_24h_summary`, `region_alert_correlation` |

## Running by hand

```bash
# Python (what Airflow runs)
docker exec hop python3 /files/project/transforms/bronze_to_silver.py --all
docker exec hop python3 /files/project/transforms/silver_to_gold.py  --all

# One entity only
docker exec hop python3 /files/project/transforms/bronze_to_silver.py --stream=seismic_events
docker exec hop python3 /files/project/transforms/silver_to_gold.py   --mart=seismic_24h_summary

# Hop CLI (after configuring the project in the GUI)
docker exec hop /opt/hop/hop-run.sh \
    --project=day10 --workflow=workflows/wf_full_medallion.hwf
```

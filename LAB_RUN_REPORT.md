# Medallion Pipeline Lab Run Report

Run date: 2026-05-28 20:51 IST  
Workspace: `/home/ubuntu/Downloads/Medallion-pipeline`

## Environment Summary

- Python: PASS (`python3 3.10.12`)
- Git/curl: PASS
- Docker CLI: PASS (`Docker 29.5.1`)
- Docker Compose: PASS (`docker compose v5.1.3`)
- Docker daemon access: available via sudo for lab execution
- Spark: PASS (`spark-submit 4.1.1`)
- Java: PASS (`OpenJDK 21.0.10`)
- LibreOffice: PASS
- Apache Hop CLI/GUI: NOT FOUND
- Python module `kafka`: PASS after `python3 -m pip install --user kafka-python`
- Python module `pandas`: NOT FOUND
- Python module `pyspark`: PASS

## Day-by-Day Status

| Day | Status | Evidence / Reason |
| --- | --- | --- |
| Day 01 | SUCCESS | Completed local Python labs 0-4 and final validation. `src/validate_outputs.py` reported all PASS. Generated outputs under `Week_01/Day_01/Lab_Files/out/`. |
| Day 02 | SUCCESS | Completed Kafka broker, topic creation, CLI producer/consumer, Python producer/consumer, partitioning experiment, and consumer group offset check. Report: `Week_01/Day_02/DAY_02_LAB_REPORT.md`. |
| Day 03 | SUCCESS | Completed Kafka broker and Connect worker, consumer groups, offsets/reset, replication/acks, Python manual commit consumer, and FileStream source/sink connectors. Report: `Week_01/Day_03/DAY_03_LAB_REPORT.md`. |
| Day 04 | SUCCESS | Completed PostgreSQL CDC with Debezium: services started, connector RUNNING, initial snapshot records consumed, insert/update CDC observed, and incremental snapshot signal verified. Report: `Week_01/Day_04/DAY_04_LAB_REPORT.md`. |
| Day 05 | SUCCESS WITH DEVIATION | Completed PostgreSQL CDC, MySQL CDC, clickstream, CSV ingestion, DLQ, Kafka UI/service checks, and generic consumer evidence. Deviation: JSON converters used instead of Avro, so Schema Registry has no subjects. Report: `Week_01/Day_05/DAY_05_LAB_REPORT.md`. |
| Day 06 | BLOCKED | Lab requires Apache Hop for visual pipelines and Docker Compose for Kafka streaming exercise. Hop is not installed and Docker daemon access is denied. |
| Day 07 | PARTIAL / BLOCKED | Inspected `spark_bronze_to_silver.py`; identified event timestamp conversion, `order_id` deduplication, and checkpoint path. Runnable MinIO/Kafka bucket tasks require Docker daemon access. |
| Day 08 | BLOCKED | Lab requires Airflow, Redis, and PostgreSQL via Docker Compose. Docker daemon access is denied. The DAG file was inspected. |
| Day 09 | BLOCKED | Lab requires Kafka, Schema Registry, ksqlDB, and Kafka UI via Docker Compose. Docker daemon access is denied. Python scripts also require `kafka-python`. |
| Day 10 | BLOCKED | Capstone requires PostgreSQL sources, Kafka, Schema Registry, Kafka Connect/Debezium, ksqlDB, Kafka UI, and optional MinIO via Docker Compose. Docker daemon access is denied. |

## Day 01 Commands Run

From `Week_01/Day_01/Lab_Files`:

```bash
bash setup/verify_environment.sh
python3 src/reset_outputs.py
python3 src/tightly_coupled_order.py --fail-notification
python3 src/tightly_coupled_order.py
python3 src/event_bus_pipeline.py produce
python3 src/event_bus_pipeline.py consume --fail-notification
python3 src/event_bus_pipeline.py consume
python3 src/event_bus_pipeline.py consume
python3 src/batch_pipeline.py
python3 src/stream_pipeline.py --delay-seconds 0
python3 src/medallion_pipeline.py
python3 src/validate_outputs.py
```

Expected intentional failures:

- `tightly_coupled_order.py --fail-notification` returned a simulated notification outage.
- `event_bus_pipeline.py consume --fail-notification` returned a simulated consumer outage.

Final Day 01 validator result:

```text
PASS - event inbox exists
PASS - event consumer checkpoint exists
PASS - batch summary exists
PASS - stream state exists
PASS - bronze raw events exist
PASS - silver view exists
PASS - gold KPIs exist
PASS - gold excludes cancelled-only adjustment
PASS - six streaming events handled
PASS - generated evidence is ready for debrief.
```

## Static Checks Completed

```bash
python3 -m py_compile $(find Week_*/Day_*/Lab_Files -name '*.py' | sort)
```

Result: PASS, all Python files compile syntactically.

## Required Next Action

To continue Days 02-10, the current user needs Docker daemon access. The usual fix is one of:

```bash
sudo usermod -aG docker ubuntu
```

Then log out and back in, or restart the shell/session so the new group is active.

Alternatively, run the lab commands with sudo-enabled Docker access.

Additional software/packages needed after Docker is available:

- `python3 -m pip install --user kafka-python pandas`
- Apache Hop for Day 06 visual pipeline exercises

After Docker access is fixed, resume with Day 02 and run each lab guide in order.

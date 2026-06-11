"""Regenerate Capstone.docx with the actual real-streams capstone content.

Preserves the original Word-document structure (Title, Heading 1 sections,
List Bullet / List Number) so the document still looks consistent with the
course style. Overwrites Capstone.docx in place.
"""
from pathlib import Path
import docx

HERE = Path(__file__).resolve().parent
OUT  = HERE / "Capstone.docx"

doc = docx.Document()

def H(text, level=1): doc.add_heading(text, level=level)
def P(text):          doc.add_paragraph(text)
def CODE(text):
    p = doc.add_paragraph(text)
    p.style = doc.styles["Normal"]
    for r in p.runs: r.font.name = "Consolas"
def UL(items):
    for it in items: doc.add_paragraph(it, style="List Bullet")
def OL(items):
    for it in items: doc.add_paragraph(it, style="List Number")

# ---- Title ---------------------------------------------------------------
t = doc.add_paragraph()
t.style = doc.styles["Title"]
t.add_run("Day 10 Student Lab Guide")
P("Capstone: Real-Time Medallion Pipeline with CDC, Kafka, MinIO, Apache Hop, Airflow and Live Dashboards")
P("")

# ---- Capstone scenario ---------------------------------------------------
H("Capstone Scenario")
P(
    "Build and operate an end-to-end real-time data pipeline that ingests THREE genuine public "
    "streaming sources, lands them in object storage as a medallion (bronze / silver / gold), "
    "exposes the gold layer through dashboards, and is fully observable. The pipeline must include "
    "Postgres CDC via Debezium so reference / configuration changes flow through the same medallion "
    "as the streams. A glidertracker.org-style live map is the user-facing 'wow' surface."
)
P("Sources you will integrate:")
UL([
    "Open Glider Network (OGN) — APRS-IS TCP feed of live aircraft positions.",
    "NOAA api.weather.gov — REST polling of US weather station observations + alerts.",
    "EMSC Seismic Portal — WebSocket of real earthquake events worldwide.",
    "Postgres reference tables (regions, alert_thresholds, subscriber_watchlist) via Debezium pgoutput.",
])

# ---- Deliverables --------------------------------------------------------
H("Deliverables")
UL([
    "A running docker-compose stack (~22 containers) on a single bridge network.",
    "A scoped demo city (default: Denver / Boulder, CO) configured through a single .env block.",
    "Avro contracts in Schema Registry for the 4 stream subjects.",
    "Debezium source + two S3 Sink connectors (CDC topics, stream topics) configured and RUNNING.",
    "Bronze landing in MinIO under s3://bronze/<topic>/year=…/month=…/day=…/hour=…/ as Avro.",
    "Silver layer (Avro) produced by Apache Hop pipelines or their Python reference transforms.",
    "Gold layer (Parquet) with FOUR marts: aircraft_density_by_region, weather_snapshot, "
    "seismic_24h_summary, region_alert_correlation.",
    "Airflow DAGs orchestrating the medallion every 5 min plus a config-drift DAG.",
    "Three Flask dashboards: quality (DataOps), business (gold parquet), live map (Leaflet + Socket.IO).",
    "Prometheus + Alertmanager + Grafana with at least one freshness rule + one DLQ rule.",
    "A passing smoke test (tests/smoke_test.py) and a written demo script.",
])

# ---- Task 1 --------------------------------------------------------------
H("Task 1 — Start the Stack")
P("All commands are run from the capstone folder:")
CODE("cd Week_02\\Day_10\\Capstone_Files\nCopy-Item .env.example .env\n.\\bootstrap.ps1                  # macOS/Linux: bash bootstrap.sh\n")
P(
    "bootstrap.ps1 is idempotent — re-run it any time. It downloads Kafka Connect plugins + the "
    "JMX agent, brings the stack up, waits for MinIO / Schema Registry / Kafka Connect / Airflow to "
    "become healthy, creates the three MinIO buckets, registers the 4 Avro schemas, registers the "
    "Debezium source + two S3 Sink connectors, unpauses the 4 continuous DAGs, then prints the URL list."
)
P("Verify everything is up:")
CODE("docker compose ps\npython tests\\smoke_test.py\n")

# ---- Task 2 --------------------------------------------------------------
H("Task 2 — Configure the Demo City")
P(
    "Every source is scoped to one city to keep volumes small. Open .env and edit the DEMO CITY block. "
    "Default is Denver / Boulder, CO (real glider activity at Boulder + 6 NOAA stations within 250 km)."
)
CODE(
    "CITY_NAME=Denver\n"
    "CITY_LAT=39.74\n"
    "CITY_LON=-104.99\n"
    "CITY_RADIUS_KM=250\n"
    "NOAA_STATIONS=KDEN,KBJC,KAPA,KCOS,KFNL,KGJT\n"
)
P("Presets pre-written in .env.example comments: New York (KJFK,KLGA,KEWR…), Reno/Minden NV (KRNO,KMEV…).")
P("If you change the city after the stack is already up, recreate only the affected services:")
CODE("docker compose --env-file .env up -d --force-recreate ingestor-ogn ingestor-noaa live-map-dashboard\n")

# ---- Task 3 --------------------------------------------------------------
H("Task 3 — Verify Stream Ingestion (Bronze)")
P("Three long-running ingestor services produce Avro into Kafka:")
UL([
    "ingestor-ogn  → topic ogn.aircraft.positions",
    "ingestor-noaa → topics noaa.observations and noaa.alerts",
    "ingestor-seismic → topic seismic.events",
])
P("Watch them populate in Kafka UI (http://localhost:8088). Then confirm landing in MinIO:")
CODE(
    "# Kafka REST view of connector status\n"
    "curl -s http://localhost:8083/connectors | jq\n"
    "curl -s http://localhost:8083/connectors/pipeline-config-source/status | jq\n"
    "curl -s http://localhost:8083/connectors/s3-sink-bronze-streams/status | jq\n"
)
P("Browse MinIO console (http://localhost:9001, minioadmin / minioadmin) → bucket 'bronze' → see hourly-partitioned Avro files.")
P("Capture evidence: one screenshot of Kafka UI topic with messages, one of MinIO bronze tree, one of /connectors REST output.")

# ---- Task 4 --------------------------------------------------------------
H("Task 4 — Run the Medallion (Silver and Gold)")
P("Airflow DAG 30_hop_medallion runs every 5 minutes; you can also trigger it manually from the Airflow UI (http://localhost:8080, airflow / airflow).")
P("It fans out:")
UL([
    "bronze → silver for 4 stream entities + 3 CDC entities (Apache Hop pipeline bronze_to_silver_*.hpl, "
    "or the Python reference transform in hop/transforms/bronze_to_silver.py).",
    "silver → gold for all 4 marts (silver_to_gold_mart.hpl with a MART parameter, or silver_to_gold.py).",
])
P("Inspect the result in MinIO:")
CODE(
    "# silver — Avro, deduped, late-filtered\n"
    "s3://silver/<entity>/year=YYYY/month=MM/day=DD/part-<hash>.avro\n"
    "# gold — Parquet, one immutable snapshot + one overwritten 'latest'\n"
    "s3://gold/<mart>/snapshot=YYYYMMDDTHHMMSSZ/part-0.parquet\n"
    "s3://gold/<mart>/latest.parquet\n"
)
P("Force-rebuild any single mart from the host:")
CODE("docker exec hop python3 /files/project/transforms/silver_to_gold.py --mart=seismic_24h_summary\n")

# ---- Task 5 --------------------------------------------------------------
H("Task 5 — Stakeholder View (Live Map + Business Dashboard)")
P("Two surfaces target two audiences.")
P("(a) Live map — http://localhost:5003 — built directly from the three Kafka stream topics. Open it and walk through:")
OL([
    "Aircraft appear as rotated plane silhouettes with a yellow callsign tag (glidertracker.org style). Click one — it turns magenta and its trail polyline draws live.",
    "Weather tab: NOAA chips (e.g. KDEN 22°C) populate after ~120 s. Click for the full popup (temp / wind / humidity / pressure / visibility).",
    "Seismic tab: events render as filled circles sized + coloured by magnitude. Anything inside the city radius gets a magenta outline + permanent pulse + 'nearby' counter.",
    "Toggle 'Auto-zoom new quakes' so the demo audience sees a quake fly in from anywhere on Earth.",
    "Switch basemap to Esri satellite for the reveal.",
])
P("(b) Business dashboard — http://localhost:5002 — reads gold parquet. Shows the four marts side-by-side. Each mart cell is your Gold-level KPI artefact.")
P("Pick one KPI for your write-up — recommended choices:")
UL([
    "aircraft_density_by_region.positions_last_1h — 'gliders tracked in the last hour, per region'.",
    "weather_snapshot.temperature_c — 'most recent NOAA observation per station'.",
    "seismic_24h_summary.events with magnitude_bucket — 'earthquakes in the last 24h by magnitude band'.",
    "region_alert_correlation.subscribers_notified — 'subscribers whose region just received a quake above the live threshold'.",
])
P("Document the KPI in ONE sentence. Explain freshness: which Kafka event starts the update, and where the user can see the timestamp.")

# ---- Task 6 --------------------------------------------------------------
H("Task 6 — Show the CDC Loop (Debezium → Silver → Gold)")
P("Trigger a live config change and watch it propagate through every layer.")
CODE(
    "# Lower the seismic alert threshold so more quakes will trigger correlation.\n"
    "docker exec config-db psql -U postgres -d config -c \\\n"
    "  \"UPDATE alert_thresholds SET threshold=5.0 WHERE source='seismic' AND metric='magnitude' AND op='>=';\"\n"
)
P(
    "Within ~5 min the change flows Postgres WAL → Debezium → config.public.alert_thresholds → "
    "bronze (Avro) → silver/alert_thresholds/ → region_alert_correlation mart. Refresh the business "
    "dashboard and screenshot the 'before' and 'after' rows."
)
P("The 15_config_drift DAG runs every 2 min and produces small random mutations on the same tables so the CDC stream is always warm during a demo — pause it from Airflow UI if you want quiet topics.")

# ---- Task 7 --------------------------------------------------------------
H("Task 7 — Operational Readiness")
P("Inject one controlled failure each and capture evidence in the quality dashboard (http://localhost:5001) and Alertmanager (http://localhost:9093).")
UL([
    "Stop an ingestor: docker stop ingestor-seismic. After 5–10 min the seismic.freshness rule turns FAIL and the IngestorStale alert fires.",
    "Cause a DLQ event: docker exec config-db psql -U postgres -d config -c \"ALTER TABLE regions ADD COLUMN broken_col TEXT;\" — watch the DLQ panel grow, then revert with DROP COLUMN.",
    "Pause a DAG in Airflow → notice silver and gold for that entity stop refreshing on the quality dashboard timestamps.",
])
P("State for each failure: the monitoring signal, the recovery action, and how long until normal service resumed (visible in the dashboard).")

# ---- Submission ----------------------------------------------------------
H("Submission Checklist")
UL([
    "Architecture diagram (ARCHITECTURE.md is provided — annotate or replace).",
    "Screenshot pack: Kafka UI topics, MinIO bronze tree, MinIO gold mart, business dashboard, live map (with selected plane + magenta trail), quality dashboard during a failure.",
    "Output of python tests/smoke_test.py with all checks green.",
    "One sentence KPI definition + two dashboard artefacts (live map + business dashboard count as two).",
    "Short operations note for each of the three Task 7 failures: incident → evidence → resolution → prevention.",
    "When finished: docker compose down. Use docker compose down -v only if you intentionally want to erase MinIO + Postgres volumes.",
])

# ---- References ----------------------------------------------------------
H("References inside Capstone_Files/")
UL([
    "README.md — project overview and URLs.",
    "ARCHITECTURE.md — service inventory, topic map, medallion contracts, mart definitions.",
    "RUNBOOK.md — terse operator handbook.",
    "../Capstone_Guide/Capstone_Run_Guide.md — full step-by-step run guide with troubleshooting table.",
    "bootstrap.ps1 / bootstrap.sh — idempotent stack bring-up.",
    "tests/smoke_test.py — end-to-end smoke test.",
])

doc.save(OUT)
print(f"wrote {OUT}")

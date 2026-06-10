# Day 9 — 15 Hands-On Labs

> **Pipeline Monitoring · Logging & Error Handling · Data Quality · Scaling**
> Reference dataset: **NYC Uber-style taxi data** (lat/lon, fares, surge, payments, drivers from CDC)
> Stack: **KRaft Kafka** (3 brokers) + Postgres + Debezium CDC + ksqlDB + Schema Registry + Prometheus + Grafana + Loki + Alertmanager + Kafka UI + an expectation-based validator

Each lab has: **Goal → Steps → What to observe → Discussion**.
Plan ~20 minutes per lab over a 5-hour session.

## Architecture at a glance

```
                  Postgres.drivers ──► Debezium ──► cdc.public.drivers (CDC topic)
                                                            │
                                                            ▼  (KTable)
   taxi_simulator.py ─► gps-pings (12p/r3)                  │
                       taxi-trips (6p/r3) ─► taxi_consumer  │
                                                │           │
                                                ▼           ▼
                                         trips-clean  ─►  driver_enricher  ─► trips-enriched
                                          │       │                                 │
                                          │       ▼                                 │
                                          │  surge_detector ─► surge-events         │
                                          │  quality_validator (expectation suite)  │
                                          ▼                                         ▼
                                      trips-dlq  ◄────────── dashboard.py ◄───────  │
                                                            (live map @ :5000)      │

   Observability: JMX → Prometheus → Grafana | logs → Promtail → Loki → Grafana | alerts → Alertmanager
```

This is the **medallion pattern** in streaming form:

| Layer        | Topic              | Meaning                                    |
| ------------ | ------------------ | ------------------------------------------ |
| **Bronze**   | `taxi-trips`       | Raw events from the simulator (with bad data) |
| **Silver**   | `trips-clean`      | Schema-validated, light checks passed      |
| **Gold**    | `trips-enriched`   | Joined with driver KTable from Postgres CDC |
| Quarantine  | `trips-dlq`        | Failed records with `_dlq_reason`          |

---

## Prerequisites (do this once)

Open a terminal in `Lab_Files/`.

**Linux / macOS / WSL / Git Bash:**
```bash
cd Lab_Files
chmod +x bootstrap.sh
./bootstrap.sh
```

**Windows cmd:**
```cmd
cd Lab_Files
bootstrap.cmd
```

The bootstrap will:
1. Download the JMX Prometheus javaagent.
2. Create a Python virtual environment in `.venv/` and install dependencies.
3. Start the full Docker stack (3 Kafka brokers, Postgres, Kafka Connect, ksqlDB, Schema Registry, Prometheus, Grafana, Loki, Alertmanager, Kafka UI).
4. Create all topics, seed the `drivers` table, and register the Debezium PostgreSQL connector.

**Activate the venv in every new terminal you open** (the lab scripts run on the host, not in containers):

```bash
# Linux / macOS / WSL / Git Bash
source .venv/bin/activate
```
```cmd
:: Windows cmd
.venv\Scripts\activate.bat
```

Confirm everything is up:

```bash
docker compose ps           # 15 containers, all "running"
curl http://localhost:8080  # Kafka UI
curl http://localhost:8083/connectors  # ["drivers-postgres-source"]
```

---

# PART 1 — Pipeline & Data Sources (Labs 1–4)

## Lab 1 — KRaft Cluster & Topic Design

**Goal:** Understand why KRaft replaces Zookeeper, and design topics for a real taxi pipeline.

**Steps:**

1. Inspect the cluster — note there is **no Zookeeper container**:
   ```bash
   docker compose ps
   docker exec kafka1 kafka-metadata-quorum --bootstrap-server kafka1:29092 describe --status
   ```
2. List the topics that `setup_topics.py` created:
   ```bash
   docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --list
   ```
3. Describe each topic and read its partition count, replication, and configs:
   ```bash
   docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic taxi-trips
   docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic gps-pings
   docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic surge-events
   ```

**What to observe:**
- The KRaft quorum lives **inside** the three Kafka processes (no external coordinator).
- `taxi-trips` has 6 partitions; `gps-pings` has 12 (10× higher volume justifies more parallelism).
- `surge-events` uses `cleanup.policy=compact` — only the **latest** surge value per zone is kept (compacted topic ≈ KTable).
- `trips-dlq` keeps 24 h of retention so you have time to triage failures.

**Discussion:**
- Why partition `gps-pings` more aggressively than `taxi-trips`?
- Why is `cleanup.policy=compact` appropriate for `surge-events` but not for `taxi-trips`?
- What does `min.insync.replicas=2` guarantee on a 3-broker cluster?

---

## Lab 2 — Cluster Inspection with Kafka UI

**Goal:** Get fluent with the operator's GUI before going terminal-only.

**Steps:**

1. Open http://localhost:8080 → `taxi-kraft-cluster`.
2. **Brokers** tab → confirm 3 brokers with their JMX metrics visible.
3. **Topics** tab → click `taxi-trips` → look at the **Messages** view.
4. **Schema Registry** tab → empty for now (we'll see it later).
5. **KSQL DB** tab → ksqlDB cluster reachable.
6. Run the simulator briefly:
   ```bash
   python taxi_simulator.py --drivers 20
   ```
7. Watch messages stream into `taxi-trips` and `gps-pings` live in Kafka UI.
8. Stop the simulator (Ctrl+C).

**What to observe:**
- Per-partition message counts are roughly balanced → good key distribution.
- Headers, keys, values, and offsets are all visible.
- **Consumer Groups** tab is empty — no one is consuming yet.

**Discussion:**
- Why does the simulator key messages by `driver_id`?
- What would happen if every message used the same key?

---

## Lab 3 — Multiple Sources: Postgres + Debezium CDC

**Goal:** Combine an *event* stream (taxi trips) with a *reference* stream (drivers) using Change Data Capture. This is how real pipelines stitch together data from operational databases.

**Steps:**

1. Inspect the seeded drivers table:
   ```bash
   docker exec -it postgres psql -U taxi -d taxi -c "SELECT driver_id, full_name, vehicle_make, rating FROM drivers LIMIT 5;"
   ```
2. Verify the Debezium connector is running:
   ```bash
   curl -s http://localhost:8083/connectors/drivers-postgres-source/status | python -m json.tool
   ```
   You should see `connector.state = RUNNING` and `tasks[0].state = RUNNING`.
3. Confirm the CDC topic exists and contains a snapshot of all 100 drivers:
   ```bash
   docker exec kafka1 kafka-console-consumer \
     --bootstrap-server kafka1:29092 \
     --topic cdc.public.drivers \
     --from-beginning --max-messages 3
   ```
4. **Live CDC test** — change a driver in Postgres while watching Kafka:
   ```bash
   docker exec -it postgres psql -U taxi -d taxi -c \
     "UPDATE drivers SET rating = 4.99 WHERE driver_id = 'DRV-0001';"
   ```
   Within 1 second a new message appears on `cdc.public.drivers` for `DRV-0001`.
5. Now start the **enricher** which joins the trip stream with the driver KTable:
   ```bash
   python driver_enricher.py
   ```
   Then in another terminal start the simulator + consumer:
   ```bash
   python taxi_simulator.py --drivers 30   # terminal A
   python taxi_consumer.py                  # terminal B
   ```
6. Inspect an enriched trip:
   ```bash
   docker exec kafka1 kafka-console-consumer \
     --bootstrap-server kafka1:29092 \
     --topic trips-enriched \
     --from-beginning --max-messages 1 | python -m json.tool
   ```
   It now contains `driver_name`, `vehicle`, `driver_rating`, `license_number`.

**What to observe:**
- CDC gives you the database table as a Kafka topic, *automatically maintained*.
- The enricher implements the canonical Kafka Streams **stream-table join** in pure Python: it materializes the CDC topic into an in-memory dict, then enriches each trip on-the-fly.
- A single UPDATE in Postgres flows through Debezium → Kafka → enricher in <1 s.

**Discussion:**
- What's the difference between Debezium's *snapshot* phase and *streaming* phase?
- Why does Debezium need `wal_level=logical` in Postgres?
- When would you reach for CDC vs. periodic batch sync (e.g. nightly dump)?

---

## Lab 4 — The Live Map Dashboard (the wow moment)

**Goal:** See the entire pipeline running on a live NYC map, with DLQ + quality side panels.

**Steps:**

Open 5 terminals (activate `.venv` in each) and run:

```bash
# Terminal 1 — produces taxi-trips + gps-pings (uses real driver IDs from Postgres)
python taxi_simulator.py --drivers 50

# Terminal 2 — bronze→silver (validates, splits to trips-clean + trips-dlq)
python taxi_consumer.py

# Terminal 3 — silver→gold (joins driver KTable from CDC)
python driver_enricher.py

# Terminal 4 — windowed aggregation: surge multiplier per zone
python surge_detector.py

# Terminal 5 — Expectation validator on trips-clean
python quality_validator.py
```

Open http://localhost:5000.

**What to observe:**
- ~50 taxi emojis moving across Manhattan.
- `ON_TRIP` drivers are bright, `IDLE` drivers dim.
- Zone circles change colour when the surge multiplier rises.
- **Right-side DLQ panel** shows the most recent bad records with their reason.
- **Right-side Quality panel** shows live pass/fail counts per rule.
- Bottom event log streams `TRIP / SURGE / DLQ` lines as they happen.

**Discussion:**
- Trace each of the four data flows on the dashboard back to its Kafka topic.
- What is the latency from "trip produced" to "appears on map"? (Open browser DevTools → Network → WS.)

---

# PART 2 — Pipeline Monitoring (Labs 5–7)

## Lab 5 — JMX → Prometheus → Grafana

**Goal:** Expose Kafka broker internals via JMX and visualize them.

**Steps:**

1. Verify Prometheus is scraping every target:
   ```bash
   curl -s http://localhost:9090/api/v1/targets | grep -o '"health":"[^"]*"' | sort -u
   ```
2. Open http://localhost:9090 → **Status → Targets**. All `kafka-brokers` should be UP.
3. Run a few queries in Prometheus:
   ```promql
   sum(rate(kafka_server_brokertopicmetrics_messagesin_total[1m]))
   sum by (topic) (rate(kafka_server_brokertopicmetrics_bytesin_total[1m]))
   kafka_server_replicamanager_underreplicatedpartitions
   ```
4. Open Grafana → http://localhost:3000 → **Dashboards → Taxi Pipeline → Taxi Pipeline – Live Overview**.

**What to observe:**
- Trips/sec, GPS pings/sec, data quality, surge per zone — all live.
- Each broker line under "Bytes In" — load should be balanced across kafka1/2/3.
- Bottom panels: DLQ counts and expectation failures by rule.

**Discussion:**
- Kafka brokers expose 1000+ JMX metrics. Why do we project only the patterns in `jmx_exporter/kafka.yml`?
- Which two metrics would you alert on first in production?

---

## Lab 6 — Consumer-Group Lag Monitoring

**Goal:** Lag is the #1 health signal for a Kafka consumer. Learn to read it.

**Steps:**

1. Check current lag from the CLI:
   ```bash
   docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:29092 \
     --describe --group trip-processor-v1
   ```
2. Confirm `kafka-lag-exporter` is scraping:
   ```promql
   kafka_consumergroup_group_lag{group="trip-processor-v1"}
   ```
3. Generate intentional lag — kill the consumer and let the simulator pile up messages:
   - In Terminal 2 (`taxi_consumer.py`) press Ctrl+C.
   - Watch lag climb in Grafana for ~60 s.
   - Restart it: `python taxi_consumer.py`.
   - Watch lag drain.

**What to observe:**
- Lag chart spikes when the consumer is down, then drains rapidly when it returns.
- Each partition's lag is independent — uneven keys would show uneven lag.

**Discussion:**
- A consumer at lag = 50 000 on a topic doing 5 000 msg/s — how far behind is it in *time*?
- When would lag *never* drain to zero, even on a healthy consumer?

---

## Lab 7 — Structured Logging with Loki

**Goal:** Treat logs like metrics — searchable, filterable, correlated with dashboards.

**Steps:**

1. In Grafana, go to **Explore** and switch the datasource to **Loki**.
2. Query container logs (LogQL):
   ```logql
   {container="kafka1"}                              # broker logs
   {container=~"kafka.*"} |= "ERROR"                 # errors across brokers
   {container="taxi-dashboard"} | json               # parsed JSON logs
   ```
3. Correlate with metrics: filter for the time range a surge spike happened.

**What to observe:**
- Logs from all containers ship to Loki via Promtail with no agent install.
- LogQL syntax mirrors PromQL — same mental model.

**Discussion:**
- Why is Loki cheaper to run than Elasticsearch for high-volume container logs?
- What is the trade-off?

---

# PART 3 — Error Handling & Data Quality (Labs 8–10)

## Lab 8 — Error Handling + Dead Letter Queue

**Goal:** Bad data is inevitable. Don't let it crash the pipeline — quarantine it.

**Steps:**

1. The simulator injects ~1.5 % corrupted records on purpose (rate set in `config.json`). The consumer routes them to `trips-dlq`.
2. Inspect the DLQ:
   ```bash
   python dlq_tool.py --mode inspect --limit 100
   ```
   Expected output:
   ```
   --- DLQ inspection (last 100) ---
     34  neg_fare
     22  outlier_dist
     18  missing_driver_id
     16  bad_coord
     10  future_ts
   ```
3. The DLQ count on the live dashboard ticks up in real time. The right-hand DLQ table shows the most recent bad records.
4. Replay the DLQ (e.g. after fixing the downstream bug):
   ```bash
   python dlq_tool.py --mode replay
   ```

**What to observe:**
- The pipeline never crashes — even on garbage input.
- DLQ has 24 h retention → you have time to triage.
- Replay re-publishes to `taxi-trips`, NOT directly to `trips-clean`, so validation runs again.

**Discussion:**
- Why route to `trips-dlq` instead of just logging and dropping?
- What metadata should a DLQ message always carry? (We add `_dlq_reason`, `_dlq_ts`.)

---

## Lab 9 — Idempotent Producers, Retries, Backoff

**Goal:** Configure a producer for **at-least-once with deduplication**.

**Steps:**

1. Look at the producer config in [taxi_simulator.py](taxi_simulator.py):
   ```python
   {"acks": "all",
    "enable.idempotence": True,
    "compression.type": "snappy",
    "linger.ms": 50,
    "batch.size": 32 * 1024}
   ```
2. Force broker failure mid-stream and watch retries:
   ```bash
   # leave taxi_simulator.py running, then in another terminal:
   docker pause kafka2
   # watch the simulator logs — retries kick in, no data lost
   docker unpause kafka2
   ```
3. In Kafka UI → **Brokers**, you'll see kafka2 leave and rejoin the ISR (In-Sync Replicas).

**What to observe:**
- With `acks=all` + `min.insync.replicas=2`, the producer waits until ≥2 brokers ack each write.
- With `enable.idempotence=true`, retries don't create duplicates.
- The simulator pauses briefly during failover, then resumes — no message loss.

**Discussion:**
- What changes if you set `acks=1`?
- When is `enable.idempotence=false` acceptable?

---

## Lab 10 — Expectation-Based Validation on a Streaming Pipeline

**Goal:** Apply the same kind of declarative data-quality rules you'd use in batch analytics (Great Expectations / Soda / Deequ style) to micro-batches off Kafka — using a tiny in-house validator so the mechanics are visible end-to-end.

**Quality parameters in this lab** (all defined in [config.json](config.json) → `quality` block):

| Column          | Rule                  | Bounds                                   |
| --------------- | --------------------- | ---------------------------------------- |
| `trip_id`       | not null              | —                                        |
| `driver_id`     | not null              | —                                        |
| `fare_amount`   | between               | 0 .. 1000                                |
| `distance_miles`| between               | 0 .. 150                                 |
| `passenger_count`| between              | 1 .. 8                                   |
| `payment_type` | in set               | `credit_card`, `cash`, `mobile`, `corporate` |
| `pickup_lat`    | between (NYC bbox)    | 40.4 .. 41.0                             |
| `pickup_lon`    | between (NYC bbox)    | -74.3 .. -73.5                           |

**Steps:**

1. Watch the validator's console output — it prints batch results every 25 records:
   ```
   batch=25 score=87.5% passed=7/8
   FAIL distance_in_range  sample=[9999.0]
   ```
2. In Grafana, the **Data Quality Score** tile reflects the `data_quality_score` gauge.
3. Try a **stricter suite** — edit `config.json` → `quality.passenger_max: 4`, restart `quality_validator.py`, watch the score drop.
4. Look at per-rule counters in Prometheus:
   ```promql
   sum by (expectation) (gx_expectations_failed_total)
   ```

**What to observe:**
- Each rule emits its own `gx_expectations_passed_total{expectation="..."}` and `_failed_` counter.
- Rows that fail any rule are routed to `trips-dlq` with the rule name preserved as `_dlq_reason`.
- Changing a threshold needs **zero code edits** — only `config.json`.

**Discussion:**
- Streaming vs batch validation: what changes? (Here we validate micro-batches in memory; no checkpoint store, no validation history written back.)
- Why an in-house validator instead of Great Expectations? GX adds a heavy dependency chain that doesn't always cleanly support newer Python interpreters. The 30-line `Expectation` API in [quality_validator.py](quality_validator.py) keeps the **idea** of declarative expectations without the install pain.
- When would you reject the **whole batch** vs reject **individual rows**?

---

# PART 4 — Stream Processing & Alerting (Labs 11–12)

## Lab 11 — Windowed Aggregations: Surge Pricing in Python

**Goal:** Implement a real Kafka-Streams-style tumbling-window aggregation in pure Python so the logic is *visible*.

**Steps:**

1. Read [surge_detector.py](surge_detector.py). The window is 30 s, slide = 30 s (tumbling).
2. Watch it print:
   ```
   zone=TIMES_SQUARE  demand=14 supply= 2 surge=2.00x
   zone=JFK_AIRPORT   demand= 6 supply= 1 surge=1.60x
   zone=MIDTOWN       demand= 4 supply= 5 surge=1.00x
   ```
3. Confirm the `surge-events` topic is being written (it is a **compacted** topic — only the latest surge value per zone is kept):
   ```bash
   docker exec kafka1 kafka-console-consumer \
     --bootstrap-server kafka1:29092 \
     --topic surge-events \
     --from-beginning --max-messages 5 \
     --property print.key=true
   ```
4. Open the live dashboard — zone circles change colour based on the published surge multiplier.

**What to observe:**
- The `compute_surge(demand, supply)` function is the "business logic". Same shape as a Kafka Streams `aggregate()` call in Java.
- Tumbling = non-overlapping. Sliding = overlapping.

**Discussion:**
- Where would you use a *sliding* window instead of tumbling?
- What happens to a zone's surge value at midnight if demand drops to 0? (The compacted topic keeps the last value forever unless we publish a new one.)

---

## Lab 12 — ksqlDB: Same Logic as SQL + Alerting

**Goal:** Express stream processing as **SQL** that runs continuously inside Kafka, and route metric breaches to Alertmanager.

### Part A — ksqlDB

ksqlDB and Schema Registry are already running. The taxi-flavored statements are in [ksql_taxi.sql](ksql_taxi.sql).

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```
Inside the `ksql>` prompt paste each statement from `ksql_taxi.sql` one at a time:

```sql
SET 'auto.offset.reset' = 'earliest';

CREATE STREAM trips_raw (...) WITH (KAFKA_TOPIC='taxi-trips', VALUE_FORMAT='JSON');

SHOW STREAMS;
SHOW TABLES;

-- Push query: latest surge trips
SELECT * FROM surge_trips EMIT CHANGES LIMIT 5;

-- Pull query: latest revenue per zone
SELECT pickup_zone, trip_count, revenue
FROM revenue_per_zone_per_minute
WHERE pickup_zone='TIMES_SQUARE';
```

### Part B — Alerting

Open [alerts.yml](alerts.yml) — five rules already defined:
- `HighConsumerLag` (lag > 1000)
- `BrokerDown`
- `UnderReplicatedPartitions`
- `HighErrorRate` (DLQ rate > 5/s)
- `SurgeActive` (any zone > 1.5 × for 30 s)

Trigger `HighConsumerLag` — kill the consumer for ~2 minutes, then check:
- http://localhost:9090/alerts (Prometheus side)
- http://localhost:9093 (Alertmanager side)

**What to observe:**
- ksqlDB writes its output to new Kafka topics (`surge_trips`, `taxi_zone_revenue_1m`, …).
- TUMBLING vs HOPPING vs SESSION windows produce visibly different table cardinalities.
- Alerts transition: `inactive → pending (during 'for:' interval) → firing`.

**Discussion:**
- When is ksqlDB the right tool, vs. Kafka Streams (Java) or a custom Python consumer?
- Why does an alert have a `for:` duration? Why not fire immediately?

---

# PART 5 — Scaling (Labs 13–15)

## Lab 13 — Horizontal Scaling: Partitions & Consumers

**Goal:** Scale a consumer group by adding more instances, then more partitions.

**Steps:**

1. With one consumer running, look at partition assignment:
   ```bash
   docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:29092 \
     --describe --group trip-processor-v1
   ```
   One consumer owns all 6 partitions.
2. In a new terminal (activate `.venv`), start a second consumer in the same group:
   ```bash
   python taxi_consumer.py
   ```
   Re-describe — partitions are now 3+3.
3. Start a third → 2+2+2. Start a 4th → one consumer is **idle** (more consumers than partitions).
4. Add partitions on the fly:
   ```bash
   docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 \
     --alter --topic taxi-trips --partitions 12
   ```
   Now all 4 consumers get work (3+3+3+3).

**What to observe:**
- Partition count is the **upper limit** on parallelism inside a single consumer group.
- Rebalances appear in consumer logs: `Revoking partitions` → `Assigning partitions`.

**Discussion:**
- Why can you never *decrease* partitions on a topic?
- What happens to ordering guarantees if you change the partitioner?

---

## Lab 14 — Vertical Scaling: Add a Broker, Rebalance Partitions

**Goal:** Grow the cluster from 3 → 4 brokers and move data over.

**Steps:**

1. Add a 4th broker. Edit [docker-compose.yml](docker-compose.yml) and append a `kafka4` service modelled on `kafka3`:
   - `KAFKA_NODE_ID: 4`
   - Add `4@kafka4:9093` to `KAFKA_CONTROLLER_QUORUM_VOTERS` **on all four nodes**.
   - Map host port `9096:9096` and set `EXTERNAL://0.0.0.0:9096`.
2. Bring it up:
   ```bash
   docker compose up -d kafka4
   ```
3. Generate a reassignment plan.

   **Linux/macOS/WSL:**
   ```bash
   cat > reassign-topics.json <<'JSON'
   {"topics":[{"topic":"taxi-trips"}],"version":1}
   JSON
   ```
   **Windows cmd:**
   ```cmd
   echo {"topics":[{"topic":"taxi-trips"}],"version":1} > reassign-topics.json
   ```

   Then:
   ```bash
   docker cp reassign-topics.json kafka1:/tmp/reassign-topics.json
   docker exec kafka1 kafka-reassign-partitions \
     --bootstrap-server kafka1:29092 \
     --topics-to-move-json-file /tmp/reassign-topics.json \
     --broker-list "1,2,3,4" --generate
   # Copy the "Proposed partition reassignment" JSON block, save as reassignment.json, then:
   docker cp reassignment.json kafka1:/tmp/reassignment.json
   docker exec kafka1 kafka-reassign-partitions \
     --bootstrap-server kafka1:29092 \
     --reassignment-json-file /tmp/reassignment.json --execute
   docker exec kafka1 kafka-reassign-partitions \
     --bootstrap-server kafka1:29092 \
     --reassignment-json-file /tmp/reassignment.json --verify
   ```

**What to observe:**
- During reassignment, "Bytes Out" on brokers 1/2/3 spikes (they're shipping data to broker 4).
- `UnderReplicatedPartitions` is briefly > 0, then returns to 0.

**Discussion:**
- Why doesn't Kafka rebalance partitions automatically when you add a broker?
- What is **Cruise Control** and how would it automate this?

---

## Lab 15 — Throughput Tuning + Broker-Failure Resilience

**Goal:** Measure how producer settings change throughput, then prove the cluster survives a broker loss.

### Part A — Throughput tuning

1. Baseline:
   ```bash
   python load_test.py --workers 4 --rate 2000 --duration 30
   ```
   Note the achieved `rate=X/s` per worker.
2. Edit `load_test.py` producer config and re-run, varying one setting at a time:

   | Config             | Try                                   |
   | ------------------ | ------------------------------------- |
   | `linger.ms`        | 0 vs 20 vs 100                        |
   | `batch.size`       | 16K vs 64K vs 256K                    |
   | `compression.type` | `none` vs `snappy` vs `lz4` vs `zstd` |
   | `acks`             | `0`, `1`, `all`                       |

3. For each run capture: throughput (msg/s), `taxi_produce_latency_seconds` p99, broker Bytes In (Grafana).

### Part B — Broker failure

1. With the full pipeline running, kill broker 2:
   ```bash
   docker kill kafka2
   ```
2. In Kafka UI → **Brokers**, kafka2 is "offline". Topics → `taxi-trips` shows new leaders for the partitions that were on kafka2.
3. The Prometheus alert `UnderReplicatedPartitions` fires after 1 min.
4. Watch the live dashboard — taxi trips continue without interruption.
5. Bring broker 2 back:
   ```bash
   docker start kafka2
   ```
6. Watch the under-replicated count return to 0 as kafka2 re-syncs.

**What to observe:**
- `linger.ms > 0` + larger `batch.size` ⇒ higher throughput, slightly higher latency.
- `compression.type=zstd` typically wins on text-heavy payloads.
- `acks=0` is dangerously fast — data loss on broker failure.
- KRaft controllers handle leader election faster than the old Zookeeper-based controller.
- No messages lost because `min.insync.replicas=2` and `acks=all`.

**Discussion:**
- Where is the bottleneck on your laptop — CPU, network, or disk?
- What would happen if you killed **two** brokers simultaneously on this 3-broker cluster?

---

## Wrap-Up / Capstone Questions

After all 15 labs you should be able to answer:

1. Name three things you would alert on for a Kafka pipeline in production.
2. Describe the path of a single bad record from producer → DLQ → replay.
3. Walk through what happens when you add a partition to a topic with 3 active consumers.
4. Why KRaft over Zookeeper? Give two operational reasons.
5. How does CDC change your architecture compared to periodic batch sync?
6. Where would Kubernetes / Minikube actually help here that Docker Compose doesn't?

## When to use Kubernetes (the Minikube question)

You do **not** need Minikube for these labs — Docker Compose handles multi-broker, scaling consumers, and broker failure scenarios on a single machine.

Use Kubernetes / Helm (e.g. the **Strimzi** operator) when you want:
- **Self-healing**: pods restart automatically on node failure.
- **Autoscaling**: HPA / KEDA scales consumer pods based on Kafka lag.
- **Multi-node distribution**: brokers spread across physical nodes for true HA.
- **Rolling upgrades**: rebalance + restart brokers one at a time without downtime.

Minikube is good for *learning* Kubernetes; for production use a real cluster (EKS / GKE / AKS) with Strimzi or Confluent Operator.

---

## Cleanup

```bash
docker compose down -v
deactivate     # leave the Python venv
```

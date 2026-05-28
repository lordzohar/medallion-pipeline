# Day 01 Lab Report

Course: Modern Data Engineering with Medallion Pipelines  
Date completed: 2026-05-28  
Workspace: `/home/ubuntu/Downloads/Medallion-pipeline/Week_01/Day_01/Lab_Files`

## Overall Result

Status: SUCCESS

Day 01 was completed using the provided local Python lab files. The two failure commands in the manual were performed and behaved as expected because they simulate dependency outages. Final validation passed.

## Lab 0 - Prepare Ubuntu Learning Host

Status: SUCCESS

Command run:

```bash
bash setup/verify_environment.sh
```

Evidence file:

```text
Week_01/Day_01/Lab_Files/out/environment_report.txt
```

Key results:

```text
PASS python3: Python 3.10.12
PASS git: git version 2.34.1
PASS curl: curl 7.81.0
PASS docker: Docker version 29.5.1
PASS docker compose: Docker Compose version v5.1.3
PASS TCP 8080 available
PASS TCP 8083 available
PASS TCP 8088 available
PASS TCP 9000 available
PASS TCP 9092 available
```

Observation: Docker Compose is already installed, so I did not install it. Day 1 itself does not need Docker services; it only verifies that the host is ready for later Kafka and platform labs.

Reflection: Installing Docker today is prerequisite management. Running Kafka today would add extra service complexity before the first data engineering concepts are clear.

## Lab 1 - Tightly Coupled Failure

Status: SUCCESS, with expected simulated failure

Commands run:

```bash
python3 src/reset_outputs.py
python3 src/tightly_coupled_order.py --fail-notification
python3 src/tightly_coupled_order.py
cat out/tight/committed_orders.csv
cat out/tight/notifications.txt
```

Evidence:

```text
Order API received a valid order.
FAILED: notification service is unavailable; the order was not committed.
Observation: a secondary dependency blocked primary work.

Order API received a valid order.
SUCCESS: notification sent and order committed.
```

Committed order:

```text
order_id,customer_id,amount,submitted_at
O-LIVE-001,C-041,149.90,2026-05-28T15:20:36.400718+00:00
```

Observation answers:

- A valid order was not committed during the simulated notification outage.
- The notification component controlled the final success decision because the order API waited synchronously for it.
- This coupling is acceptable only when the dependent action is truly required before the business transaction can complete.
- As receivers grow, the order API inherits more failure points and slower response time.

## Lab 2 - Loosely Coupled Event Notification

Status: SUCCESS, with expected simulated consumer failure

Commands run:

```bash
python3 src/event_bus_pipeline.py produce
python3 src/event_bus_pipeline.py consume --fail-notification
python3 src/event_bus_pipeline.py consume
python3 src/event_bus_pipeline.py consume
cat out/events/order_notifications.jsonl
cat out/events/notification_checkpoint.json
cat out/events/operations_notifications.txt
```

Evidence:

```text
PRODUCED ev-live-001: source work is durably recorded
The producer does not need a notification consumer to be online.
CONSUMER FAILED: operations notification endpoint is unavailable.
Event remains in the durable inbox; rerun consume after recovery.
ACKNOWLEDGED ev-live-001; checkpoint written after successful action.
No pending notifications. Existing event IDs are already checkpointed.
```

Checkpoint:

```json
{
  "completed_ids": [
    "ev-live-001"
  ]
}
```

Observation: The producer did not need to wait for the consumer to recover. The event was stored first, then the consumer retried and checkpointed after successful processing.

Reflection: A real system still needs authentication, schema compatibility, replay control, duplicate handling, and monitoring. The fact is the business event that happened; the notification is the durable message telling other systems about that fact.

## Lab 3 - Batch and Streaming Comparison

Status: SUCCESS

Commands run:

```bash
python3 src/batch_pipeline.py
cat out/batch/product_summary.csv
python3 src/stream_pipeline.py --delay-seconds 0
cat out/stream/rolling_state.json
cat out/stream/alerts.txt
```

Batch evidence:

```text
product,submitted_orders,cancelled_orders,net_value,batch_processed_at
cable,1,0,24.00,2026-05-28T15:20:36.588233+00:00
dock,1,0,98.75,2026-05-28T15:20:36.588233+00:00
headset,1,0,134.00,2026-05-28T15:20:36.588233+00:00
keyboard,1,0,42.50,2026-05-28T15:20:36.588233+00:00
monitor,1,1,210.00,2026-05-28T15:20:36.588233+00:00
```

Streaming evidence:

```json
{
  "last_event_id": "ev-1006",
  "events_processed": 6,
  "net_value_by_product": {
    "cable": "24.00",
    "dock": "98.75",
    "headset": "134.00",
    "keyboard": "42.50",
    "monitor": "210.00"
  }
}
```

Alert evidence:

```text
2026-05-28T15:20:36.626221+00:00 REVIEW cancellation for O-1005
```

Comparison:

| Dimension | Batch observation | Streaming observation |
| --- | --- | --- |
| Input boundary | Closed CSV input set | Events handled one by one |
| Earliest visible result | After complete batch finishes | As soon as each event is processed |
| Cancellation | Included in final batch summary | Alert appears immediately |
| Failure/restart | Rerun complete batch | Need checkpoint/state handling |
| Reason to choose | Stable periodic totals | Low-latency operational reaction |

## Lab 4 - Medallion Data Product

Status: SUCCESS

Commands run:

```bash
python3 src/medallion_pipeline.py
find out/lake -type f -maxdepth 5 -print
head -n 2 out/lake/bronze/order_events/events.jsonl
cat out/lake/silver/orders/orders_current.csv
cat out/lake/gold/daily_product_sales/product_kpis.csv
```

Generated layers:

```text
out/lake/bronze/order_events/events.jsonl
out/lake/silver/orders/orders_current.csv
out/lake/gold/daily_product_sales/product_kpis.csv
```

Gold evidence:

```text
product,order_count,net_sales
cable,1,24.00
dock,1,98.75
headset,1,134.00
keyboard,1,42.50
monitor,1,420.00
```

Investigation answers:

- Bronze keeps the raw notification so the original evidence can be replayed, audited, or reprocessed later.
- Silver cleaning includes typed values, consistent columns, current order state, and explicit status.
- Gold excludes the cancelled adjustment because its metric is submitted non-cancelled product sales.
- The streaming alert and Gold KPI are both valid because they answer different questions. Clear metric names remove ambiguity.
- A product category dimension would be joined after Silver and before or inside the Gold serving layer.

## Lab 5 - Model and Design

Status: COMPLETED AS WRITTEN DESIGN

Chosen grain: one current accepted order line for Gold reporting.

Model sketch:

| Object | Grain / definition | Key fields |
| --- | --- | --- |
| `fact_order_line` | One current accepted order line | `order_id`, `customer_id`, `product_id`, `date_id`, `quantity`, `net_sales` |
| `dim_customer` | One customer version | `customer_id`, customer attributes |
| `dim_product` | One product/version | `product_id`, `product_name`, `category` |
| `dim_date` | One calendar day | `date_id`, day, month, quarter, year |

Future-tool map:

| Responsibility | Day 1 stand-in | Later implementation |
| --- | --- | --- |
| Durable stream of notifications | JSONL inbox | Kafka topic |
| Capture committed database changes | Hand-authored sample events | Debezium CDC |
| Reusable transformations | Python script | Spark, Hop, SQL, or orchestration-managed jobs |
| Scheduling/monitoring bounded tasks | Manual terminal execution | Airflow |
| Object/lake storage | `out/lake/` directories | MinIO / object storage |
| BI consumption | CSV inspection | BI dashboard or serving table |

Boundary notes:

- Synchronous operation: tightly coupled order API waiting for notification.
- Asynchronous notification: event inbox and consumer flow.
- Raw replayable history: Bronze layer.
- Schema contract needed: event envelope and Silver output.
- Duplicate risk: event replay or consumer retry.
- Operator signal: checkpoint, alerts, task logs, and monitoring metrics.

## Final Validation

Status: SUCCESS

Command run:

```bash
python3 src/validate_outputs.py
```

Result:

```text
Day 1 Lab Validation
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

## Final Statement

A producer should not need to know every consumer. Day 1 showed that a durable notification allows work to be recorded first, then consumed independently. Bronze keeps replayable evidence, Silver creates a consistent business shape, and Gold answers a defined reporting question.

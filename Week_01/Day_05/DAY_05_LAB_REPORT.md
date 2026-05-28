# Day 05 Lab Report

Course: Modern Data Engineering with Medallion Pipelines  
Date completed: 2026-05-29  
Workspace: `/home/ubuntu/Downloads/Medallion-pipeline/Week_01/Day_05/Lab_Files`

## Overall Result

Status: SUCCESS WITH DOCUMENTED RUNTIME DEVIATIONS

Day 05 was performed with PostgreSQL CDC, MySQL CDC, clickstream events, CSV ingestion, and DLQ routing. Kafka UI and Schema Registry were running. The runnable stack used JSON converters instead of Avro converters because the `debezium/connect:2.5` image did not provide Confluent Avro converter classes.

## Setup Notes

The supplied stack needed these runtime corrections:

- Kafka was changed to advertise both internal and host listeners:
  - internal: `kafka:29092`
  - host: `localhost:9092`
- Kafka Connect bootstrap was changed to `kafka:29092`.
- Schema Registry bootstrap was changed to `kafka:29092`.
- PostgreSQL image was changed to `debezium/postgres:15` so logical decoding works.
- Kafka Connect converters were changed from Confluent Avro converters to JSON converters.
- MySQL connector schema history settings were updated for Debezium 2.x:
  - `schema.history.internal.kafka.bootstrap.servers`
  - `schema.history.internal.kafka.topic`
- MySQL replication privileges were granted to the `debezium` user.
- `csv_producer.py` was fixed so DLQ messages can be sent without a key.

Schema Registry note before the Avro producer:

```text
curl http://localhost:8081/subjects
[]
```

This was expected before the practical Avro step because the CDC connectors used JSON converters. The CDC messages still include inline schema fields, but they are not Avro schemas registered in Schema Registry.

Practical Schema Registry / Avro evidence:

To make Schema Registry part of a real pipeline, I added a practical Avro producer and consumer:

```text
avro_logistics_producer.py
avro_logistics_consumer.py
```

The producer reads `logistics.csv`, validates rows, serializes valid records as Avro, registers the schema through Schema Registry, and writes to Kafka topic `northstar.logistics.avro`.

Command:

```bash
python3 avro_logistics_producer.py
```

Producer evidence:

```text
Produced Avro record: {'shipment_id': '1001', 'shipment_status': 'IN_TRANSIT', 'updated_at': '2024-01-10T12:00:00Z'}
Produced Avro record: {'shipment_id': '1002', 'shipment_status': 'DELAYED', 'updated_at': '2024-01-11T14:00:00Z'}
Produced Avro record: {'shipment_id': '1003', 'shipment_status': 'DELIVERED', 'updated_at': '2024-01-12T16:30:00Z'}
Skipped invalid row for Avro topic: {'shipment_id': '1004', 'shipment_status': '', 'updated_at': '2024-01-13T10:00:00Z'}
Produced Avro record: {'shipment_id': '1005', 'shipment_status': 'IN_TRANSIT', 'updated_at': '2026-05-28T18:44:04.327513Z'}
Finished producing Avro records to northstar.logistics.avro.
```

Subjects after Avro producer:

```json
[
  "northstar.logistics.avro-value"
]
```

Latest registered schema:

```json
{
  "subject": "northstar.logistics.avro-value",
  "version": 1,
  "id": 2,
  "schema": "{\"type\":\"record\",\"name\":\"LogisticsUpdate\",\"namespace\":\"northstar.logistics\",\"fields\":[{\"name\":\"shipment_id\",\"type\":\"string\"},{\"name\":\"shipment_status\",\"type\":\"string\"},{\"name\":\"updated_at\",\"type\":\"string\"}]}"
}
```

Consumer command:

```bash
python3 avro_logistics_consumer.py
```

Consumer evidence:

```text
Consumed Avro record key=1001 partition=0 offset=0 value={'shipment_id': '1001', 'shipment_status': 'IN_TRANSIT', 'updated_at': '2024-01-10T12:00:00Z'}
Consumed Avro record key=1002 partition=0 offset=1 value={'shipment_id': '1002', 'shipment_status': 'DELAYED', 'updated_at': '2024-01-11T14:00:00Z'}
Consumed Avro record key=1003 partition=0 offset=2 value={'shipment_id': '1003', 'shipment_status': 'DELIVERED', 'updated_at': '2024-01-12T16:30:00Z'}
Consumed Avro record key=1005 partition=0 offset=3 value={'shipment_id': '1005', 'shipment_status': 'IN_TRANSIT', 'updated_at': '2026-05-28T18:44:04.327513Z'}
Consumed 4 Avro records from northstar.logistics.avro.
```

## Task 1 - Start Environment

Command:

```bash
sudo docker compose up -d
sudo docker compose ps
```

Service evidence:

```text
connect           debezium/connect:2.5                    Up
kafka             confluentinc/cp-kafka:7.5.0             Up (healthy)
kafka-ui          provectuslabs/kafka-ui:latest           Up
mysql             mysql:8.0                               Up
postgres          debezium/postgres:15                    Up
schema-registry   confluentinc/cp-schema-registry:7.5.0   Up
zookeeper         confluentinc/cp-zookeeper:7.5.0         Up
```

Kafka UI:

```text
http://localhost:8080
```

## Task 2 - Register Debezium Connectors

Commands:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @orders-connector.json \
  http://localhost:8083/connectors

curl -X POST -H "Content-Type: application/json" \
  --data @inventory-connector.json \
  http://localhost:8083/connectors
```

Connector status:

```json
{
  "name": "orders-connector",
  "connector": { "state": "RUNNING" },
  "tasks": [{ "id": 0, "state": "RUNNING" }],
  "type": "source"
}
```

```json
{
  "name": "inventory-connector",
  "connector": { "state": "RUNNING" },
  "tasks": [{ "id": 0, "state": "RUNNING" }],
  "type": "source"
}
```

Topics:

```text
northstar.orders.public.orders
northstar.inventory.inventorydb.inventory
schema-changes.inventory
```

## Task 3 - CDC Events

PostgreSQL source change:

```bash
sudo docker compose exec postgres psql -U debezium -d ordersdb -c \
  "INSERT INTO orders (customer_id, status) VALUES (4, 'NEW');"
```

Evidence:

```text
INSERT 0 1
```

Kafka order CDC evidence:

```json
{
  "payload": {
    "before": null,
    "after": {
      "id": 4,
      "customer_id": 4,
      "status": "NEW"
    },
    "op": "c"
  }
}
```

MySQL source change:

```bash
sudo docker compose exec mysql mysql -udebezium -pdbz -e \
  "UPDATE inventorydb.inventory SET quantity = quantity - 10 WHERE product_name = 'Widget A';"
```

Evidence:

```text
Widget A quantity changed to 90
```

Kafka inventory CDC evidence after granting replication privileges and restarting the connector:

```json
{
  "payload": {
    "before": null,
    "after": {
      "id": 1,
      "product_name": "Widget A",
      "quantity": 90
    },
    "op": "r"
  }
}
```

## Task 4 - Clickstream Producer

Command:

```bash
timeout 6 python3 -u clickstream_producer.py
```

Producer evidence:

```text
Produced event: {'user_id': 'd1dd8f0b-4ef0-410f-b0c5-dd07ed5b1f92', 'page': 'product', 'event_time': '2026-05-28T18:37:28.109528Z'}
Produced event: {'user_id': '3553b6e2-560e-48c1-a293-13de3f208739', 'page': 'cart', 'event_time': '2026-05-28T18:37:29.325853Z'}
Produced event: {'user_id': 'c0cadc38-ab35-40f7-a298-3904428817d8', 'page': 'home', 'event_time': '2026-05-28T18:37:30.328424Z'}
```

Kafka topic evidence:

```json
{"user_id": "d1dd8f0b-4ef0-410f-b0c5-dd07ed5b1f92", "page": "product", "event_time": "2026-05-28T18:37:28.109528Z"}
```

## Task 5 - CSV Producer and DLQ

Original issue found:

```text
AttributeError: 'NoneType' object has no attribute 'encode'
```

Cause: DLQ records were sent without a key, but `key_serializer` assumed every key was a string.

Fix:

```python
key_serializer=lambda k: k.encode("utf-8") if k is not None else None
```

Command:

```bash
python3 csv_producer.py
```

Producer evidence:

```text
Sent valid record: {'shipment_id': '1001', 'shipment_status': 'IN_TRANSIT', 'updated_at': '2024-01-10T12:00:00Z'}
Sent valid record: {'shipment_id': '1002', 'shipment_status': 'DELAYED', 'updated_at': '2024-01-11T14:00:00Z'}
Sent valid record: {'shipment_id': '1003', 'shipment_status': 'DELIVERED', 'updated_at': '2024-01-12T16:30:00Z'}
Sent to DLQ: {'error': 'Missing required fields', 'row': {'shipment_id': '1004', 'shipment_status': '', 'updated_at': '2024-01-13T10:00:00Z'}, 'timestamp': '2024-01-13T10:00:00Z'}
Sent valid record: {'shipment_id': '1005', 'shipment_status': 'IN_TRANSIT', 'updated_at': '2026-05-28T18:38:19.850744Z'}
Finished sending CSV records.
```

Valid CSV topic evidence:

```json
{"shipment_id": "1001", "shipment_status": "IN_TRANSIT", "updated_at": "2024-01-10T12:00:00Z"}
{"shipment_id": "1002", "shipment_status": "DELAYED", "updated_at": "2024-01-11T14:00:00Z"}
{"shipment_id": "1003", "shipment_status": "DELIVERED", "updated_at": "2024-01-12T16:30:00Z"}
{"shipment_id": "1005", "shipment_status": "IN_TRANSIT", "updated_at": "2026-05-28T18:38:19.850744Z"}
```

DLQ evidence:

```json
{
  "error": "Missing required fields",
  "row": {
    "shipment_id": "1004",
    "shipment_status": "",
    "updated_at": "2024-01-13T10:00:00Z"
  },
  "timestamp": "2024-01-13T10:00:00Z"
}
```

## Task 6 - Generic Consumer

Command:

```bash
timeout 10 python3 -u consumer.py \
  northstar.orders.public.orders \
  northstar.inventory.inventorydb.inventory \
  northstar.clickstream.events \
  northstar.logistics.csv \
  northstar.ingestion.dlq
```

Evidence: the generic consumer printed records from all five requested topics.

## Final Topic List

```text
__consumer_offsets
_schemas
connect-configs
connect-offsets
connect-status
northstar.clickstream.events
northstar.ingestion.dlq
northstar.inventory.inventorydb.inventory
northstar.logistics.csv
northstar.orders.public.orders
schema-changes.inventory
```

## Mini Exercises

Ingestion risks:

- CDC replay can create duplicate records unless consumers are idempotent.
- Ordering is only guaranteed per key/partition.
- Schema changes can break consumers if compatibility is not managed.
- CSV and clickstream sources can emit malformed or late records.

Schema changes:

- Adding optional `shipping_address`: backward-compatible.
- Removing non-null `status`: breaking.
- Changing `quantity` from `INT` to `BIGINT`: potentially breaking for strict consumers; should be versioned and tested.

DLQ strategy for invalid clickstream:

- Include original payload, validation error, event time, ingestion time, source topic, producer name, key, and retry/correlation ID.

Late/out-of-order strategy:

- Use event-time processing, watermarks, keyed ordering, deduplication IDs, and a grace period before finalizing aggregates.

MCQ answers:

1. B
2. B
3. C
4. A
5. B
6. B

## Final Statement

Day 05 demonstrated an end-to-end ingestion pipeline with CDC from PostgreSQL and MySQL, host-generated clickstream events, CSV ingestion, invalid-record DLQ routing, and multi-topic consumption. The only material deviation is Schema Registry: it is running, but this validated run used JSON converters, so no Avro subjects were registered.

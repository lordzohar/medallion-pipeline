# Day 04 Lab Report

Course: Modern Data Engineering with Medallion Pipelines  
Date completed: 2026-05-28  
Workspace: `/home/ubuntu/Downloads/Medallion-pipeline/Week_01/Day_04/Lab_Files`

## Overall Result

Status: SUCCESS

Day 04 was performed with PostgreSQL, Kafka, Kafka Connect, and the Debezium PostgreSQL connector. Initial snapshot records, live insert/update CDC events, and a Debezium signaling request were all observed.

## Setup Notes

The original compose file referenced `debezium/connect:3.5`, which was not available from Docker Hub during the lab run. I pinned the stack to available course-compatible images:

```text
confluentinc/cp-zookeeper:7.5.0
confluentinc/cp-kafka:7.5.0
debezium/connect:2.5
debezium/postgres:15
```

Day 3 containers were stopped first to avoid port conflicts on `9092` and `8083`.

## Task 1 - Start Services

Command:

```bash
sudo docker compose up -d
sudo docker compose ps
curl -s http://localhost:8083/connector-plugins
```

Service evidence:

```text
NAME                    IMAGE                             SERVICE     STATUS
lab_files-connect-1     debezium/connect:2.5              connect     Up
lab_files-kafka-1       confluentinc/cp-kafka:7.5.0       kafka       Up
lab_files-postgres-1    debezium/postgres:15              postgres    Up
lab_files-zookeeper-1   confluentinc/cp-zookeeper:7.5.0   zookeeper   Up
```

Connector plugin evidence:

```text
io.debezium.connector.postgresql.PostgresConnector
```

Mini-exercise answer:

- PostgreSQL owns the database transaction log / WAL.
- Debezium running inside Kafka Connect converts committed row changes into Kafka records.

## Task 2 - Register Debezium Connector

Command:

```bash
curl -i -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @postgres-connector.json
```

Registration evidence:

```text
HTTP/1.1 201 Created
```

Connector status:

```json
{
  "name": "inventory-connector",
  "connector": {
    "state": "RUNNING",
    "worker_id": "172.18.0.5:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "172.18.0.5:8083"
    }
  ],
  "type": "source"
}
```

Topics created:

```text
__consumer_offsets
__debezium-heartbeat.inventory-raw
debezium_connect_configs
debezium_connect_offsets
debezium_connect_status
inventory.public.customers-raw
```

## Task 3 - Initial Snapshot and CDC Events

Command:

```bash
sudo docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic inventory.public.customers-raw \
  --from-beginning \
  --group day4-snapshot \
  --timeout-ms 7000
```

Initial snapshot evidence:

```json
{"payload":{"id":1,"name":"Alice","email":"alice@example.com","balance":"JxA="}}
{"payload":{"id":2,"name":"Bob","email":"bob@example.com","balance":"TiA="}}
{"payload":{"id":3,"name":"Carol","email":"carol@example.com","balance":"Opg="}}
```

Commands for live changes:

```bash
sudo docker compose exec postgres psql -U postgres -d inventory -c \
  "INSERT INTO customers(name,email,balance) VALUES ('Dina','dina@example.com',325.00);"

sudo docker compose exec postgres psql -U postgres -d inventory -c \
  "UPDATE customers SET balance=410.00 WHERE email='dina@example.com';"
```

PostgreSQL evidence:

```text
INSERT 0 1
UPDATE 1
```

Kafka evidence after insert and update:

```json
{"payload":{"id":4,"name":"Dina","email":"dina@example.com","balance":"fvQ="}}
{"payload":{"id":4,"name":"Dina","email":"dina@example.com","balance":"AKAo"}}
```

Observation: The initial snapshot captured Alice, Bob, and Carol. The later insert and update for Dina appeared in Kafka without restarting the connector.

## Task 4 - Incremental Snapshot by Signaling

Command:

```bash
sudo docker compose exec postgres psql -U postgres -d inventory -c \
  "INSERT INTO debezium_signal(id,type,data) VALUES ('snap-01','execute-snapshot','{\"data-collections\":[\"public.customers\"],\"type\":\"incremental\"}');"
```

PostgreSQL evidence:

```text
INSERT 0 1
```

Connector log evidence:

```text
Requested 'INCREMENTAL' snapshot of data collections '[public.customers]'
Incremental snapshot for table 'public.customers' will end at position [4]
```

Kafka evidence after signal included a re-read of current customer rows:

```json
{"payload":{"id":1,"name":"Alice","email":"alice@example.com","balance":"JxA="}}
{"payload":{"id":2,"name":"Bob","email":"bob@example.com","balance":"TiA="}}
{"payload":{"id":3,"name":"Carol","email":"carol@example.com","balance":"Opg="}}
{"payload":{"id":4,"name":"Dina","email":"dina@example.com","balance":"ALmM"}}
```

Explanation: A running pipeline may re-snapshot a table to backfill or refresh a subset of data without throwing away the existing CDC stream history. The signal lets Debezium coordinate this work while the connector remains online.

## Mini-Exercises

Changed Dina twice:

```bash
UPDATE customers SET balance=450.00 WHERE email='dina@example.com';
UPDATE customers SET balance=475.00 WHERE email='dina@example.com';
```

Kafka showed successive current row states for Dina:

```json
{"payload":{"id":4,"name":"Dina","email":"dina@example.com","balance":"AK/I"}}
{"payload":{"id":4,"name":"Dina","email":"dina@example.com","balance":"ALmM"}}
```

Caveat: The configured `ExtractNewRecordState` unwrap transform removes the full Debezium envelope, so this topic does not expose a nested `before` and `after` object. It shows the post-change row state. The connector logs also note that PostgreSQL `REPLICA IDENTITY` is `DEFAULT`, so full old row values would not be available for updates unless the table were configured differently and the envelope were kept.

Snapshot mode explanation:

- `snapshot.mode=initial` captures existing table rows first, then continues with new WAL changes.
- `snapshot.mode=no_data` is preferable when only future changes should be captured and an initial baseline is not needed or will be loaded by another process.

Four-box flow:

```text
Database commit -> PostgreSQL WAL -> Debezium / Kafka Connect -> Kafka consumer
```

## Final Statement

Day 04 demonstrated PostgreSQL CDC with Debezium: an initial snapshot established baseline rows, later inserts and updates became Kafka records, and a Debezium signal triggered an incremental snapshot request while the connector stayed running.

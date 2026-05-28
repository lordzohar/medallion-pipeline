# Day 03 Lab Report

Course: Modern Data Engineering with Medallion Pipelines  
Date completed: 2026-05-28  
Workspace: `/home/ubuntu/Downloads/Medallion-pipeline/Week_01/Day_03/Lab_Files`

## Overall Result

Status: SUCCESS

Day 03 was performed with Kafka, Kafka Connect, the provided Python producer/consumer scripts, and the FileStream source/sink connector exercise.

## Setup Notes

The supplied Compose file needed environment fixes before it would run correctly in this local Docker setup:

- Added `CLUSTER_ID` for the KRaft Kafka broker.
- Added separate internal and host listeners so Kafka Connect can use `broker:29092` while Python clients can use `localhost:9092`.
- Changed `CONNECT_BOOTSTRAP_SERVERS` to `broker:29092`.
- Changed the Connect image to `confluentinc/cp-kafka-connect:7.5.0`.
- Added Apache Kafka `connect-file-3.5.0.jar` as a local plugin and mounted it into `CONNECT_PLUGIN_PATH`.

Python dependency installed:

```bash
python3 -m pip install --user confluent-kafka
```

Official reference used for the FileStream plugin path: Confluent documents that FileStream connector artifacts must be present in the worker plugin path.

## Environment Status

Command:

```bash
sudo docker compose ps
```

Evidence:

```text
NAME      IMAGE                                 SERVICE   STATUS
broker    confluentinc/cp-kafka:7.5.0           broker    Up
connect   confluentinc/cp-kafka-connect:7.5.0   connect   Up (healthy)
```

Kafka topics visible:

```text
__consumer_offsets
connect-configs
connect-offsets
connect-status
fs-source
logs
orders
```

Kafka Connect connectors visible:

```json
[
  "fs-source-connector",
  "fs-sink-connector"
]
```

## Exercise 1 - Consumer Groups

Created topic:

```bash
sudo docker exec broker kafka-topics --bootstrap-server broker:29092 \
  --create --if-not-exists --topic logs --partitions 3 --replication-factor 1
```

Topic evidence:

```text
Topic: logs PartitionCount: 3 ReplicationFactor: 1
Partition: 0 Leader: 1 Replicas: 1 Isr: 1
Partition: 1 Leader: 1 Replicas: 1 Isr: 1
Partition: 2 Leader: 1 Replicas: 1 Isr: 1
```

Produced records:

```text
alpha log line
beta log line
gamma log line
delta log line
epsilon log line
```

Consumer output from group `cg1`:

```text
Partition:0 alpha log line
Partition:0 beta log line
Partition:0 gamma log line
Partition:0 delta log line
Partition:0 epsilon log line
```

Consumer group evidence:

```text
GROUP  TOPIC  PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
cg1    logs   0          5               5               0
cg1    logs   1          0               0               0
cg1    logs   2          0               0               0
```

Observation: `cg1` consumed all available records and had zero lag.

## Exercise 2 - Offset Management

Read with auto-commit disabled:

```bash
sudo docker exec broker kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic logs \
  --group cg2 \
  --from-beginning \
  --consumer-property enable.auto.commit=false \
  --timeout-ms 5000
```

Output:

```text
alpha log line
beta log line
gamma log line
delta log line
epsilon log line
```

Reset offsets:

```bash
sudo docker exec broker kafka-consumer-groups \
  --bootstrap-server broker:29092 \
  --group cg2 \
  --topic logs \
  --reset-offsets --to-earliest --execute
```

Evidence:

```text
GROUP  TOPIC  PARTITION  NEW-OFFSET
cg2    logs   0          0
cg2    logs   1          0
cg2    logs   2          0
```

Replay evidence:

```text
alpha log line
beta log line
gamma log line
delta log line
epsilon log line
```

Observation: Resetting offsets to earliest allowed the same messages to be read again.

## Exercise 3 - Replication and Fault Tolerance

Created topic:

```bash
sudo docker exec broker kafka-topics --bootstrap-server broker:29092 \
  --create --if-not-exists --topic orders --partitions 2 --replication-factor 1
```

Topic evidence:

```text
Topic: orders PartitionCount: 2 ReplicationFactor: 1
Partition: 0 Leader: 1 Replicas: 1 Isr: 1
Partition: 1 Leader: 1 Replicas: 1 Isr: 1
```

Python producer:

```bash
python3 producer.py orders
```

Producer evidence:

```text
Delivered message to orders [1] at offset 0
Delivered message to orders [1] at offset 1
Delivered message to orders [1] at offset 2
Delivered message to orders [1] at offset 3
Delivered message to orders [1] at offset 4
Delivered message to orders [1] at offset 5
Delivered message to orders [0] at offset 0
Delivered message to orders [0] at offset 1
Delivered message to orders [0] at offset 2
Delivered message to orders [0] at offset 3
Produced 10 messages to topic "orders"
```

Python manual-commit consumer:

```bash
timeout 8 python3 -u consumer.py orders
```

Consumer evidence:

```text
Received record key=0 value=message 0 partition=1 offset=0
Received record key=1 value=message 1 partition=1 offset=1
Received record key=2 value=message 2 partition=1 offset=2
Received record key=3 value=message 3 partition=1 offset=3
Received record key=8 value=message 8 partition=1 offset=4
Received record key=9 value=message 9 partition=1 offset=5
Received record key=4 value=message 4 partition=0 offset=0
Received record key=5 value=message 5 partition=0 offset=1
Received record key=6 value=message 6 partition=0 offset=2
Received record key=7 value=message 7 partition=0 offset=3
```

Consumer group evidence:

```text
GROUP    TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
cg_lab3  orders  0          4               4               0
cg_lab3  orders  1          6               6               0
```

Observation: `acks=all` succeeded in a single-replica topic because the leader is the only in-sync replica.

## Exercise 4 - Kafka Connect FileStream

Created input:

```bash
mkdir -p data/input data/output
printf 'apple\nbanana\ncarrot\n' > data/input/produce.txt
```

Source connector registration:

```bash
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @connector-source.json
```

Source status:

```json
{
  "name": "fs-source-connector",
  "connector": {
    "state": "RUNNING",
    "worker_id": "connect:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "connect:8083"
    }
  ],
  "type": "source"
}
```

Topic verification:

```text
apple
banana
carrot
```

Sink connector registration:

```bash
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @connector-sink.json
```

Sink status:

```json
{
  "name": "fs-sink-connector",
  "connector": {
    "state": "RUNNING",
    "worker_id": "connect:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "connect:8083"
    }
  ],
  "type": "sink"
}
```

Output file:

```text
apple
banana
carrot
```

Updated source connector config with `poll.interval.ms=5000`, appended one more line, and verified the flow:

```text
dragonfruit
apple
banana
carrot
```

Sink output after append:

```text
apple
banana
carrot
dragonfruit
```

Observation: Kafka Connect moved file data into a Kafka topic and then back out to a file using source and sink tasks.

## Knowledge Check Answers

1. `__consumer_offsets`
2. Cooperative sticky assignment minimizes partition movement during rebalances.
3. Kafka elects a new leader when the current leader broker fails.
4. CDC using WAL/binlog is the low-latency database change pattern.
5. Schema Registry manages and validates Avro, Protobuf, and JSON schemas.
6. With `min.insync.replicas=2`, two broker failures in a three-replica topic cause produce requests to fail.

## Final Statement

Day 03 demonstrated consumer groups, offsets and replay, single-broker replication limits, producer acknowledgements, manual offset commits in Python, and Kafka Connect source/sink data movement.

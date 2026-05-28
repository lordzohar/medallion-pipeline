# Day 02 Lab Report

Course: Modern Data Engineering with Medallion Pipelines  
Date completed: 2026-05-28  
Workspace: `/home/ubuntu/Downloads/Medallion-pipeline/Week_01/Day_02/Lab_Files`

## Overall Result

Status: SUCCESS

Day 02 was performed with the supplied Docker Compose Kafka environment and the provided Python producer/consumer scripts.

## Setup Notes

`kafka-python` was missing, so it was installed for the current user:

```bash
python3 -m pip install --user kafka-python
```

Docker Compose was already installed:

```text
Docker Compose version v5.1.3
```

The supplied Compose file needed one lab-environment fix for the Confluent KRaft broker:

- Added `CLUSTER_ID` to the Kafka environment.
- Changed the local `data/` directory ownership so the Kafka container user could write to `/var/lib/kafka/data`.

## Start Kafka

Command:

```bash
sudo docker compose up -d
sudo docker compose ps
```

Evidence:

```text
NAME         IMAGE                         SERVICE   STATUS          PORTS
day2_kafka   confluentinc/cp-kafka:7.4.0   kafka     Up              0.0.0.0:9092-9093->9092-9093/tcp
```

Kafka log evidence:

```text
Kafka Server started
```

## Create and Inspect Topic

Commands:

```bash
sudo docker exec day2_kafka kafka-topics --create --if-not-exists \
  --topic demo-topic --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092

sudo docker exec day2_kafka kafka-topics --describe \
  --topic demo-topic --bootstrap-server localhost:9092
```

Evidence:

```text
Created topic demo-topic.
Topic: demo-topic PartitionCount: 3 ReplicationFactor: 1
Partition: 0 Leader: 1 Replicas: 1 Isr: 1
Partition: 1 Leader: 1 Replicas: 1 Isr: 1
Partition: 2 Leader: 1 Replicas: 1 Isr: 1
```

Observation: `demo-topic` has three partitions, which means Kafka can distribute keyed records across three ordered logs.

## CLI Producer and Consumer

Produced records:

```text
key1:Hello world
key2:Another event
key1:A second event on the same key
```

Consumer evidence:

```text
key1|Hello world
key2|Another event
key1|A second event on the same key
```

Observation: The topic accepted keyed records, and the consumer could read them from the beginning.

## Python Producer and Consumer

Command:

```bash
python3 producer.py
timeout 10 python3 -u consumer.py
```

Producer evidence:

```text
Sent message 0 to demo-topic
Sent message 1 to demo-topic
Sent message 2 to demo-topic
Sent message 3 to demo-topic
Sent message 4 to demo-topic
Sent message 5 to demo-topic
Sent message 6 to demo-topic
Sent message 7 to demo-topic
Sent message 8 to demo-topic
Sent message 9 to demo-topic
```

Consumer evidence:

```text
Partition 0, offset 0, key=partition-key, value={"id": 0, "message": "hello event 0"}
Partition 0, offset 1, key=partition-key, value={"id": 1, "message": "hello event 1"}
Partition 0, offset 2, key=partition-key, value={"id": 2, "message": "hello event 2"}
Partition 0, offset 3, key=partition-key, value={"id": 3, "message": "hello event 3"}
Partition 0, offset 4, key=partition-key, value={"id": 4, "message": "hello event 4"}
Partition 0, offset 5, key=partition-key, value={"id": 5, "message": "hello event 5"}
Partition 0, offset 6, key=partition-key, value={"id": 6, "message": "hello event 6"}
Partition 0, offset 7, key=partition-key, value={"id": 7, "message": "hello event 7"}
Partition 0, offset 8, key=partition-key, value={"id": 8, "message": "hello event 8"}
Partition 0, offset 9, key=partition-key, value={"id": 9, "message": "hello event 9"}
Partition 2, offset 0, key=key1, value=Hello world
Partition 2, offset 1, key=key2, value=Another event
Partition 2, offset 2, key=key1, value=A second event on the same key
```

Observation: The Python script used one fixed key, so its ten JSON messages stayed on the same partition. This demonstrates Kafka's per-key ordering behavior within a partition.

## Partitioning Experiment

Created another topic:

```bash
sudo docker exec day2_kafka kafka-topics --create --if-not-exists \
  --topic demo-topic-5 --partitions 5 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

Evidence:

```text
Created topic demo-topic-5.
Topic: demo-topic-5 PartitionCount: 5 ReplicationFactor: 1
Partition: 0 Leader: 1 Replicas: 1 Isr: 1
Partition: 1 Leader: 1 Replicas: 1 Isr: 1
Partition: 2 Leader: 1 Replicas: 1 Isr: 1
Partition: 3 Leader: 1 Replicas: 1 Isr: 1
Partition: 4 Leader: 1 Replicas: 1 Isr: 1
```

Produced and consumed keyed events:

```text
user-2|first order
user-2|second order same key
user-3|first order
user-1|first order
user-1|second order same key
```

Observation: Same-key events stayed grouped in their consumed order. Increasing partitions gives more possible parallelism, but ordering is still guaranteed only within a partition.

## Consumer Group Evidence

Command:

```bash
sudo docker exec day2_kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --group demo-group
```

Evidence:

```text
Consumer group 'demo-group' has no active members.

GROUP       TOPIC       PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
demo-group  demo-topic  0          10              10              0
demo-group  demo-topic  1          0               0               0
demo-group  demo-topic  2          3               3               0
```

Observation: The group consumed all available messages and had zero lag at the time of inspection.

## Mini Quiz Answers

1. Topic vs partition: A topic is the logical event channel; partitions are the ordered logs inside the topic and provide parallelism.
2. At-least-once producer setting from the manual's answer key: `acks=1`.
3. Two consumers in the same group with three partitions: Kafka assigns partitions across the consumers so each partition is consumed by only one group member.
4. Avro over JSON: Avro supports schemas, schema evolution, and compact binary encoding.

## Final Statement

Day 02 showed Kafka topic creation, keyed production, consumption from the beginning, partition behavior, Python client usage, and consumer group offset evidence.

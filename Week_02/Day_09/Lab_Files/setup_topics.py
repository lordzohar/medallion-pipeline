"""
setup_topics.py
---------------
Creates every Kafka topic the lab uses. Idempotent — safe to re-run.

Topic design:
  taxi-trips       6p / r3      retention=1h   one event per completed trip
  gps-pings        12p / r3     retention=10m  high-volume location stream
  trips-clean      6p / r3                     validated by taxi_consumer.py
  trips-enriched   6p / r3                     joined with driver CDC table
  trips-dlq        3p / r3      retention=24h  poison messages
  surge-events     3p / r3      compacted      latest surge value per zone
"""
from confluent_kafka.admin import AdminClient, NewTopic

from config import KAFKA_BOOTSTRAP

TOPICS = [
    # (name, partitions, replication, configs)
    ("taxi-trips",     6,  3, {"retention.ms": "3600000",  "compression.type": "snappy"}),
    ("gps-pings",      12, 3, {"retention.ms": "600000",   "compression.type": "snappy"}),
    ("trips-clean",    6,  3, {}),
    ("trips-enriched", 6,  3, {}),
    ("trips-dlq",      3,  3, {"retention.ms": "86400000"}),
    ("surge-events",   3,  3, {"cleanup.policy": "compact"}),
]


def main():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    new_topics = [
        NewTopic(name, num_partitions=p, replication_factor=r, config=cfg)
        for (name, p, r, cfg) in TOPICS
    ]
    fs = admin.create_topics(new_topics)
    for topic, f in fs.items():
        try:
            f.result()
            print(f"  created {topic}")
        except Exception as e:
            print(f"  skip    {topic}: {e}")


if __name__ == "__main__":
    main()

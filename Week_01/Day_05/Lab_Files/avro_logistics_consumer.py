"""
Practical Schema Registry consumer for Day 5.

This consumer reads Avro-encoded logistics records from Kafka and uses
Schema Registry to look up the writer schema during deserialization.
"""

from __future__ import annotations

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer


TOPIC = "northstar.logistics.avro"


def main() -> None:
    schema_registry = SchemaRegistryClient({"url": "http://localhost:8081"})
    avro_deserializer = AvroDeserializer(schema_registry)

    consumer = DeserializingConsumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "day5-avro-logistics-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": avro_deserializer,
        }
    )
    consumer.subscribe([TOPIC])

    seen = 0
    idle_polls = 0
    try:
        while True:
            msg = consumer.poll(2.0)
            if msg is None:
                idle_polls += 1
                if seen > 0 and idle_polls >= 2:
                    break
                if seen == 0 and idle_polls >= 5:
                    break
                continue
            idle_polls = 0
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            seen += 1
            print(
                "Consumed Avro record "
                f"key={msg.key()} partition={msg.partition()} offset={msg.offset()} value={msg.value()}"
            )
    finally:
        consumer.close()

    print(f"Consumed {seen} Avro records from {TOPIC}.")


if __name__ == "__main__":
    main()

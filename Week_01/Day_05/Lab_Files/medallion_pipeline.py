#!/usr/bin/env python3
"""Local proof pipeline for Day 5 ingestion contracts and medallion layers."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


ROOT = Path(__file__).resolve().parent
INPUT = ROOT / "input"
OUTPUT = ROOT / "outputs"
BRONZE = OUTPUT / "bronze"
SILVER = OUTPUT / "silver"
GOLD = OUTPUT / "gold"
QUARANTINE = OUTPUT / "quarantine"

KAFKA_TOPICS = {
    "orders": "northstar.orders.public.orders",
    "inventory": "northstar.inventory.inventorydb.inventory",
    "clickstream": "northstar.clickstream.events",
    "logistics": "northstar.logistics.csv",
    "dlq": "northstar.ingestion.dlq",
}
KAFKA_TOPIC_TO_BRONZE = {
    KAFKA_TOPICS["orders"]: BRONZE / "orders_cdc.json",
    KAFKA_TOPICS["clickstream"]: BRONZE / "clickstream_events.json",
    KAFKA_TOPICS["logistics"]: BRONZE / "logistics_batch.json",
}


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def write_json_records(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8")


def read_json_records(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"{path} must contain a JSON array of records")
    return data


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def stable_hash(row: dict) -> str:
    payload = json.dumps(row, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def reset_outputs() -> None:
    if OUTPUT.exists():
        shutil.rmtree(OUTPUT)
    for path in (BRONZE, SILVER, GOLD, QUARANTINE):
        path.mkdir(parents=True, exist_ok=True)


def add_bronze_metadata(row: dict, loaded_at: str) -> dict:
    enriched = dict(row)
    enriched["_bronze_loaded_at"] = loaded_at
    enriched["_record_hash"] = stable_hash(row)
    return enriched


def bronze_load() -> dict:
    reset_outputs()
    loaded_at = datetime.now(timezone.utc).isoformat()

    orders = []
    for row in read_json_records(INPUT / "orders_cdc.json"):
        orders.append(add_bronze_metadata(row, loaded_at))
    write_json_records(BRONZE / "orders_cdc.json", orders)

    clicks = []
    for row in read_json_records(INPUT / "clickstream_events.json"):
        clicks.append(add_bronze_metadata(row, loaded_at))
    write_json_records(BRONZE / "clickstream_events.json", clicks)

    shipments = []
    with (INPUT / "logistics_batch.csv").open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            shipments.append(add_bronze_metadata(row, loaded_at))
    write_json_records(BRONZE / "logistics_batch.json", shipments)

    return {
        "orders_bronze": len(orders),
        "clickstream_bronze": len(clicks),
        "logistics_bronze": len(shipments),
    }


def topic_partitions_for(consumer, topics: list[str], timeout_seconds: float = 10.0):
    from kafka import TopicPartition

    deadline = time.monotonic() + timeout_seconds
    assignments = []
    while time.monotonic() < deadline:
        assignments = []
        for topic in topics:
            partitions = consumer.partitions_for_topic(topic)
            if partitions is None:
                assignments = []
                break
            assignments.extend(TopicPartition(topic, partition) for partition in partitions)
        if assignments:
            return sorted(assignments, key=lambda tp: (tp.topic, tp.partition))
        time.sleep(0.25)
    missing = [topic for topic in topics if consumer.partitions_for_topic(topic) is None]
    raise RuntimeError(f"Kafka topic metadata was not available for: {', '.join(missing)}")


def reached_end_offsets(consumer, end_offsets: dict) -> bool:
    for topic_partition, end_offset in end_offsets.items():
        if consumer.position(topic_partition) < end_offset:
            return False
    return True


def decode_kafka_json(raw: bytes | None) -> dict:
    if raw is None:
        return {}
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Kafka message value must be a JSON object")
    return payload


def unwrap_connect_payload(value: dict) -> dict:
    payload = value.get("payload")
    if isinstance(payload, dict) and ("before" in payload or "after" in payload):
        return payload
    return value


def normalize_order_event_from_kafka(value: dict, offset: int) -> dict | None:
    value = unwrap_connect_payload(value)
    if "before" not in value and "after" not in value:
        return value

    op = value.get("op")
    source = value.get("source") if isinstance(value.get("source"), dict) else {}
    row = value.get("after") if op != "d" else value.get("before")
    if not isinstance(row, dict):
        return None

    order_id = row.get("order_id", row.get("id"))
    source_lsn = source.get("lsn") or offset
    order_ts = row.get("order_ts") or row.get("order_date") or value.get("ts_ms")
    if isinstance(order_ts, int):
        order_ts = datetime.fromtimestamp(order_ts / 1000, timezone.utc).isoformat().replace("+00:00", "Z")

    normalized = {
        "amount": float(row["amount"]) if row.get("amount") is not None else 0.0,
        "customer_id": row.get("customer_id"),
        "event_id": f"dbz-{source_lsn}-{op}-{order_id}-{offset}",
        "op": op,
        "order_id": order_id,
        "order_ts": order_ts,
        "source": source.get("name", "orders-db"),
        "source_lsn": source_lsn,
        "status": row.get("status"),
        "table": source.get("table", "orders"),
    }
    return normalized


def normalize_inventory_event_from_kafka(value: dict, offset: int) -> dict | None:
    value = unwrap_connect_payload(value)
    if "before" not in value and "after" not in value:
        return value

    op = value.get("op")
    source = value.get("source") if isinstance(value.get("source"), dict) else {}
    row = value.get("after") if op != "d" else value.get("before")
    if not isinstance(row, dict):
        return None

    source_pos = source.get("pos") or source.get("file") or offset
    item_id = row.get("id")
    return {
        "event_id": f"dbz-inventory-{source_pos}-{op}-{item_id}-{offset}",
        "op": op,
        "inventory_id": item_id,
        "product_name": row.get("product_name"),
        "quantity": row.get("quantity"),
        "last_updated": row.get("last_updated"),
        "source": source.get("name", "inventory-db"),
        "source_file": source.get("file"),
        "source_pos": source.get("pos", offset),
        "table": source.get("table", "inventory"),
    }


def load_file_bronze_without_reset(loaded_at: str, include_orders: bool = True) -> dict:
    metrics = {}

    if include_orders:
        orders = [add_bronze_metadata(row, loaded_at) for row in read_json_records(INPUT / "orders_cdc.json")]
        write_json_records(BRONZE / "orders_cdc.json", orders)
        metrics["orders_bronze"] = len(orders)

    clicks = [add_bronze_metadata(row, loaded_at) for row in read_json_records(INPUT / "clickstream_events.json")]
    write_json_records(BRONZE / "clickstream_events.json", clicks)
    metrics["clickstream_bronze"] = len(clicks)

    shipments = []
    with (INPUT / "logistics_batch.csv").open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            shipments.append(add_bronze_metadata(row, loaded_at))
    write_json_records(BRONZE / "logistics_batch.json", shipments)
    metrics["logistics_bronze"] = len(shipments)

    return metrics


def bronze_load_from_kafka(
    bootstrap_servers: str = "localhost:9092",
    group_id: str | None = None,
    idle_timeout_seconds: float = 5.0,
    run_id: str | None = None,
) -> dict:
    try:
        from kafka import KafkaConsumer
    except ImportError as exc:
        raise RuntimeError("Kafka mode requires kafka-python. Install it with: pip install kafka-python") from exc

    reset_outputs()
    loaded_at = datetime.now(timezone.utc).isoformat()
    group_id = group_id or f"day5-bronze-loader-{uuid4()}"
    rows_by_topic = {topic: [] for topic in KAFKA_TOPIC_TO_BRONZE}

    topics = list(KAFKA_TOPIC_TO_BRONZE.keys())
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=group_id,
        key_deserializer=lambda m: m.decode("utf-8") if m else None,
        value_deserializer=decode_kafka_json,
        consumer_timeout_ms=500,
    )
    try:
        assignments = topic_partitions_for(consumer, topics)
        consumer.assign(assignments)
        consumer.seek_to_beginning(*assignments)
        end_offsets = consumer.end_offsets(assignments)
        deadline = time.monotonic() + idle_timeout_seconds
        while not reached_end_offsets(consumer, end_offsets) and time.monotonic() < deadline:
            polled = consumer.poll(timeout_ms=500, max_records=100)
            if not polled:
                continue
            deadline = time.monotonic() + idle_timeout_seconds
            for messages in polled.values():
                for message in messages:
                    value = dict(message.value)
                    message_run_id = value.pop("_lab_run_id", None)
                    if run_id and message_run_id != run_id:
                        continue
                    if message.topic == KAFKA_TOPICS["orders"]:
                        value = normalize_order_event_from_kafka(value, message.offset)
                        if value is None:
                            continue
                    bronze_record = add_bronze_metadata(value, loaded_at)
                    bronze_record["_kafka_topic"] = message.topic
                    bronze_record["_kafka_partition"] = message.partition
                    bronze_record["_kafka_offset"] = message.offset
                    bronze_record["_kafka_key"] = message.key
                    if message_run_id:
                        bronze_record["_lab_run_id"] = message_run_id
                    rows_by_topic[message.topic].append(bronze_record)
    finally:
        consumer.close()

    for topic, path in KAFKA_TOPIC_TO_BRONZE.items():
        rows = sorted(rows_by_topic[topic], key=lambda r: (r["_kafka_partition"], r["_kafka_offset"]))
        write_json_records(path, rows)

    return {
        "orders_bronze": len(rows_by_topic[KAFKA_TOPICS["orders"]]),
        "clickstream_bronze": len(rows_by_topic[KAFKA_TOPICS["clickstream"]]),
        "logistics_bronze": len(rows_by_topic[KAFKA_TOPICS["logistics"]]),
        "kafka_group_id": group_id,
        "kafka_run_id_filter": run_id or "",
    }


def bronze_load_debezium_orders(
    bootstrap_servers: str = "localhost:9092",
    group_id: str | None = None,
    idle_timeout_seconds: float = 5.0,
) -> dict:
    try:
        from kafka import KafkaConsumer
    except ImportError as exc:
        raise RuntimeError("Debezium mode requires kafka-python. Install it with: pip install kafka-python") from exc

    reset_outputs()
    loaded_at = datetime.now(timezone.utc).isoformat()
    metrics = load_file_bronze_without_reset(loaded_at, include_orders=False)
    group_id = group_id or f"day5-debezium-orders-loader-{uuid4()}"
    orders = []

    consumer = KafkaConsumer(
        KAFKA_TOPICS["orders"],
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=group_id,
        key_deserializer=lambda m: m.decode("utf-8") if m else None,
        value_deserializer=decode_kafka_json,
        consumer_timeout_ms=500,
    )
    try:
        deadline = time.monotonic() + idle_timeout_seconds
        while time.monotonic() < deadline:
            polled = consumer.poll(timeout_ms=500, max_records=100)
            if not polled:
                continue
            deadline = time.monotonic() + idle_timeout_seconds
            for messages in polled.values():
                for message in messages:
                    normalized = normalize_order_event_from_kafka(dict(message.value), message.offset)
                    if normalized is None or "dbz-" not in str(normalized.get("event_id", "")):
                        continue
                    bronze_record = add_bronze_metadata(normalized, loaded_at)
                    bronze_record["_kafka_topic"] = message.topic
                    bronze_record["_kafka_partition"] = message.partition
                    bronze_record["_kafka_offset"] = message.offset
                    bronze_record["_kafka_key"] = message.key
                    orders.append(bronze_record)
    finally:
        consumer.close()

    orders = sorted(orders, key=lambda r: (r["source_lsn"], r["_kafka_partition"], r["_kafka_offset"]))
    write_json_records(BRONZE / "orders_cdc.json", orders)
    metrics["orders_bronze"] = len(orders)
    metrics["kafka_group_id"] = group_id
    metrics["orders_source"] = "debezium"
    return metrics


def bronze_load_debezium_inventory(
    bootstrap_servers: str = "localhost:9092",
    group_id: str | None = None,
    idle_timeout_seconds: float = 5.0,
) -> dict:
    try:
        from kafka import KafkaConsumer
    except ImportError as exc:
        raise RuntimeError("Inventory CDC mode requires kafka-python. Install it with: pip install kafka-python") from exc

    loaded_at = datetime.now(timezone.utc).isoformat()
    group_id = group_id or f"day5-debezium-inventory-loader-{uuid4()}"
    rows = []

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=group_id,
        key_deserializer=lambda m: m.decode("utf-8") if m else None,
        value_deserializer=decode_kafka_json,
        consumer_timeout_ms=500,
    )
    try:
        assignments = topic_partitions_for(consumer, [KAFKA_TOPICS["inventory"]])
        consumer.assign(assignments)
        consumer.seek_to_beginning(*assignments)
        end_offsets = consumer.end_offsets(assignments)
        deadline = time.monotonic() + idle_timeout_seconds
        while not reached_end_offsets(consumer, end_offsets) and time.monotonic() < deadline:
            polled = consumer.poll(timeout_ms=500, max_records=100)
            if not polled:
                continue
            deadline = time.monotonic() + idle_timeout_seconds
            for messages in polled.values():
                for message in messages:
                    normalized = normalize_inventory_event_from_kafka(dict(message.value), message.offset)
                    if normalized is None:
                        continue
                    bronze_record = add_bronze_metadata(normalized, loaded_at)
                    bronze_record["_kafka_topic"] = message.topic
                    bronze_record["_kafka_partition"] = message.partition
                    bronze_record["_kafka_offset"] = message.offset
                    bronze_record["_kafka_key"] = message.key
                    rows.append(bronze_record)
    finally:
        consumer.close()

    rows = sorted(rows, key=lambda r: (str(r.get("source_file") or ""), int(r.get("source_pos") or 0), r["_kafka_partition"], r["_kafka_offset"]))
    write_json_records(BRONZE / "inventory_cdc.json", rows)
    return {
        "inventory_bronze": len(rows),
        "inventory_source": "debezium-mysql",
        "kafka_group_id": group_id,
    }


def build_silver() -> dict:
    orders = read_json_records(BRONZE / "orders_cdc.json")
    seen_event_ids = set()
    duplicate_events = []
    current_orders = {}

    for event in sorted(orders, key=lambda r: (r["source_lsn"], r["event_id"])):
        if event["event_id"] in seen_event_ids:
            duplicate_events.append(event)
            continue
        seen_event_ids.add(event["event_id"])
        order_id = str(event["order_id"])
        if event["op"] == "d":
            current_orders.pop(order_id, None)
        else:
            current_orders[order_id] = {
                "order_id": order_id,
                "customer_id": str(event["customer_id"]),
                "order_date": event["order_ts"][:10],
                "status": event["status"],
                "amount": f"{float(event['amount']):.2f}",
                "last_source_lsn": str(event["source_lsn"]),
            }

    customers = {}
    with (INPUT / "customers.csv").open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            customers[row["customer_id"]] = row

    order_rows = sorted(current_orders.values(), key=lambda r: int(r["order_id"]))
    write_csv(
        SILVER / "orders_current.csv",
        order_rows,
        ["order_id", "customer_id", "order_date", "status", "amount", "last_source_lsn"],
    )
    write_json_records(QUARANTINE / "duplicate_events.json", duplicate_events)

    shipments_valid = []
    dlq = []
    known_orders = set(current_orders)
    for row in read_json_records(BRONZE / "logistics_batch.json"):
        errors = []
        if not row.get("shipment_id"):
            errors.append("missing shipment_id")
        if row.get("status") not in {"CREATED", "IN_TRANSIT", "DELIVERED"}:
            errors.append("invalid shipment status")
        if row.get("order_id") not in known_orders:
            errors.append("unknown order_id")
        if errors:
            dlq.append({"source": "logistics_batch", "errors": errors, "record": row})
        else:
            shipments_valid.append(
                {
                    "shipment_id": row["shipment_id"],
                    "order_id": row["order_id"],
                    "carrier": row["carrier"],
                    "status": row["status"],
                    "event_ts": row["event_ts"],
                }
            )
    write_csv(
        SILVER / "shipments_valid.csv",
        shipments_valid,
        ["shipment_id", "order_id", "carrier", "status", "event_ts"],
    )
    write_json_records(QUARANTINE / "dlq_logistics.json", dlq)

    dim_rows = []
    for customer in customers.values():
        dim_rows.append(
            {
                "customer_key": customer["customer_id"],
                "customer_id": customer["customer_id"],
                "customer_name": customer["customer_name"],
                "customer_tier": customer["customer_tier"],
                "region": customer["region"],
            }
        )
    write_csv(
        SILVER / "dim_customer.csv",
        sorted(dim_rows, key=lambda r: r["customer_id"]),
        ["customer_key", "customer_id", "customer_name", "customer_tier", "region"],
    )

    fact_rows = []
    for order in order_rows:
        cust = customers.get(order["customer_id"], {})
        fact_rows.append(
            {
                "order_id": order["order_id"],
                "order_date": order["order_date"],
                "customer_key": order["customer_id"],
                "region": cust.get("region", "UNKNOWN"),
                "status": order["status"],
                "amount": order["amount"],
            }
        )
    write_csv(
        SILVER / "fact_orders.csv",
        fact_rows,
        ["order_id", "order_date", "customer_key", "region", "status", "amount"],
    )

    quality_report = {
        "duplicate_events": len(duplicate_events),
        "valid_current_orders": len(order_rows),
        "valid_shipments": len(shipments_valid),
        "dlq_records": len(dlq),
        "latest_order_lsn": max((int(r["last_source_lsn"]) for r in order_rows), default=0),
    }
    (SILVER / "data_quality_report.json").write_text(
        json.dumps(quality_report, indent=2, sort_keys=True), encoding="utf-8"
    )
    return quality_report


def build_silver_inventory() -> dict:
    inventory_path = BRONZE / "inventory_cdc.json"
    if not inventory_path.exists():
        write_csv(SILVER / "inventory_current.csv", [], ["inventory_id", "product_name", "quantity", "last_updated", "last_source_pos"])
        return {"inventory_current_rows": 0}

    current_inventory = {}
    for event in read_json_records(inventory_path):
        item_id = str(event["inventory_id"])
        if event["op"] == "d":
            current_inventory.pop(item_id, None)
        else:
            current_inventory[item_id] = {
                "inventory_id": item_id,
                "product_name": event["product_name"],
                "quantity": str(event["quantity"]),
                "last_updated": str(event.get("last_updated") or ""),
                "last_source_pos": str(event.get("source_pos") or ""),
            }

    rows = sorted(current_inventory.values(), key=lambda r: int(r["inventory_id"]))
    write_csv(
        SILVER / "inventory_current.csv",
        rows,
        ["inventory_id", "product_name", "quantity", "last_updated", "last_source_pos"],
    )
    return {"inventory_current_rows": len(rows)}


def publish_quarantine_to_kafka(bootstrap_servers: str = "localhost:9092") -> dict:
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError("Kafka DLQ publishing requires kafka-python. Install it with: pip install kafka-python") from exc

    dlq_path = QUARANTINE / "dlq_logistics.json"
    records = read_json_records(dlq_path) if dlq_path.exists() else []
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v, sort_keys=True).encode("utf-8"),
    )
    for record in records:
        errors = ",".join(record.get("errors", []))
        source = record.get("source", "unknown")
        producer.send(KAFKA_TOPICS["dlq"], key=f"{source}:{errors}", value=record)
    producer.flush()
    producer.close()
    return {"published_dlq_records": len(records), "dlq_topic": KAFKA_TOPICS["dlq"]}


def build_gold() -> dict:
    fact_path = SILVER / "fact_orders.csv"
    with fact_path.open(encoding="utf-8", newline="") as f:
        facts = list(csv.DictReader(f))

    daily = defaultdict(lambda: {"order_count": 0, "paid_or_shipped_count": 0, "revenue": 0.0})
    region = defaultdict(lambda: {"order_count": 0, "revenue": 0.0})
    for row in facts:
        daily_row = daily[row["order_date"]]
        daily_row["order_count"] += 1
        if row["status"] in {"PAID", "SHIPPED", "DELIVERED"}:
            daily_row["paid_or_shipped_count"] += 1
            daily_row["revenue"] += float(row["amount"])
        region_row = region[row["region"]]
        region_row["order_count"] += 1
        region_row["revenue"] += float(row["amount"])

    daily_rows = [
        {
            "order_date": date,
            "order_count": vals["order_count"],
            "paid_or_shipped_count": vals["paid_or_shipped_count"],
            "recognized_revenue": f"{vals['revenue']:.2f}",
        }
        for date, vals in sorted(daily.items())
    ]
    region_rows = [
        {
            "region": name,
            "order_count": vals["order_count"],
            "gross_order_amount": f"{vals['revenue']:.2f}",
        }
        for name, vals in sorted(region.items())
    ]
    write_csv(
        GOLD / "daily_order_kpis.csv",
        daily_rows,
        ["order_date", "order_count", "paid_or_shipped_count", "recognized_revenue"],
    )
    write_csv(
        GOLD / "region_order_summary.csv",
        region_rows,
        ["region", "order_count", "gross_order_amount"],
    )

    click_counts = Counter(row["event_type"] for row in read_json_records(BRONZE / "clickstream_events.json"))
    write_csv(
        GOLD / "clickstream_event_summary.csv",
        [{"event_type": k, "event_count": v} for k, v in sorted(click_counts.items())],
        ["event_type", "event_count"],
    )

    return {
        "daily_kpi_rows": len(daily_rows),
        "region_summary_rows": len(region_rows),
        "clickstream_summary_rows": len(click_counts),
    }


def validate_schema_proposals() -> list[dict]:
    proposals = json.loads((INPUT / "schema_change_proposals.json").read_text(encoding="utf-8"))
    out = []
    for item in proposals:
        status = "ACCEPT" if item["recommended_policy"] != "reject" else "REJECT"
        out.append({**item, "decision": status})
    (SILVER / "schema_change_decisions.json").write_text(
        json.dumps(out, indent=2, sort_keys=True), encoding="utf-8"
    )
    return out


def run_all() -> dict:
    metrics = {}
    metrics.update(bronze_load())
    metrics.update(build_silver())
    metrics.update(build_gold())
    metrics["schema_change_proposals"] = len(validate_schema_proposals())
    metrics["generated_at"] = datetime.now(timezone.utc).isoformat()
    (OUTPUT / "run_summary.json").write_text(
        json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8"
    )
    return metrics


def run_all_from_kafka(bootstrap_servers: str = "localhost:9092", run_id: str | None = None) -> dict:
    metrics = {}
    metrics.update(bronze_load_from_kafka(bootstrap_servers=bootstrap_servers, run_id=run_id))
    metrics.update(build_silver())
    metrics.update(build_gold())
    metrics["schema_change_proposals"] = len(validate_schema_proposals())
    metrics["generated_at"] = datetime.now(timezone.utc).isoformat()
    (OUTPUT / "run_summary.json").write_text(
        json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8"
    )
    return metrics


def run_all_from_debezium_orders(bootstrap_servers: str = "localhost:9092") -> dict:
    metrics = {}
    metrics.update(bronze_load_debezium_orders(bootstrap_servers=bootstrap_servers))
    metrics.update(build_silver())
    metrics.update(build_gold())
    metrics["schema_change_proposals"] = len(validate_schema_proposals())
    metrics["generated_at"] = datetime.now(timezone.utc).isoformat()
    (OUTPUT / "run_summary.json").write_text(
        json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8"
    )
    return metrics


def run_inventory_from_debezium(bootstrap_servers: str = "localhost:9092") -> dict:
    metrics = {}
    metrics.update(bronze_load_debezium_inventory(bootstrap_servers=bootstrap_servers))
    metrics.update(build_silver_inventory())
    (OUTPUT / "run_summary.json").write_text(
        json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8"
    )
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=[
            "bronze",
            "bronze-kafka",
            "bronze-debezium-orders",
            "inventory-debezium",
            "publish-dlq",
            "silver",
            "gold",
            "all",
            "all-kafka",
            "all-debezium-orders",
        ],
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--run-id", default=None, help="Only consume lab records produced with this run id")
    args = parser.parse_args()
    if args.command == "bronze":
        print(json.dumps(bronze_load(), indent=2, sort_keys=True))
    elif args.command == "bronze-kafka":
        print(
            json.dumps(
                bronze_load_from_kafka(bootstrap_servers=args.bootstrap_servers, run_id=args.run_id),
                indent=2,
                sort_keys=True,
            )
        )
    elif args.command == "bronze-debezium-orders":
        print(json.dumps(bronze_load_debezium_orders(bootstrap_servers=args.bootstrap_servers), indent=2, sort_keys=True))
    elif args.command == "inventory-debezium":
        print(json.dumps(run_inventory_from_debezium(bootstrap_servers=args.bootstrap_servers), indent=2, sort_keys=True))
    elif args.command == "publish-dlq":
        print(json.dumps(publish_quarantine_to_kafka(bootstrap_servers=args.bootstrap_servers), indent=2, sort_keys=True))
    elif args.command == "silver":
        print(json.dumps(build_silver(), indent=2, sort_keys=True))
    elif args.command == "gold":
        print(json.dumps(build_gold(), indent=2, sort_keys=True))
    elif args.command == "all-kafka":
        print(json.dumps(run_all_from_kafka(bootstrap_servers=args.bootstrap_servers, run_id=args.run_id), indent=2, sort_keys=True))
    elif args.command == "all-debezium-orders":
        print(json.dumps(run_all_from_debezium_orders(bootstrap_servers=args.bootstrap_servers), indent=2, sort_keys=True))
    else:
        print(json.dumps(run_all(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

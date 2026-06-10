"""
DLQ Replayer - inspects trips-dlq, prints summary, optionally re-publishes.
Useful for Lab 7 (error handling) and Lab 9 (operational runbooks).
"""
import argparse
import json
import time
from collections import Counter

from confluent_kafka import Consumer, Producer

from config import KAFKA_BOOTSTRAP, TOPIC_TRIPS_DLQ, TOPIC_TRIPS_RAW


def inspect(bootstrap: str, limit: int):
    c = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"dlq-inspector-{int(time.time())}",
        "auto.offset.reset": "earliest",
    })
    c.subscribe([TOPIC_TRIPS_DLQ])
    seen = 0
    reasons = Counter()
    samples = []
    deadline = time.time() + 8
    while seen < limit and time.time() < deadline:
        msg = c.poll(1.0)
        if not msg or msg.error():
            continue
        try:
            r = json.loads(msg.value())
            reasons[r.get("_dlq_reason", "unknown")] += 1
            if len(samples) < 3:
                samples.append(r)
            seen += 1
        except Exception:
            pass
    c.close()
    print(f"--- DLQ inspection (last {seen}) ---")
    for k, v in reasons.most_common():
        print(f"  {v:4d}  {k}")
    print("--- samples ---")
    for s in samples:
        print(json.dumps({k: s[k] for k in list(s)[:6]}, indent=2))


def replay(bootstrap: str):
    c = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "dlq-replayer-v1",
        "auto.offset.reset": "earliest",
    })
    p = Producer({"bootstrap.servers": bootstrap})
    c.subscribe([TOPIC_TRIPS_DLQ])
    n = 0
    deadline = time.time() + 15
    while time.time() < deadline:
        msg = c.poll(1.0)
        if not msg or msg.error():
            continue
        try:
            r = json.loads(msg.value())
            # strip metadata then re-publish
            for k in ("_dlq_reason", "_dlq_ts", "_corruption"):
                r.pop(k, None)
            p.produce(TOPIC_TRIPS_RAW, key=str(r.get("driver_id", "x")).encode(),
                      value=json.dumps(r).encode())
            n += 1
        except Exception:
            pass
    p.flush(5)
    print(f"replayed {n} messages back to taxi-trips")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    ap.add_argument("--mode", choices=["inspect", "replay"], default="inspect")
    ap.add_argument("--limit", type=int, default=200)
    args = ap.parse_args()
    if args.mode == "inspect":
        inspect(args.bootstrap, args.limit)
    else:
        replay(args.bootstrap)


if __name__ == "__main__":
    main()

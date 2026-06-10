"""
Load Tester
===========
Spawns N parallel producer processes, each producing K msg/sec,
for the scaling labs. Measures end-to-end produce throughput.
"""
import argparse
import json
import multiprocessing as mp
import random
import time

from confluent_kafka import Producer

from config import KAFKA_BOOTSTRAP, TOPIC_TRIPS_RAW


def worker(worker_id: int, bootstrap: str, rate: int, duration: int, topic: str):
    p = Producer({
        "bootstrap.servers": bootstrap,
        "client.id": f"loadgen-{worker_id}",
        "compression.type": "snappy",
        "linger.ms": 20,
        "batch.size": 64 * 1024,
        "acks": "1",
    })
    interval = 1.0 / rate
    start = time.time()
    sent = 0
    while time.time() - start < duration:
        t0 = time.time()
        msg = {
            "worker": worker_id,
            "seq": sent,
            "ts": time.time(),
            "payload": "x" * random.randint(100, 500),
        }
        p.produce(topic, key=str(worker_id).encode(), value=json.dumps(msg).encode())
        sent += 1
        p.poll(0)
        sleep = interval - (time.time() - t0)
        if sleep > 0:
            time.sleep(sleep)
    p.flush(10)
    elapsed = time.time() - start
    print(f"[w{worker_id}] sent={sent} rate={sent/elapsed:.0f}/s")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--rate", type=int, default=2000, help="msg/sec per worker")
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--topic", default=TOPIC_TRIPS_RAW)
    args = ap.parse_args()

    print(f"firing {args.workers} workers × {args.rate} msg/s for {args.duration}s "
          f"(target ~{args.workers*args.rate} msg/s)")
    procs = [mp.Process(target=worker, args=(i, args.bootstrap, args.rate, args.duration, args.topic))
             for i in range(args.workers)]
    for p in procs: p.start()
    for p in procs: p.join()


if __name__ == "__main__":
    main()

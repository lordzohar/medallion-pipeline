"""Quality dashboard — Flask UI showing:

  - latest rule pack results (POSTed by Airflow DAG 40_data_quality)
  - active alerts (POSTed by Alertmanager webhook)
  - DLQ topic sizes (sampled from Kafka)
  - quality_dashboard /metrics for Prometheus

Single-process Flask, in-memory store, intentionally simple.
"""
from __future__ import annotations

import os
import threading
import time
from collections import deque
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, render_template, request
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST

app = Flask(__name__)

# In-memory stores.
_lock = threading.Lock()
RULE_HISTORY: deque = deque(maxlen=200)   # list of [{rule, status, detail, checked_at, rows_scanned}, ...]
ALERTS_ACTIVE: dict = {}                  # fingerprint -> alert dict
ALERTS_HISTORY: deque = deque(maxlen=500)
DLQ_SIZES: dict = {}                      # topic -> size (refreshed in background)

# Metrics
M_RULE_RESULTS = Counter(
    "quality_rule_results_total",
    "Number of rule evaluations received, partitioned by status.",
    labelnames=("rule", "status"),
)
M_ACTIVE_ALERTS = Gauge("quality_active_alerts", "Currently firing alerts.")
M_DLQ_SIZE = Gauge("quality_dlq_size", "Sampled DLQ topic size.", labelnames=("topic",))


# ---------- background DLQ sampler ----------

def _sample_dlq_loop():
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
    try:
        from confluent_kafka import Consumer, TopicPartition
        from confluent_kafka.admin import AdminClient
    except Exception:
        return
    admin = AdminClient({"bootstrap.servers": bootstrap})
    while True:
        try:
            md = admin.list_topics(timeout=10)
            dlq_topics = [t for t in md.topics if t.startswith("dlq.")]
            consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "qd-sampler",
                                 "enable.auto.commit": False})
            for t in dlq_topics:
                total = 0
                for p in md.topics[t].partitions:
                    low, high = consumer.get_watermark_offsets(TopicPartition(t, p), timeout=5)
                    total += (high - low)
                DLQ_SIZES[t] = total
                M_DLQ_SIZE.labels(topic=t).set(total)
            consumer.close()
        except Exception as e:
            print(f"[dlq-sampler] {type(e).__name__}: {e}")
        time.sleep(30)


threading.Thread(target=_sample_dlq_loop, daemon=True).start()


# ---------- routes ----------

@app.route("/")
def index():
    with _lock:
        latest_by_rule: dict[str, dict] = {}
        for r in RULE_HISTORY:
            latest_by_rule[r["rule"]] = r
        return render_template(
            "dashboard.html",
            latest=sorted(latest_by_rule.values(), key=lambda r: r["rule"]),
            alerts=sorted(ALERTS_ACTIVE.values(), key=lambda a: a.get("severity", "")),
            dlq=sorted(DLQ_SIZES.items()),
            now=datetime.now(timezone.utc).isoformat(),
        )


@app.route("/health")
def health():
    return {"status": "ok"}, 200


@app.route("/metrics")
def metrics():
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}


@app.route("/api/quality/results", methods=["POST"])
def post_results():
    payload = request.get_json(silent=True) or {}
    results = payload.get("results", [])
    with _lock:
        for r in results:
            RULE_HISTORY.append(r)
            M_RULE_RESULTS.labels(rule=r.get("rule", "?"), status=r.get("status", "?")).inc()
    return {"received": len(results)}, 200


@app.route("/api/quality/history")
def history():
    with _lock:
        return jsonify(list(RULE_HISTORY))


@app.route("/webhook/alerts", methods=["POST"])
def alertmanager_webhook():
    """Alertmanager v4 webhook payload."""
    payload = request.get_json(silent=True) or {}
    with _lock:
        for alert in payload.get("alerts", []):
            fp = alert.get("fingerprint", str(time.time()))
            if alert.get("status") == "resolved":
                ALERTS_ACTIVE.pop(fp, None)
            else:
                ALERTS_ACTIVE[fp] = alert
            ALERTS_HISTORY.append({
                "ts":          datetime.now(timezone.utc).isoformat(),
                "fingerprint": fp,
                "status":      alert.get("status"),
                "labels":      alert.get("labels", {}),
                "annotations": alert.get("annotations", {}),
            })
        M_ACTIVE_ALERTS.set(len(ALERTS_ACTIVE))
    return {"received": len(payload.get("alerts", []))}, 200


@app.route("/api/alerts")
def list_alerts():
    with _lock:
        return jsonify({"active": list(ALERTS_ACTIVE.values()),
                        "history": list(ALERTS_HISTORY)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)

#!/usr/bin/env bash
# =============================================================================
#  Day 9 Monitoring Labs - one-shot bootstrap
#  Works on Linux / macOS / WSL / Git Bash on Windows
# =============================================================================
set -e

cd "$(dirname "$0")"

# 1) Download JMX Prometheus javaagent (~600 KB) if missing -------------------
JAR="jmx_exporter/jmx_prometheus_javaagent.jar"
if [ ! -f "$JAR" ]; then
    echo "Downloading JMX Prometheus javaagent..."
    mkdir -p jmx_exporter
    URL="https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar"
    curl -fSL -o "$JAR" "$URL"
fi

# 2) Create Python venv + install deps ----------------------------------------
if [ ! -d ".venv" ]; then
    echo "Creating .venv..."
    python -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate 2>/dev/null || source .venv/Scripts/activate
pip install --quiet -r requirements.txt

# 3) Bring up the stack -------------------------------------------------------
echo "Starting Kafka KRaft cluster + monitoring stack..."
docker compose up -d

# 4) Wait for Kafka -----------------------------------------------------------
echo "Waiting for Kafka cluster to be ready..."
for i in $(seq 1 30); do
    if docker exec kafka1 kafka-broker-api-versions --bootstrap-server kafka1:29092 >/dev/null 2>&1; then
        echo "  Kafka ready."
        break
    fi
    sleep 3
done

# 5) Create topics ------------------------------------------------------------
echo "Creating topics..."
python setup_topics.py

# 6) Seed Postgres + register Debezium connector ------------------------------
echo "Waiting for Postgres..."
for i in $(seq 1 20); do
    if docker exec postgres pg_isready -U taxi >/dev/null 2>&1; then break; fi
    sleep 2
done
echo "Seeding drivers table..."
python db_seeder.py

echo "Registering Debezium PostgreSQL connector..."
python register_connector.py

# 7) Print URLs ---------------------------------------------------------------
cat <<EOF

=================================================================
  All services up. Open these in your browser:
=================================================================
  Live Taxi Dashboard : http://localhost:5000
  Kafka UI            : http://localhost:8080
  Grafana             : http://localhost:3000   (admin/admin)
  Prometheus          : http://localhost:9090
  Alertmanager        : http://localhost:9093
  Kafka Connect REST  : http://localhost:8083
  Postgres            : localhost:5432  (taxi/taxi/taxi)
  ksqlDB CLI          : docker exec -it ksqldb-cli ksql http://ksqldb-server:8088

Next: open LABS_GUIDE.md and start Lab 1.
EOF

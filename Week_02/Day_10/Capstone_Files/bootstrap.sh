#!/usr/bin/env bash
# Linux/Mac twin of bootstrap.ps1
set -euo pipefail
cd "$(dirname "$0")"

[ -f .env ] || cp .env.example .env

if [ ! -f monitoring/jmx_exporter/jmx_prometheus_javaagent.jar ] \
   || [ ! -d debezium/plugins/confluentinc-kafka-connect-s3-10.5.13 ]; then
    bash debezium/download_plugins.sh
fi

docker compose --env-file .env --progress=plain build app-base
docker compose --env-file .env --progress=plain up -d

wait_http() {
    local name="$1" url="$2" timeout="${3:-240}" t0=$(date +%s)
    printf "[wait] %s " "$name"
    while true; do
        if curl -fsS -o /dev/null --max-time 3 "$url"; then echo " ok"; return; fi
        if [ $(( $(date +%s) - t0 )) -ge "$timeout" ]; then echo; echo "$name not healthy at $url"; exit 1; fi
        printf "."; sleep 3
    done
}

wait_http "minio"           "http://localhost:9000/minio/health/live"
wait_http "schema-registry" "http://localhost:8081/subjects"
wait_http "kafka-connect"   "http://localhost:8083/"
wait_http "airflow"         "http://localhost:8080/health"

export MINIO_ENDPOINT=http://localhost:9000
export SCHEMA_REGISTRY_URL=http://localhost:8081
export KAFKA_CONNECT_URL=http://localhost:8083

load_env() {
    local line key value
    while IFS= read -r line || [ -n "$line" ]; do
        line="${line#"${line%%[![:space:]]*}"}"
        line="${line%"${line##*[![:space:]]}"}"
        [[ -z "$line" || "$line" == \#* || "$line" != *=* ]] && continue
        key="${line%%=*}"
        value="${line#*=}"
        key="${key%"${key##*[![:space:]]}"}"
        [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
        export "$key=$value"
    done < .env
}

load_env

create_topic() {
    local topic="$1" partitions="${2:-1}"
    docker exec kafka sh -lc \
        "unset KAFKA_OPTS; kafka-topics --bootstrap-server localhost:29092 --create --if-not-exists --topic '$topic' --partitions '$partitions' --replication-factor 1" \
        >/dev/null
    echo "[ok] topic $topic"
}

create_topic "ogn.aircraft.positions" 3
create_topic "noaa.observations" 3
create_topic "noaa.alerts" 1
create_topic "seismic.events" 3
create_topic "config.public.regions" 1
create_topic "config.public.alert_thresholds" 1
create_topic "config.public.subscriber_watchlist" 1
create_topic "__debezium-heartbeat.config" 1
create_topic "dlq.config-source" 1
create_topic "dlq.s3-sink-bronze-streams" 1
create_topic "dlq.s3-sink-bronze-cdc" 1

run_py() {
    local script="$1"
    if command -v python3 >/dev/null && python3 -c "import boto3,requests,minio" 2>/dev/null; then
        python3 "$script"
    else
        docker exec airflow-webserver python "/opt/$script"
    fi
}

run_py minio/bootstrap.py
run_py app/register_app_schemas.py
run_py debezium/register_all_connectors.py

for d in 15_config_drift 30_hop_medallion 40_data_quality 50_business_kpis; do
    docker exec airflow-webserver airflow dags unpause "$d" >/dev/null
    echo "  - $d"
done

echo
echo "============================================================"
echo " Day 10 capstone is up. URLs:"
echo "   Airflow             http://localhost:8080"
echo "   Kafka UI            http://localhost:8088"
echo "   MinIO console       http://localhost:9001"
echo "   Apache Hop Web      http://localhost:8089"
echo "   Quality dashboard   http://localhost:5001"
echo "   Business dashboard  http://localhost:5002"
echo "   Live map (gliders)  http://localhost:5003"
echo "   Grafana             http://localhost:3000"
echo "============================================================"
echo "Run smoke test:  python tests/smoke_test.py"

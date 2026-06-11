#!/usr/bin/env bash
# Linux/Mac twin of bootstrap.ps1
set -euo pipefail
cd "$(dirname "$0")"

[ -f .env ] || cp .env.example .env

if [ ! -f monitoring/jmx_exporter/jmx_prometheus_javaagent.jar ] \
   || [ ! -d debezium/plugins/confluentinc-kafka-connect-s3 ]; then
    bash debezium/download_plugins.sh
fi

docker compose --env-file .env up -d

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
# shellcheck disable=SC2046
export $(grep -v '^\s*#' .env | xargs)

run_py() {
    local script="$1"
    if command -v python3 >/dev/null && python3 -c "import boto3,requests,minio" 2>/dev/null; then
        python3 "$script"
    else
        docker exec airflow-webserver python "$script"
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

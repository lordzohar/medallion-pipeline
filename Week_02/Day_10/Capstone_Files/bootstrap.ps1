<#
    Bootstrap the Day 10 capstone end-to-end. Idempotent — safe to re-run.

    Steps:
      1. Verify .env exists; copy from .env.example if not.
      2. Ensure plugin JARs are present (calls debezium\download_plugins.ps1).
      3. docker compose up -d (16 services).
      4. Poll for health: kafka, schema-registry, connect, minio, airflow webserver.
      5. Create MinIO buckets.
      6. Register Avro schemas.
      7. Register all Kafka Connect connectors.
      8. Unpause continuous DAGs.
      9. Print URLs.
#>

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "[ok] copied .env.example -> .env"
}

if (-not (Test-Path "monitoring\jmx_exporter\jmx_prometheus_javaagent.jar") `
    -or -not (Test-Path "debezium\plugins\confluentinc-kafka-connect-s3")) {
    Write-Host "[step] downloading Kafka Connect plugins + JMX agent..."
    .\debezium\download_plugins.ps1
}

# Build the local image first so Compose stops trying to pull `day10-app` from
# Docker Hub (it doesn't exist there). `app-base` is the template the
# app / ingestor-* services share via `image: day10-app:latest`.
Write-Host "[step] building local app image (day10-app:latest)..."
docker compose --env-file .env --progress=plain build app-base
if ($LASTEXITCODE -ne 0) { throw "docker compose build failed" }

# Pull ONLY services that have a remote image. Listing them explicitly avoids:
#   1) the "pull access denied for day10-app" warning that fires when Compose
#      walks every service (the 5 day10-app-derived services are silently
#      skipped because we don't list them), and
#   2) Compose's interactive TTY progress renderer flooding PowerShell with
#      thousands of redraw lines (`--progress=plain` prints one event per line).
$remoteServices = @(
    "zookeeper","kafka","schema-registry","connect","kafka-ui",
    "config-db","postgres-exporter","minio",
    "airflow-db",
    "prometheus","alertmanager","grafana"
)
Write-Host "[step] pulling $($remoteServices.Count) remote images (first run can be slow)..."

# Registry can flake (TLS handshake timeout etc). Retry up to 3 attempts with
# backoff. Each attempt re-runs the whole pull, so already-cached layers are
# skipped almost instantly — the cost is small and the run survives blips.
$maxAttempts = 3
for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
    docker compose --env-file .env --progress=plain pull @remoteServices
    if ($LASTEXITCODE -eq 0) { break }
    if ($attempt -lt $maxAttempts) {
        $sleep = 10 * $attempt
        Write-Host "[warn] pull attempt $attempt failed (exit $LASTEXITCODE); retrying in ${sleep}s..."
        Start-Sleep -Seconds $sleep
    } else {
        Write-Host "[warn] pull failed after $maxAttempts attempts - continuing; 'compose up -d' will retry missing images."
    }
}

Write-Host "[step] docker compose up -d..."
docker compose --env-file .env --progress=plain up -d
if ($LASTEXITCODE -ne 0) { throw "docker compose up -d failed" }

function Wait-Http($name, $url, $timeoutSec = 240) {
    Write-Host -NoNewline "[wait] $name "
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $r = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 3
            if ($r.StatusCode -lt 500) { Write-Host "ok"; return }
        } catch {}
        Write-Host -NoNewline "."
        Start-Sleep -Seconds 3
    }
    Write-Host ""
    throw "$name not healthy at $url after $timeoutSec s"
}

Wait-Http "minio"           "http://localhost:9000/minio/health/live"
Wait-Http "schema-registry" "http://localhost:8081/subjects"
Wait-Http "kafka-connect"   "http://localhost:8083/"
Wait-Http "airflow"         "http://localhost:8080/health"

Write-Host "[step] python helpers..."
$envDict = @{}
Get-Content .env | Where-Object { $_ -notmatch '^\s*#' -and $_ -match '=' } | ForEach-Object {
    $kv = $_ -split '=', 2
    $envDict[$kv[0]] = $kv[1]
}
$env:MINIO_ROOT_USER     = $envDict.MINIO_ROOT_USER
$env:MINIO_ROOT_PASSWORD = $envDict.MINIO_ROOT_PASSWORD
$env:MINIO_ENDPOINT      = "http://localhost:9000"
$env:BRONZE_BUCKET       = $envDict.BRONZE_BUCKET
$env:SILVER_BUCKET       = $envDict.SILVER_BUCKET
$env:GOLD_BUCKET         = $envDict.GOLD_BUCKET
$env:SCHEMA_REGISTRY_URL = "http://localhost:8081"
$env:KAFKA_CONNECT_URL   = "http://localhost:8083"

# Try host python first; fall back to docker exec airflow.
function Run-Py($script) {
    try {
        python $script
    } catch {
        Write-Host "[fallback] running $script inside airflow-webserver container"
        docker exec airflow-webserver python $script
    }
}

Run-Py "minio/bootstrap.py"
Run-Py "app/register_app_schemas.py"
Run-Py "debezium/register_all_connectors.py"

Write-Host "[step] unpausing DAGs..."
foreach ($dag in @("15_config_drift","30_hop_medallion","40_data_quality","50_business_kpis")) {
    docker exec airflow-webserver airflow dags unpause $dag | Out-Null
    Write-Host "  - $dag"
}

Write-Host ""
Write-Host "============================================================"
Write-Host " Day 10 capstone is up. URLs:"
Write-Host "   Airflow             http://localhost:8080  (airflow/airflow)"
Write-Host "   Kafka UI            http://localhost:8088"
Write-Host "   Schema Registry     http://localhost:8081"
Write-Host "   Kafka Connect REST  http://localhost:8083"
Write-Host "   MinIO console       http://localhost:9001  (minioadmin/minioadmin)"
Write-Host "   Apache Hop Web      http://localhost:8089"
Write-Host "   Quality dashboard   http://localhost:5001"
Write-Host "   Business dashboard  http://localhost:5002"
Write-Host "   Live map (gliders)  http://localhost:5003"
Write-Host "   Prometheus          http://localhost:9090"
Write-Host "   Grafana             http://localhost:3000  (admin/admin)"
Write-Host "   Alertmanager        http://localhost:9093"
Write-Host "============================================================"
Write-Host "Run smoke test:  python tests\smoke_test.py"

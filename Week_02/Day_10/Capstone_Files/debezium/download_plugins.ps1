# Downloads Kafka Connect plugins into ./plugins so the `connect` container
# can find them on startup. Idempotent — skips files that already exist.
#
# Plugins:
#  - confluentinc-kafka-connect-s3 (S3 sink for MinIO)
#  - debezium-connector-postgres   (already bundled in cp-kafka-connect 7.5.0+
#                                   via Confluent Hub component, but we ship
#                                   our own to keep the version pinned)

$ErrorActionPreference = "Stop"

$pluginsDir = Join-Path $PSScriptRoot "plugins"
New-Item -ItemType Directory -Path $pluginsDir -Force | Out-Null

function Get-And-Extract($name, $url, $sha256 = $null) {
    # Match either the literal $name dir or any "$name-*" versioned dir.
    $existing = Get-ChildItem -Path $pluginsDir -Directory -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -eq $name -or $_.Name -like "$name-*" } |
        Select-Object -First 1
    if ($existing) {
        Write-Host "[skip] $name (already present as $($existing.Name))"
        return
    }

    $isTar = $url -match '\.(tar\.gz|tgz)$'
    $ext   = if ($isTar) { ".tar.gz" } else { ".zip" }
    $tmp   = Join-Path $env:TEMP "$name$ext"

    Write-Host "[get ] $url"
    Invoke-WebRequest -Uri $url -OutFile $tmp
    if ($sha256) {
        $actual = (Get-FileHash $tmp -Algorithm SHA256).Hash.ToLower()
        if ($actual -ne $sha256.ToLower()) {
            throw "checksum mismatch for $name (expected $sha256, got $actual)"
        }
    }

    $target = Join-Path $pluginsDir $name
    Write-Host "[ext ] -> $target"
    if ($isTar) {
        New-Item -ItemType Directory -Path $target -Force | Out-Null
        # tar.exe ships with Windows 10+; --strip-components flattens the
        # single top-level dir inside the Debezium tarball.
        & tar.exe -xzf $tmp -C $target --strip-components=1
        if ($LASTEXITCODE -ne 0) { throw "tar extraction failed for $name" }
    } else {
        Expand-Archive -Path $tmp -DestinationPath $pluginsDir -Force
    }
    Remove-Item $tmp
}

# Confluent S3 Sink connector (Confluent Hub CDN)
Get-And-Extract `
    -name "confluentinc-kafka-connect-s3" `
    -url  "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-s3/versions/10.5.13/confluentinc-kafka-connect-s3-10.5.13.zip"

# Debezium PostgreSQL connector
Get-And-Extract `
    -name "debezium-connector-postgres" `
    -url  "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.5.4.Final/debezium-connector-postgres-2.5.4.Final-plugin.tar.gz"

Write-Host ""
Write-Host "Plugins ready in $pluginsDir"
Get-ChildItem $pluginsDir -Directory | ForEach-Object { Write-Host "  - $($_.Name)" }

# --- JMX exporter Java agent (Kafka loads it via KAFKA_OPTS) -----------------
$jmxJar = Join-Path $PSScriptRoot "..\monitoring\jmx_exporter\jmx_prometheus_javaagent.jar"
if (-not (Test-Path $jmxJar)) {
    Write-Host ""
    Write-Host "[get ] JMX exporter agent"
    Invoke-WebRequest `
        -Uri "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar" `
        -OutFile $jmxJar
    Write-Host "[ok  ] $jmxJar"
} else {
    Write-Host "[skip] JMX exporter agent already present"
}

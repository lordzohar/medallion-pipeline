#!/usr/bin/env bash
# Linux/Mac twin of download_plugins.ps1
set -euo pipefail

PLUGINS_DIR="$(cd "$(dirname "$0")" && pwd)/plugins"
mkdir -p "$PLUGINS_DIR"

fetch_zip() {
    local name="$1"
    local url="$2"
    local target="$PLUGINS_DIR/$name"
    if [ -d "$target" ]; then
        echo "[skip] $name (already present)"
        return
    fi
    local tmp
    tmp="$(mktemp --suffix=.zip)"
    echo "[get ] $url"
    curl -fsSL "$url" -o "$tmp"
    echo "[ext ] -> $target"
    unzip -q "$tmp" -d "$PLUGINS_DIR"
    rm -f "$tmp"
}

fetch_tgz() {
    local name="$1"
    local url="$2"
    local target="$PLUGINS_DIR/$name"
    if [ -d "$target" ]; then
        echo "[skip] $name (already present)"
        return
    fi
    local tmp
    tmp="$(mktemp --suffix=.tgz)"
    echo "[get ] $url"
    curl -fsSL "$url" -o "$tmp"
    echo "[ext ] -> $target"
    mkdir -p "$target"
    tar -xzf "$tmp" -C "$target" --strip-components=1
    rm -f "$tmp"
}

fetch_zip "confluentinc-kafka-connect-s3" \
    "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-s3/versions/10.5.13/confluentinc-kafka-connect-s3-10.5.13.zip"

fetch_tgz "debezium-connector-postgres" \
    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.5.4.Final/debezium-connector-postgres-2.5.4.Final-plugin.tar.gz"

echo
echo "Plugins ready in $PLUGINS_DIR"
ls -1 "$PLUGINS_DIR"

# --- JMX exporter Java agent ------------------------------------------------
JMX_JAR="$(cd "$(dirname "$0")/.." && pwd)/monitoring/jmx_exporter/jmx_prometheus_javaagent.jar"
if [ ! -f "$JMX_JAR" ]; then
    echo
    echo "[get ] JMX exporter agent"
    curl -fsSL \
        "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar" \
        -o "$JMX_JAR"
    echo "[ok  ] $JMX_JAR"
else
    echo "[skip] JMX exporter agent already present"
fi

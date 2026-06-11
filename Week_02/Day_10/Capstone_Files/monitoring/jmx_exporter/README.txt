# Placeholder.
# The JMX exporter JAR is downloaded into this folder at bootstrap time by
# debezium/download_plugins.{ps1,sh} (or you can drop it in manually):
#
#   https://github.com/prometheus/jmx_exporter/releases/download/0.20.0/jmx_prometheus_javaagent-0.20.0.jar
#
# It must be named exactly: jmx_prometheus_javaagent.jar
#
# docker-compose mounts ./monitoring/jmx_exporter into /opt/jmx_exporter/
# inside the kafka container and Kafka loads it via KAFKA_OPTS.

FROM apache/hop:2.10.0 AS hop-runtime

FROM python:3.11-slim-bookworm

ENV DEPLOYMENT_PATH=/opt/hop \
    VOLUME_MOUNT_POINT=/files \
    HOP_LOG_LEVEL=Basic \
    HOP_FILE_PATH= \
    HOP_LOG_PATH=/opt/hop/hop.err.log \
    HOP_SHARED_JDBC_FOLDERS= \
    HOP_PROJECT_NAME=day10 \
    HOP_PROJECT_DIRECTORY= \
    HOP_PROJECT_FOLDER= \
    HOP_PROJECT_CONFIG_FILE_NAME=project.json \
    HOP_ENVIRONMENT_NAME=day10 \
    HOP_ENVIRONMENT_CONFIG_FILE_NAME_PATHS= \
    HOP_RUN_CONFIG= \
    HOP_RUN_PARAMETERS= \
    HOP_RUN_METADATA_EXPORT= \
    HOP_SYSTEM_PROPERTIES= \
    HOP_OPTIONS="-Xmx768m -Xms256m" \
    HOP_CUSTOM_ENTRYPOINT_EXTENSION_SHELL_FILE_PATH= \
    HOP_SERVER_USER=cluster \
    HOP_SERVER_PASSWORD=cluster \
    HOP_SERVER_HOSTNAME=0.0.0.0 \
    HOP_SERVER_PORT=8080 \
    HOP_SERVER_SHUTDOWNPORT=8079 \
    HOP_SERVER_METADATA_FOLDER= \
    HOP_SERVER_KEYSTORE= \
    HOP_SERVER_KEYSTORE_PASSWORD= \
    HOP_SERVER_KEY_PASSWORD= \
    HOP_SERVER_MAX_LOG_LINES= \
    HOP_SERVER_MAX_LOG_TIMEOUT= \
    HOP_SERVER_MAX_OBJECT_TIMEOUT=

RUN apt-get update \
 && apt-get install -y --no-install-recommends bash openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --gid 1000 hop \
 && useradd --uid 1000 --gid 1000 --create-home --home-dir /home/hop hop

COPY --from=hop-runtime --chown=hop:hop /opt/hop /opt/hop

# Python reference transforms live under /files/project/transforms (mounted).
COPY requirements.txt /tmp/req.txt
RUN pip install --no-cache-dir -r /tmp/req.txt

WORKDIR /home/hop
USER hop
EXPOSE 8080
ENTRYPOINT ["/bin/bash", "/opt/hop/run.sh"]

FROM apache/hop:2.10.0

# Install Python + libs so the same container can also run the reference
# transforms (used by the Airflow `HopOperator` when hop-run is not desired).
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip python3-venv \
 && rm -rf /var/lib/apt/lists/*

# Python reference transforms live under /files/project/transforms (mounted).
COPY requirements.txt /tmp/req.txt
RUN pip3 install --no-cache-dir --break-system-packages -r /tmp/req.txt

ENV HOP_OPTIONS="-Xmx768m -Xms256m"
EXPOSE 8080

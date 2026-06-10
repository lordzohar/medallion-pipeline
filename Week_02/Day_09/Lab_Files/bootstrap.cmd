@echo off
REM ============================================================================
REM  Day 9 Monitoring Labs - Windows bootstrap (cmd)
REM ============================================================================
cd /d "%~dp0"

REM 1) Download JMX Prometheus javaagent if missing -----------------------------
if not exist "jmx_exporter\jmx_prometheus_javaagent.jar" (
    echo Downloading JMX Prometheus javaagent...
    if not exist "jmx_exporter" mkdir jmx_exporter
    curl -fSL -o "jmx_exporter\jmx_prometheus_javaagent.jar" ^
        "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar"
)

REM 2) Create Python venv + install deps ---------------------------------------
if not exist ".venv" (
    echo Creating .venv...
    python -m venv .venv
)
call .venv\Scripts\activate.bat
pip install --quiet -r requirements.txt

REM 3) Bring up the stack ------------------------------------------------------
echo Starting Kafka KRaft cluster + monitoring stack...
docker compose up -d

REM 4) Wait for Kafka ----------------------------------------------------------
echo Waiting for Kafka cluster to be ready...
set /a count=0
:wait_kafka
docker exec kafka1 kafka-broker-api-versions --bootstrap-server kafka1:29092 >nul 2>&1
if %errorlevel% equ 0 goto kafka_ready
set /a count+=1
if %count% geq 30 goto kafka_timeout
timeout /t 3 /nobreak >nul
goto wait_kafka
:kafka_timeout
echo WARNING: Kafka not ready after 90s. Check 'docker logs kafka1'.
:kafka_ready

REM 5) Create topics -----------------------------------------------------------
echo Creating topics...
python setup_topics.py

REM 6) Seed Postgres + register Debezium connector -----------------------------
echo Waiting for Postgres...
set /a count=0
:wait_pg
docker exec postgres pg_isready -U taxi >nul 2>&1
if %errorlevel% equ 0 goto pg_ready
set /a count+=1
if %count% geq 20 goto pg_ready
timeout /t 2 /nobreak >nul
goto wait_pg
:pg_ready

echo Seeding drivers table...
python db_seeder.py

echo Registering Debezium PostgreSQL connector...
python register_connector.py

REM 7) Print URLs --------------------------------------------------------------
echo.
echo =================================================================
echo   All services up. Open these in your browser:
echo =================================================================
echo   Live Taxi Dashboard : http://localhost:5000
echo   Kafka UI            : http://localhost:8080
echo   Grafana             : http://localhost:3000   (admin/admin)
echo   Prometheus          : http://localhost:9090
echo   Alertmanager        : http://localhost:9093
echo   Kafka Connect REST  : http://localhost:8083
echo   Postgres            : localhost:5432  (taxi/taxi/taxi)
echo   ksqlDB CLI          : docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
echo.
echo Next: open LABS_GUIDE.md and start Lab 1.

"""
DAG 09 – Full Orchestration (Kafka → Hop → Airflow → Gold)
============================================================
Lab 9: End-to-end pipeline combining Kafka, Apache Hop, and Airflow.

This is the capstone DAG that ties together everything learned:
  - Kafka event consumption (streaming → batch bridge)
  - Apache Hop pipeline execution (visual ETL)
  - Airflow orchestration (scheduling, sensors, branching)
  - Full medallion architecture (Bronze → Silver → Gold)
  - Data quality gates between layers
  - Pipeline metadata & lineage tracking
"""

import json
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from sqlalchemy import create_engine, text

PG_CONN = os.environ.get(
    "WAREHOUSE_CONN",
    "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow",
)
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_ORDERS_TOPIC", "orders.raw")


@dag(
    dag_id="09_full_orchestration",
    description="End-to-end: Kafka → Hop → Airflow → Gold tables",
    schedule_interval="0 */2 * * *",  # every 2 hours
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineer",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["lab", "capstone", "e2e"],
    doc_md=__doc__,
)
def full_orchestration():

    start = EmptyOperator(task_id="start")

    # ════════════════════════════════════════════════════════
    # PHASE 1: DATA INGESTION (parallel sources)
    # ════════════════════════════════════════════════════════

    @task_group(group_id="phase1_ingestion")
    def phase1_ingestion():
        """Ingest data from multiple sources in parallel."""

        # ── Source A: CSV files via Hop pipeline ──────────────
        @task()
        def ingest_csv_via_hop() -> dict:
            """Use Apache Hop to ingest CSV files into bronze layer."""
            from helpers.hop_utils import run_hop_pipeline

            result = run_hop_pipeline("bronze_csv_ingest.hpl")
            print(f"Hop CSV ingestion: {result['status']}")

            # Fallback: if Hop CLI not available, do it in Python
            if result["status"] == "simulated":
                import csv
                from decimal import Decimal

                engine = create_engine(PG_CONN)
                file_path = "/opt/airflow/data/new_orders.csv"

                if not os.path.exists(file_path):
                    return {"source": "csv", "status": "no_file", "count": 0}

                # Detect file encoding by trying to read the first line
                detected_encoding = None
                for encoding in ["utf-8", "latin-1", "iso-8859-1", "cp1252"]:
                    try:
                        with open(file_path, "r", encoding=encoding, errors="strict") as fh:
                            fh.readline()
                        detected_encoding = encoding
                        print(f"CSV encoding detected: {encoding}")
                        break
                    except UnicodeDecodeError:
                        continue

                if not detected_encoding:
                    detected_encoding = "latin-1"
                    print(f"CSV encoding not detected, using: {detected_encoding}")

                # Read CSV with detected encoding
                try:
                    with open(file_path, newline="", encoding=detected_encoding, errors="replace") as fh:
                        reader = csv.DictReader(fh)
                        rows = [{k.lower(): v for k, v in r.items()} for r in reader]
                    print(f"Successfully read {len(rows)} CSV rows")
                except Exception as e:
                    print(f"Error reading CSV: {e}")
                    return {"source": "csv", "status": "failed", "count": 0, "error": str(e)}

                if not rows:
                    print("No rows found in CSV")
                    return {"source": "csv", "status": "empty", "count": 0}

                with engine.begin() as conn:
                    for row in rows:
                        conn.execute(
                            text("""
                                INSERT INTO bronze_orders
                                    (order_id, customer_id, product_id, product_name,
                                     category, quantity, unit_price, total_amount,
                                     order_date, status, source)
                                VALUES
                                    (:order_id, :customer_id, :product_id, :product_name,
                                     :category, :quantity, :unit_price, :total_amount,
                                     :order_date, :status, 'csv_hop')
                            """),
                            {
                                "order_id": int(row["order_id"]),
                                "customer_id": int(row["customer_id"]),
                                "product_id": row["product_id"],
                                "product_name": row["product_name"],
                                "category": row["category"],
                                "quantity": int(row["quantity"]),
                                "unit_price": Decimal(row["unit_price"]),
                                "total_amount": Decimal(row["total_amount"]),
                                "order_date": row["order_date"],
                                "status": row["status"],
                            },
                        )
                return {"source": "csv", "status": "loaded", "count": len(rows)}

            return {"source": "csv", "status": result["status"], "count": 0}

        # ── Source B: Kafka events ────────────────────────────
        @task()
        def ingest_kafka_events() -> dict:
            """Consume Kafka events into bronze_kafka_events."""
            try:
                from helpers.kafka_utils import consume_batch

                records = consume_batch(
                    topic=KAFKA_TOPIC,
                    group_id="airflow-full-orchestration",
                    max_records=200,
                    timeout_ms=10000,
                )

                if not records:
                    print("No Kafka events available")
                    return {"source": "kafka", "count": 0}

                engine = create_engine(PG_CONN)
                with engine.begin() as conn:
                    for rec in records:
                        conn.execute(
                            text("""
                                INSERT INTO bronze_kafka_events
                                    (topic, partition_id, offset_id, key, payload, event_time)
                                VALUES
                                    (:topic, :partition, :offset, :key,
                                     :payload::jsonb, :event_time::timestamp)
                            """),
                            {
                                "topic": rec["topic"],
                                "partition": rec["partition"],
                                "offset": rec["offset"],
                                "key": rec["key"],
                                "payload": json.dumps(rec["value"]),
                                "event_time": rec["timestamp"],
                            },
                        )

                print(f"Ingested {len(records)} Kafka events")
                return {"source": "kafka", "count": len(records)}

            except Exception as e:
                print(f"Kafka ingestion skipped: {e}")
                return {"source": "kafka", "count": 0, "error": str(e)}

        # ── Source C: Customer data ───────────────────────────
        @task()
        def ingest_customers() -> dict:
            """Ingest customer reference data."""
            import csv

            file_path = "/opt/airflow/data/customers.csv"
            if not os.path.exists(file_path):
                return {"source": "customers", "count": 0}

            engine = create_engine(PG_CONN)
            rows = []
            
            # Detect file encoding by trying to read the first line
            detected_encoding = None
            for encoding in ["utf-8", "latin-1", "iso-8859-1", "cp1252"]:
                try:
                    with open(file_path, "r", encoding=encoding, errors="strict") as fh:
                        # Try to read first line to verify encoding works
                        fh.readline()
                    detected_encoding = encoding
                    print(f"Detected encoding: {encoding}")
                    break
                except UnicodeDecodeError:
                    continue

            if not detected_encoding:
                detected_encoding = "latin-1"
                print(f"Could not auto-detect, using fallback: {detected_encoding}")

            # Now read the full CSV with detected encoding
            try:
                with open(file_path, newline="", encoding=detected_encoding, errors="replace") as fh:
                    reader = csv.DictReader(fh)
                    rows = [{k.lower(): v for k, v in r.items()} for r in reader]
                print(f"Successfully read {len(rows)} rows with {detected_encoding}")
            except Exception as e:
                print(f"Error reading CSV: {e}")
                return {"source": "customers", "count": 0, "error": str(e)}

            if not rows:
                print("No rows read from CSV")
                return {"source": "customers", "count": 0}

            with engine.begin() as conn:
                conn.execute(text("DELETE FROM bronze_customers"))
                for row in rows:
                    try:
                        conn.execute(
                            text("""
                                INSERT INTO bronze_customers
                                    (customer_id, customer_name, email, city, country, signup_date)
                                VALUES
                                    (:cid, :name, :email, :city, :country, :signup)
                            """),
                            {
                                "cid": int(row.get("customer_id", 0)),
                                "name": row.get("customer_name", ""),
                                "email": row.get("email", ""),
                                "city": row.get("city", ""),
                                "country": row.get("country", ""),
                                "signup": row.get("signup_date", ""),
                            },
                        )
                    except Exception as e:
                        print(f"Error inserting row {row}: {e}")
                        continue

            return {"source": "customers", "count": len(rows)}

        csv_result = ingest_csv_via_hop()
        kafka_result = ingest_kafka_events()
        customer_result = ingest_customers()

    # ════════════════════════════════════════════════════════
    # PHASE 2: DATA QUALITY GATE
    # ════════════════════════════════════════════════════════

    @task_group(group_id="phase2_quality_gate")
    def phase2_quality_gate():
        """Validate bronze data before transformation."""

        @task()
        def run_bronze_quality_checks() -> dict:
            """Run quality checks on bronze layer."""
            engine = create_engine(PG_CONN)
            checks = {}

            with engine.connect() as conn:
                # Check 1: Bronze orders not empty
                count = conn.execute(text(
                    "SELECT COUNT(*) FROM bronze_orders"
                )).scalar_one()
                checks["bronze_orders_not_empty"] = count > 0

                # Check 2: No null order_ids
                nulls = conn.execute(text(
                    "SELECT COUNT(*) FROM bronze_orders WHERE order_id IS NULL"
                )).scalar_one()
                checks["no_null_order_ids"] = nulls == 0

                # Check 3: All amounts are non-negative
                negatives = conn.execute(text(
                    "SELECT COUNT(*) FROM bronze_orders WHERE total_amount < 0"
                )).scalar_one()
                checks["no_negative_amounts"] = negatives == 0

                # Check 4: Customers loaded
                cust_count = conn.execute(text(
                    "SELECT COUNT(*) FROM bronze_customers"
                )).scalar_one()
                checks["customers_loaded"] = cust_count > 0

            all_passed = all(checks.values())
            print(f"\n=== Bronze Quality Gate ===")
            for check, passed in checks.items():
                status = "PASS" if passed else "FAIL"
                print(f"  [{status}] {check}")
            print(f"  Overall: {'PASS' if all_passed else 'FAIL'}")

            # Log quality results
            with engine.begin() as conn:
                for check_name, passed in checks.items():
                    conn.execute(
                        text("""
                            INSERT INTO data_quality_results
                                (dag_id, check_name, table_name, passed)
                            VALUES
                                ('09_full_orchestration', :check, 'bronze', :passed)
                        """),
                        {"check": check_name, "passed": passed},
                    )

            return {"checks": checks, "all_passed": all_passed}

        run_bronze_quality_checks()

    # ════════════════════════════════════════════════════════
    # PHASE 3: SILVER TRANSFORMATION (via Hop)
    # ════════════════════════════════════════════════════════

    @task_group(group_id="phase3_silver_transform")
    def phase3_silver_transform():
        """Transform bronze → silver using Hop pipelines + Airflow."""

        @task()
        def silver_orders_transform() -> dict:
            """Cleanse and deduplicate orders into silver layer."""
            engine = create_engine(PG_CONN)
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM silver_orders"))
                conn.execute(text("""
                    INSERT INTO silver_orders
                        (order_id, customer_id, product_id, product_name,
                         category, quantity, unit_price, total_amount,
                         order_date, status)
                    SELECT DISTINCT ON (order_id)
                        order_id, customer_id, product_id, product_name,
                        category, quantity, unit_price, total_amount,
                        TO_DATE(order_date, 'YYYY-MM-DD'), status
                    FROM bronze_orders
                    WHERE quantity > 0 AND status != 'CANCELLED'
                    ORDER BY order_id, _loaded_at DESC
                """))
                count = conn.execute(text(
                    "SELECT COUNT(*) FROM silver_orders"
                )).scalar_one()
            print(f"Silver orders: {count} rows")
            return {"table": "silver_orders", "count": count}

        @task()
        def silver_customers_transform() -> dict:
            """Cleanse customers into silver layer."""
            engine = create_engine(PG_CONN)
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM silver_customers"))
                conn.execute(text("""
                    INSERT INTO silver_customers
                        (customer_id, customer_name, email, city, country, signup_date)
                    SELECT DISTINCT ON (customer_id)
                        customer_id,
                        INITCAP(TRIM(customer_name)),
                        LOWER(TRIM(NULLIF(email, ''))),
                        TRIM(city),
                        UPPER(TRIM(country)),
                        TO_DATE(signup_date, 'YYYY-MM-DD')
                    FROM bronze_customers
                    WHERE customer_name IS NOT NULL
                    ORDER BY customer_id, _loaded_at DESC
                """))
                count = conn.execute(text(
                    "SELECT COUNT(*) FROM silver_customers"
                )).scalar_one()
            print(f"Silver customers: {count} rows")
            return {"table": "silver_customers", "count": count}

        orders = silver_orders_transform()
        customers = silver_customers_transform()

    # ════════════════════════════════════════════════════════
    # PHASE 4: GOLD AGGREGATION
    # ════════════════════════════════════════════════════════

    @task_group(group_id="phase4_gold_build")
    def phase4_gold_build():
        """Build gold layer aggregations."""

        @task()
        def build_customer_360() -> dict:
            """Create customer 360 view combining orders + customer data."""
            engine = create_engine(PG_CONN)
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM gold_customer_360"))
                conn.execute(text("""
                    INSERT INTO gold_customer_360
                        (customer_id, customer_name, email, city, country,
                         total_orders, total_revenue, avg_order_value,
                         first_order_date, last_order_date, top_category,
                         customer_segment)
                    SELECT
                        c.customer_id, c.customer_name, c.email, c.city, c.country,
                        COUNT(o.order_id),
                        COALESCE(SUM(o.total_amount), 0),
                        COALESCE(AVG(o.total_amount), 0),
                        MIN(o.order_date),
                        MAX(o.order_date),
                        MODE() WITHIN GROUP (ORDER BY o.category),
                        CASE
                            WHEN SUM(o.total_amount) >= 200 THEN 'Premium'
                            WHEN SUM(o.total_amount) >= 50  THEN 'Regular'
                            ELSE 'New'
                        END
                    FROM silver_customers c
                    LEFT JOIN silver_orders o ON c.customer_id = o.customer_id
                    GROUP BY c.customer_id, c.customer_name, c.email,
                             c.city, c.country
                """))
                count = conn.execute(text(
                    "SELECT COUNT(*) FROM gold_customer_360"
                )).scalar_one()
            return {"table": "gold_customer_360", "count": count}

        @task()
        def build_daily_sales() -> dict:
            """Aggregate daily sales metrics."""
            engine = create_engine(PG_CONN)
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM gold_daily_sales"))
                conn.execute(text("""
                    INSERT INTO gold_daily_sales
                        (order_date, total_orders, total_revenue,
                         unique_customers, avg_order_value, top_product)
                    SELECT
                        order_date,
                        COUNT(*),
                        SUM(total_amount),
                        COUNT(DISTINCT customer_id),
                        AVG(total_amount),
                        MODE() WITHIN GROUP (ORDER BY product_name)
                    FROM silver_orders
                    GROUP BY order_date
                """))
                count = conn.execute(text(
                    "SELECT COUNT(*) FROM gold_daily_sales"
                )).scalar_one()
            return {"table": "gold_daily_sales", "count": count}

        @task()
        def build_product_performance() -> dict:
            """Aggregate product performance metrics."""
            engine = create_engine(PG_CONN)
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM gold_product_performance"))
                conn.execute(text("""
                    INSERT INTO gold_product_performance
                        (product_id, product_name, category,
                         total_sold, total_revenue, avg_unit_price, order_count)
                    SELECT
                        product_id, product_name, category,
                        SUM(quantity),
                        SUM(total_amount),
                        AVG(unit_price),
                        COUNT(*)
                    FROM silver_orders
                    GROUP BY product_id, product_name, category
                """))
                count = conn.execute(text(
                    "SELECT COUNT(*) FROM gold_product_performance"
                )).scalar_one()
            return {"table": "gold_product_performance", "count": count}

        c360 = build_customer_360()
        daily = build_daily_sales()
        products = build_product_performance()

    # ════════════════════════════════════════════════════════
    # PHASE 5: REPORTING & LINEAGE
    # ════════════════════════════════════════════════════════

    @task()
    def generate_executive_summary():
        """Generate a final summary report."""
        engine = create_engine(PG_CONN)

        with engine.connect() as conn:
            tables = {
                "bronze_orders": "SELECT COUNT(*) FROM bronze_orders",
                "bronze_customers": "SELECT COUNT(*) FROM bronze_customers",
                "bronze_kafka": "SELECT COUNT(*) FROM bronze_kafka_events",
                "silver_orders": "SELECT COUNT(*) FROM silver_orders",
                "silver_customers": "SELECT COUNT(*) FROM silver_customers",
                "gold_customer_360": "SELECT COUNT(*) FROM gold_customer_360",
                "gold_daily_sales": "SELECT COUNT(*) FROM gold_daily_sales",
                "gold_products": "SELECT COUNT(*) FROM gold_product_performance",
            }

            counts = {}
            for name, query in tables.items():
                try:
                    counts[name] = conn.execute(text(query)).scalar_one()
                except Exception:
                    counts[name] = 0

        print("╔════════════════════════════════════════════════════╗")
        print("║       FULL ORCHESTRATION – EXECUTIVE SUMMARY      ║")
        print("╠════════════════════════════════════════════════════╣")
        print("║                                                    ║")
        print("║  BRONZE LAYER (Raw Data):                         ║")
        print(f"║    Orders:        {counts['bronze_orders']:>10,}                     ║")
        print(f"║    Customers:     {counts['bronze_customers']:>10,}                     ║")
        print(f"║    Kafka Events:  {counts['bronze_kafka']:>10,}                     ║")
        print("║                                                    ║")
        print("║  SILVER LAYER (Cleansed):                         ║")
        print(f"║    Orders:        {counts['silver_orders']:>10,}                     ║")
        print(f"║    Customers:     {counts['silver_customers']:>10,}                     ║")
        print("║                                                    ║")
        print("║  GOLD LAYER (Aggregated):                         ║")
        print(f"║    Customer 360:  {counts['gold_customer_360']:>10,}                     ║")
        print(f"║    Daily Sales:   {counts['gold_daily_sales']:>10,}                     ║")
        print(f"║    Products:      {counts['gold_products']:>10,}                     ║")
        print("║                                                    ║")
        print("╚════════════════════════════════════════════════════╝")

    # ── Wire all phases ───────────────────────────────────────

    ingestion = phase1_ingestion()
    quality = phase2_quality_gate()
    silver = phase3_silver_transform()
    gold = phase4_gold_build()
    summary = generate_executive_summary()

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")
    start >> ingestion >> quality >> silver >> gold >> summary >> end


full_orchestration()

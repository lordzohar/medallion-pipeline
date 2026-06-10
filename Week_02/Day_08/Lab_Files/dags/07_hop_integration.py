"""
DAG 07 – Apache Hop Integration
=================================
Lab 7: Trigger Apache Hop pipelines and workflows from Airflow.

Concepts covered:
  - Running Hop pipelines via DockerOperator
  - Running Hop via BashOperator (docker exec)
  - Custom Hop utility functions
  - Hop pipeline result handling
  - Mixed orchestration: Airflow tasks + Hop transforms
  - Passing parameters from Airflow to Hop
"""

import json
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from helpers.hop_utils import build_hop_docker_command, run_hop_pipeline

PG_CONN = os.environ.get(
    "WAREHOUSE_CONN",
    "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow",
)


@dag(
    dag_id="07_hop_integration",
    description="Orchestrate Apache Hop pipelines from Airflow",
    schedule_interval=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args={
        "owner": "data-engineer",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["lab", "hop", "integration"],
    doc_md=__doc__,
)
def hop_integration():

    start = EmptyOperator(task_id="start")

    # ── Method 1: BashOperator with docker exec ──────────────
    # This sends a command to the already-running hop-cli container
    # to execute a Hop pipeline. Most straightforward approach.

    @task_group(group_id="hop_via_docker_exec")
    def hop_docker_exec():
        """Run Hop pipelines by exec-ing into the hop-cli container."""

        run_bronze_pipeline = BashOperator(
            task_id="run_bronze_csv_ingest",
            bash_command="""
                echo "=== Running Hop Pipeline: bronze_csv_ingest.hpl ==="
                echo "Command: docker exec day8-hop-cli {{ params.hop_cmd }}"
                echo ""
                # In production, uncomment the docker exec command:
                # docker exec day8-hop-cli {{ params.hop_cmd }}
                echo "Pipeline: bronze_csv_ingest.hpl"
                echo "Status:   SIMULATED (uncomment docker exec in production)"
                echo "The Hop pipeline would:"
                echo "  1. Read /project/data/new_orders.csv"
                echo "  2. Validate & cleanse fields"
                echo "  3. Write to bronze_orders table in PostgreSQL"
            """,
            params={
                "hop_cmd": build_hop_docker_command(
                    "pipelines/bronze_csv_ingest.hpl"
                ),
            },
        )

        run_silver_pipeline = BashOperator(
            task_id="run_silver_transform",
            bash_command="""
                echo "=== Running Hop Pipeline: silver_transform.hpl ==="
                echo "Command: docker exec day8-hop-cli {{ params.hop_cmd }}"
                echo ""
                echo "Pipeline: silver_transform.hpl"
                echo "Status:   SIMULATED"
                echo "The Hop pipeline would:"
                echo "  1. Read from bronze_orders table"
                echo "  2. Filter invalid/cancelled orders"
                echo "  3. Deduplicate by order_id"
                echo "  4. Cast dates and normalize fields"
                echo "  5. Write to silver_orders table"
            """,
            params={
                "hop_cmd": build_hop_docker_command(
                    "pipelines/silver_transform.hpl"
                ),
            },
        )

        run_bronze_pipeline >> run_silver_pipeline

    # ── Method 2: PythonOperator calling Hop utility ──────────
    # Uses the hop_utils helper to execute pipelines programmatically

    @task_group(group_id="hop_via_python")
    def hop_python_approach():
        """Run Hop pipelines using Python helper functions."""

        @task()
        def run_hop_bronze():
            """Execute the bronze ingestion Hop pipeline."""
            result = run_hop_pipeline("bronze_csv_ingest.hpl")
            print(f"Hop pipeline result: {json.dumps(result, indent=2)}")
            return result

        @task()
        def run_hop_silver(bronze_result: dict):
            """Execute the silver transformation Hop pipeline."""
            print(f"Bronze pipeline status: {bronze_result['status']}")
            result = run_hop_pipeline("silver_transform.hpl")
            print(f"Hop pipeline result: {json.dumps(result, indent=2)}")
            return result

        @task()
        def run_hop_gold_aggregation(silver_result: dict):
            """Execute the gold aggregation Hop pipeline."""
            print(f"Silver pipeline status: {silver_result['status']}")
            result = run_hop_pipeline("gold_aggregation.hpl")
            print(f"Hop pipeline result: {json.dumps(result, indent=2)}")
            return result

        bronze = run_hop_bronze()
        silver = run_hop_silver(bronze)
        gold = run_hop_gold_aggregation(silver)

    # ── Method 3: Hybrid – Airflow tasks + Hop transforms ────
    # Use Airflow for orchestration logic and Hop for complex transforms

    @task_group(group_id="hybrid_approach")
    def hybrid_airflow_hop():
        """Combine Airflow's scheduling with Hop's transformation power."""

        @task()
        def pre_check() -> dict:
            """Airflow task: validate prerequisites before Hop runs."""
            from sqlalchemy import create_engine, text

            engine = create_engine(PG_CONN)
            checks = {}
            with engine.connect() as conn:
                for table in ["bronze_orders", "silver_orders"]:
                    try:
                        count = conn.execute(
                            text(f"SELECT COUNT(*) FROM {table}")
                        ).scalar_one()
                        checks[table] = count
                    except Exception:
                        checks[table] = 0

            all_ok = all(v >= 0 for v in checks.values())
            print(f"Pre-checks: {checks}")
            print(f"Ready for Hop: {all_ok}")
            return {"checks": checks, "ready": all_ok}

        hop_workflow = BashOperator(
            task_id="run_hop_medallion_workflow",
            bash_command="""
                echo "=== Hop Workflow: day8_medallion_batch.hwf ==="
                echo ""
                echo "This workflow orchestrates multiple Hop pipelines:"
                echo "  1. bronze_csv_ingest.hpl   → Load raw data"
                echo "  2. silver_transform.hpl    → Cleanse & deduplicate"
                echo "  3. gold_aggregation.hpl    → Build aggregates"
                echo ""
                echo "Command would be:"
                echo "  docker exec day8-hop-cli /opt/hop/hop-run.sh \\\\"
                echo "    --file /project/workflows/day8_medallion_batch.hwf \\\\"
                echo "    --project day8_airflow_hop \\\\"
                echo "    --runconfig local"
                echo ""
                echo "Status: SIMULATED - open Hop Web UI at http://localhost:8082 to run"
            """,
        )

        @task()
        def post_validate() -> dict:
            """Airflow task: validate Hop pipeline results."""
            from sqlalchemy import create_engine, text

            engine = create_engine(PG_CONN)
            results = {}
            with engine.connect() as conn:
                for table in ["bronze_orders", "silver_orders", "gold_customer_360"]:
                    try:
                        count = conn.execute(
                            text(f"SELECT COUNT(*) FROM {table}")
                        ).scalar_one()
                        results[table] = count
                    except Exception:
                        results[table] = 0

            print(f"\n=== Post-Hop Validation ===")
            for table, count in results.items():
                status = "OK" if count > 0 else "EMPTY"
                print(f"  [{status:5s}] {table}: {count} rows")

            return results

        checks = pre_check()
        checks >> hop_workflow >> post_validate()

    # ── Log the integration run ───────────────────────────────
    log_integration = PostgresOperator(
        task_id="log_hop_integration",
        postgres_conn_id="warehouse_postgres",
        sql="""
            INSERT INTO pipeline_execution_log
                (dag_id, run_id, task_id, layer, table_name,
                 records_processed, status, started_at)
            VALUES
                ('07_hop_integration', '{{ run_id }}', 'summary',
                 'all', 'hop_integration', 0, 'success', CURRENT_TIMESTAMP);
        """,
    )

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # ── Dependencies ──────────────────────────────────────────
    docker_tasks = hop_docker_exec()
    python_tasks = hop_python_approach()
    hybrid_tasks = hybrid_airflow_hop()

    start >> docker_tasks
    start >> python_tasks
    start >> hybrid_tasks

    [docker_tasks, python_tasks, hybrid_tasks] >> log_integration >> end


hop_integration()

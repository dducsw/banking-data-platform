"""
Airflow DAG: End-to-End Banking Lakehouse Medallion Pipeline.
Self-contained under pipeline/dags/ running standalone jobs.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="banking_medallion_pipeline",
    default_args=default_args,
    description="Orchestrates Bronze Postgres Ingest -> Silver Stage Dim/Fact -> Gold Curated Marts -> DQ Suite",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["banking", "medallion", "iceberg", "lakehouse"],
) as dag:

    # 1. Bronze Ingestion
    task_bronze_ingest = BashOperator(
        task_id="bronze_postgres_ingest",
        bash_command="python -m pipeline.jobs.bronze.ingest_postgres",
    )

    # 2. Silver Transformations Demo
    task_silver = BashOperator(
        task_id="silver_transform_demo",
        bash_command="python -m pipeline.jobs.silver.demo",
    )

    # 3. Gold Aggregations Demo
    task_gold = BashOperator(
        task_id="gold_aggregate_demo",
        bash_command="python -m pipeline.jobs.gold.demo",
    )

    # 4. Data Quality Assertions
    task_quality = BashOperator(
        task_id="lakehouse_data_quality",
        bash_command="python -m pipeline.jobs.quality.data_quality",
    )

    # Graph dependencies: Bronze -> Silver -> Gold -> Data Quality
    task_bronze_ingest >> task_silver >> task_gold >> task_quality

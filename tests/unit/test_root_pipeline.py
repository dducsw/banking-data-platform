"""
Unit tests for the root pipeline package.
Verifies Stage and Gold jobs, configuration mapping, schemas, and lifecycle execution.
"""

from unittest.mock import MagicMock
import pytest

from pipeline.config.pipeline_config import TABLE_CONFIGS, CORE_BANKING_TABLES
from pipeline.jobs.silver.demo import StageDimCustomersJob
from pipeline.jobs.gold.demo import GoldDailyBranchSummaryJob
from pipeline.schemas.bronze_schemas import BRONZE_ALL_DDLS
from pipeline.schemas.silver_schemas import SILVER_ALL_DDLS
from pipeline.schemas.gold_schemas import GOLD_ALL_DDLS


def test_pipeline_config_tables():
    assert "customers" in CORE_BANKING_TABLES
    assert "transactions" in CORE_BANKING_TABLES
    assert "dim_customers" in TABLE_CONFIGS
    assert "fact_transactions" in TABLE_CONFIGS


def test_stage_dim_customers_job_init():
    job = StageDimCustomersJob()
    assert job.pipeline_layer == "silver"
    assert job.table_name == "dim_customers"
    assert job.primary_key == "customer_id"
    assert "customers" in job.source_table
    assert "dim_customers" in job.target_table


def test_gold_daily_branch_summary_job_init():
    job = GoldDailyBranchSummaryJob()
    assert job.pipeline_layer == "gold"
    assert job.table_name == "daily_branch_summary"
    assert "txn_date" in job.partition_by


def test_bronze_postgres_job_init():
    from pipeline.jobs.bronze.ingest_postgres import BronzePostgresIngestJob
    job = BronzePostgresIngestJob("customers")
    assert job.pipeline_layer == "bronze"
    assert job.table_name == "customers"
    assert "postgres.customers" == job.source_table
    assert "lakehouse.bronze.customers" == job.target_table
    assert job.source_system == "core_banking_postgres"


def test_bronze_kafka_job_init():
    from pipeline.jobs.bronze.ingest_kafka import BronzeKafkaStreamingJob
    job = BronzeKafkaStreamingJob()
    assert job.pipeline_layer == "bronze"
    assert job.table_name == "streaming_transactions"
    assert "kafka.banking.public.transactions" == job.source_table
    assert job.source_system == "kafka_cdc_stream"


def test_schemas_ddl_coverage():
    assert len(BRONZE_ALL_DDLS) >= 3
    assert len(SILVER_ALL_DDLS) >= 3
    assert len(GOLD_ALL_DDLS) >= 2
    for ddl in BRONZE_ALL_DDLS + SILVER_ALL_DDLS + GOLD_ALL_DDLS:
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        assert "USING iceberg" in ddl

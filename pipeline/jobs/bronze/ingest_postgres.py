"""
Bronze Layer Pipeline: Ingest Core Banking PostgreSQL tables into Iceberg Bronze tables.
Inherits from BaseIcebergJob for unified extract, validate, load, audit, and metadata lifecycle.
"""

from datetime import datetime, timezone
from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession

from pipeline.common.base_job import BaseIcebergJob, WriteMode
from pipeline.config.pipeline_config import CORE_BANKING_TABLES, CATALOG_NAME, NAMESPACE_BRONZE
from pipeline.config.settings import settings
from pipeline.common.logger import get_logger

logger = get_logger("BronzePostgresIngestion")


class BronzePostgresIngestJob(BaseIcebergJob):
    """
    Bronze Ingestion Job for a single PostgreSQL table.
    Inherits lifecycle management, auditing, and metadata tagging from BaseIcebergJob.
    """

    def __init__(self, table_name: str, target_namespace: str = NAMESPACE_BRONZE):
        self.raw_table_name = table_name
        self.target_namespace = target_namespace
        super().__init__(
            pipeline_layer="bronze",
            table_name=table_name,
            source_table=f"postgres.{table_name}",
            target_table=f"{CATALOG_NAME}.{target_namespace}.{table_name}",
            write_mode=WriteMode.OVERWRITE,
            source_system="core_banking_postgres",
        )

    def extract(self, spark: SparkSession) -> DataFrame:
        """Extracts table via PostgreSQL JDBC."""
        self.logger.info(f"Extracting [{self.raw_table_name}] from PostgreSQL ({settings.postgres.jdbc_url})...")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{self.target_namespace}")
        return (
            spark.read.format("jdbc")
            .option("url", settings.postgres.jdbc_url)
            .option("dbtable", self.raw_table_name)
            .option("user", settings.postgres.username)
            .option("password", settings.postgres.password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )


def ingest_table(spark: SparkSession, table_name: str, target_namespace: str = NAMESPACE_BRONZE) -> int:
    """Helper executing a single table ingestion job."""
    job = BronzePostgresIngestJob(table_name=table_name, target_namespace=target_namespace)
    return job.run(spark=spark)


def run_bronze_ingestion(spark: Optional[SparkSession] = None, tables: Optional[List[str]] = None) -> None:
    """Runs ingestion across all specified core banking tables."""
    should_stop = False
    if spark is None:
        active = SparkSession.getActiveSession()
        if active is not None:
            spark = active
        else:
            from pipeline.common.spark_session import get_spark_session
            spark = get_spark_session(app_name="BronzePostgresIngestion")
            should_stop = True

    try:
        tables_to_ingest = tables or CORE_BANKING_TABLES
        total_rows = 0
        start_time = datetime.now(timezone.utc)

        logger.info(f"Starting Bronze Ingestion for {len(tables_to_ingest)} tables using BaseIcebergJob...")
        for t in tables_to_ingest:
            try:
                cnt = ingest_table(spark, t)
                total_rows += cnt
            except Exception as e:
                logger.error(f"Skipping or failed table {t}: {e}")

        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info(f"Bronze Ingestion completed: {total_rows:,} total rows ingested in {elapsed:.2f}s.")
    finally:
        if should_stop:
            spark.stop()


if __name__ == "__main__":
    run_bronze_ingestion()

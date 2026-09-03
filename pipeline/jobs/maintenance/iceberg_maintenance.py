"""
Iceberg Lakehouse Table Maintenance & Compaction.
Standardized standalone job under pipeline/jobs/maintenance/.
"""

from typing import List, Optional
from pyspark.sql import SparkSession

from pipeline.config.pipeline_config import CATALOG_NAME, NAMESPACE_BRONZE, NAMESPACE_SILVER, NAMESPACE_GOLD
from pipeline.common.logger import get_logger

logger = get_logger("IcebergMaintenance")

MAINTENANCE_TABLES: List[str] = [
    f"{NAMESPACE_BRONZE}.transactions",
    f"{NAMESPACE_BRONZE}.accounts",
    f"{NAMESPACE_BRONZE}.customers",
    f"{NAMESPACE_SILVER}.fact_transactions",
    f"{NAMESPACE_SILVER}.dim_customers",
    f"{NAMESPACE_SILVER}.dim_accounts",
    f"{NAMESPACE_GOLD}.daily_branch_summary",
    f"{NAMESPACE_GOLD}.customer_360_summary",
]


def expire_snapshots(spark: SparkSession, table_name: str, retention_days: int = 7) -> None:
    """Expires Iceberg snapshots older than retention days."""
    logger.info(f"Expiring snapshots older than {retention_days} days on [{CATALOG_NAME}.{table_name}]...")
    try:
        spark.sql(f"""
            CALL {CATALOG_NAME}.system.expire_snapshots(
                table => '{CATALOG_NAME}.{table_name}',
                older_than => TIMESTAMP '${{date_sub(current_timestamp(), {retention_days})}}',
                retain_last => 5
            )
        """)
        logger.info(f"[OK] Expired old snapshots for [{CATALOG_NAME}.{table_name}].")
    except Exception as e:
        logger.warning(f"Could not expire snapshots on [{table_name}] (may have few snapshots): {e}")


def remove_orphan_files(spark: SparkSession, table_name: str, older_than_days: int = 3) -> None:
    """Removes orphan data files not referenced in table metadata."""
    logger.info(f"Removing orphan files for [{CATALOG_NAME}.{table_name}]...")
    try:
        spark.sql(f"""
            CALL {CATALOG_NAME}.system.remove_orphan_files(
                table => '{CATALOG_NAME}.{table_name}',
                older_than => TIMESTAMP '${{date_sub(current_timestamp(), {older_than_days})}}'
            )
        """)
        logger.info(f"[OK] Removed orphan files for [{CATALOG_NAME}.{table_name}].")
    except Exception as e:
        logger.warning(f"Could not remove orphan files on [{table_name}]: {e}")


def compact_data_files(spark: SparkSession, table_name: str) -> None:
    """Compacts small files into optimal size files (binpack)."""
    logger.info(f"Compacting data files for [{CATALOG_NAME}.{table_name}]...")
    try:
        spark.sql(f"""
            CALL {CATALOG_NAME}.system.rewrite_data_files(
                table => '{CATALOG_NAME}.{table_name}',
                strategy => 'binpack',
                options => map('max-file-size-bytes', '536870912')
            )
        """)
        logger.info(f"[OK] Compacted small files for [{CATALOG_NAME}.{table_name}].")
    except Exception as e:
        logger.warning(f"Could not compact data files on [{table_name}]: {e}")


def run_full_maintenance(spark: Optional[SparkSession] = None, tables: Optional[List[str]] = None) -> None:
    """Runs end-to-end Iceberg maintenance: expire -> orphan files -> compaction."""
    should_stop = False
    if spark is None:
        from pipeline.common.spark_session import get_spark_session
        spark = get_spark_session(app_name="LakehouseIcebergMaintenance")
        should_stop = True

    try:
        target_tables = tables or MAINTENANCE_TABLES
        logger.info(f"Starting Iceberg Table Maintenance for {len(target_tables)} tables...")

        for tbl in target_tables:
            logger.info(f"--- Maintaining Table: {tbl} ---")
            expire_snapshots(spark, tbl, retention_days=7)
            remove_orphan_files(spark, tbl, older_than_days=3)
            compact_data_files(spark, tbl)

        logger.info("Lakehouse Maintenance Completed Successfully.")
    finally:
        if should_stop:
            spark.stop()


if __name__ == "__main__":
    run_full_maintenance()

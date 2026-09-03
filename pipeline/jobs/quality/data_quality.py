"""
Automated Data Quality & Assertion Suite for Lakehouse Tables.
Standardized standalone job under pipeline/jobs/quality/.
"""

from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pipeline.config.pipeline_config import CATALOG_NAME, NAMESPACE_SILVER, NAMESPACE_GOLD
from pipeline.common.logger import get_logger

logger = get_logger("LakehouseDataQuality")


def check_not_null(spark: SparkSession, table_name: str, column_name: str) -> bool:
    """Verifies that a required column has zero null values."""
    df = spark.table(f"{CATALOG_NAME}.{table_name}")
    null_count = df.filter(col(column_name).isNull()).count()
    if null_count > 0:
        logger.error(f"[DQ FAIL] Table [{table_name}] column [{column_name}] contains {null_count:,} NULL values.")
        return False
    logger.info(f"[DQ PASS] Table [{table_name}] column [{column_name}] has 0 NULLs.")
    return True


def check_uniqueness(spark: SparkSession, table_name: str, primary_key: str) -> bool:
    """Verifies primary key uniqueness."""
    df = spark.table(f"{CATALOG_NAME}.{table_name}")
    total_count = df.count()
    distinct_count = df.select(primary_key).distinct().count()
    if total_count != distinct_count:
        logger.error(
            f"[DQ FAIL] Table [{table_name}] PK [{primary_key}] has duplicates: total={total_count:,}, distinct={distinct_count:,}"
        )
        return False
    logger.info(f"[DQ PASS] Table [{table_name}] PK [{primary_key}] is unique ({distinct_count:,} rows).")
    return True


def check_positive_values(spark: SparkSession, table_name: str, amount_column: str) -> bool:
    """Verifies numeric values are strictly positive."""
    df = spark.table(f"{CATALOG_NAME}.{table_name}")
    non_positive = df.filter(col(amount_column) <= 0).count()
    if non_positive > 0:
        logger.error(
            f"[DQ FAIL] Table [{table_name}] column [{amount_column}] has {non_positive:,} non-positive values."
        )
        return False
    logger.info(f"[DQ PASS] Table [{table_name}] column [{amount_column}] contains only positive values.")
    return True


def run_full_quality_suite(spark: Optional[SparkSession] = None) -> bool:
    """Runs standard banking data quality assertions suite."""
    should_stop = False
    if spark is None:
        from pipeline.common.spark_session import get_spark_session
        spark = get_spark_session(app_name="LakehouseDataQualitySuite")
        should_stop = True

    try:
        logger.info("Executing Lakehouse Data Quality Assertions...")
        checks = [
            check_not_null(spark, f"{NAMESPACE_SILVER}.dim_customers", "customer_id"),
            check_uniqueness(spark, f"{NAMESPACE_SILVER}.dim_customers", "customer_id"),
            check_not_null(spark, f"{NAMESPACE_SILVER}.dim_accounts", "account_id"),
            check_uniqueness(spark, f"{NAMESPACE_SILVER}.dim_accounts", "account_id"),
            check_not_null(spark, f"{NAMESPACE_SILVER}.fact_transactions", "transaction_id"),
            check_uniqueness(spark, f"{NAMESPACE_SILVER}.fact_transactions", "transaction_id"),
            check_positive_values(spark, f"{NAMESPACE_SILVER}.fact_transactions", "amount"),
            check_not_null(spark, f"{NAMESPACE_GOLD}.daily_branch_summary", "branch_code"),
            check_not_null(spark, f"{NAMESPACE_GOLD}.customer_360_summary", "customer_id"),
        ]

        passed = all(checks)
        if passed:
            logger.info("[ALL DQ CHECKS PASSED] Data quality verified across Silver & Gold Lakehouse layers.")
        else:
            logger.warning("[DQ VIOLATIONS ENCOUNTERED] One or more assertions failed.")
        return passed
    finally:
        if should_stop:
            spark.stop()


if __name__ == "__main__":
    success = run_full_quality_suite()
    exit(0 if success else 1)

"""
Pipeline Watermark State Management for Apache Iceberg Lakehouse.
Tracks incremental high-watermarks for batch and micro-batch stream ingestion.
"""

from datetime import datetime, timezone
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from pipeline.config.pipeline_config import CATALOG_NAME, NAMESPACE_METADATA
from pipeline.common.logger import get_logger

logger = get_logger("WatermarkManager")

WATERMARK_TABLE = "pipeline_watermark"
FULL_WATERMARK_TABLE = f"{CATALOG_NAME}.{NAMESPACE_METADATA}.{WATERMARK_TABLE}"


def get_watermark_schema():
    try:
        from pyspark.sql.types import (
            StructType as PySparkStructType,
            StructField as PySparkStructField,
            StringType as PySparkStringType,
            TimestampType as PySparkTimestampType,
        )

        return PySparkStructType(
            [
                PySparkStructField("table_name", PySparkStringType(), False),
                PySparkStructField("watermark_column", PySparkStringType(), False),
                PySparkStructField("last_watermark_value", PySparkStringType(), False),
                PySparkStructField("last_updated_at", PySparkTimestampType(), False),
                PySparkStructField("status", PySparkStringType(), False),
            ]
        )
    except Exception:
        return "table_name STRING, watermark_column STRING, last_watermark_value STRING, last_updated_at TIMESTAMP, status STRING"


def ensure_watermark_table(spark: SparkSession) -> None:
    """Ensures metadata namespace and pipeline_watermark table exist in Iceberg."""
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{NAMESPACE_METADATA}")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {FULL_WATERMARK_TABLE} (
                table_name STRING NOT NULL,
                watermark_column STRING NOT NULL,
                last_watermark_value STRING NOT NULL,
                last_updated_at TIMESTAMP NOT NULL,
                status STRING NOT NULL
            )
            USING iceberg
            """
        )
    except Exception as e:
        logger.warning(f"Could not initialize watermark table {FULL_WATERMARK_TABLE}: {e}")


def get_watermark(spark: SparkSession, table_name: str) -> Optional[str]:
    """Retrieves the latest successful watermark value for a given table."""
    try:
        ensure_watermark_table(spark)
        df = (
            spark.table(FULL_WATERMARK_TABLE)
            .filter(f"table_name = '{table_name}' AND status = 'SUCCESS'")
            .sort("last_updated_at", ascending=False)
        )
        if df.rdd.isEmpty():
            return None
        return df.select("last_watermark_value").first()[0]
    except Exception as e:
        logger.warning(f"Failed to retrieve watermark for table {table_name}: {e}")
        return None


def update_watermark(
    spark: SparkSession,
    table_name: str,
    watermark_column: str = "execution_timestamp",
    last_watermark_value: Optional[str] = None,
    status: str = "SUCCESS",
) -> None:
    """Inserts or updates the watermark state for a table in Iceberg."""
    ensure_watermark_table(spark)
    now = datetime.now(timezone.utc)
    if last_watermark_value is None:
        last_watermark_value = now.strftime("%Y-%m-%d %H:%M:%S")

    row = [(table_name, watermark_column, str(last_watermark_value), now, status)]
    df_new = spark.createDataFrame(row, schema=get_watermark_schema())

    try:
        df_new.createOrReplaceTempView("new_watermark")
        spark.sql(
            f"""
            MERGE INTO {FULL_WATERMARK_TABLE} t
            USING new_watermark s
            ON t.table_name = s.table_name
            WHEN MATCHED THEN
                UPDATE SET
                    t.watermark_column = s.watermark_column,
                    t.last_watermark_value = s.last_watermark_value,
                    t.last_updated_at = s.last_updated_at,
                    t.status = s.status
            WHEN NOT MATCHED THEN
                INSERT (table_name, watermark_column, last_watermark_value, last_updated_at, status)
                VALUES (s.table_name, s.watermark_column, s.last_watermark_value, s.last_updated_at, s.status)
            """
        )
        logger.info(f"Updated watermark for [{table_name}] -> {last_watermark_value} (status: {status})")
    except Exception as e:
        logger.warning(f"MERGE INTO failed ({e}), falling back to append...")
        try:
            df_new.write.format("iceberg").mode("append").saveAsTable(FULL_WATERMARK_TABLE)
        except Exception as inner_e:
            logger.error(f"Failed to record watermark for {table_name}: {inner_e}")

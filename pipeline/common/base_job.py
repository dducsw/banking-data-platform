"""
Base Iceberg Job template using the Template Method Pattern.
Orchestrates standardized extract, validate, transform, load, audit, and watermark lifecycle.
Supports Iceberg ACID MERGE INTO (upsert), dynamic partition overwrite, append, and full overwrite.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession

from pipeline.common.watermark import get_watermark, update_watermark
from pipeline.common.audit import start_audit, PipelineAuditRecord
from pipeline.common.logger import get_logger


class WriteMode(str, Enum):
    OVERWRITE = "overwrite"
    APPEND = "append"
    MERGE = "merge"
    DYNAMIC_OVERWRITE = "dynamic_overwrite"


class BaseIcebergJob(ABC):
    """
    Template Method Base Class for Lakehouse Pipelines on Apache Iceberg.
    """

    def __init__(
        self,
        pipeline_layer: str,
        table_name: str,
        source_table: str,
        target_table: str,
        primary_key: Optional[str] = None,
        merge_keys: Optional[List[str]] = None,
        partition_by: Optional[List[str]] = None,
        write_mode: WriteMode = WriteMode.OVERWRITE,
        watermark_col: Optional[str] = None,
        source_system: Optional[str] = None,
    ):
        self.pipeline_layer = pipeline_layer
        self.table_name = table_name
        self.source_table = source_table
        self.target_table = target_table
        self.primary_key = primary_key
        self.merge_keys = merge_keys or ([primary_key] if primary_key else [])
        self.partition_by = partition_by or []
        self.write_mode = write_mode
        self.watermark_col = watermark_col or f"_{pipeline_layer}_processed_at"
        self.source_system = source_system or source_table
        self.logger = get_logger(f"{pipeline_layer.capitalize()}_{table_name}")

    def inject_metadata(self, df: DataFrame) -> DataFrame:
        """Injects automatic metadata lineage columns: <layer>_processed_at and source_system."""
        try:
            from pyspark.sql.functions import current_timestamp, lit

            processed_col = f"_{self.pipeline_layer}_processed_at"
            if processed_col not in df.columns:
                df = df.withColumn(processed_col, current_timestamp())

            if "source_system" not in df.columns:
                df = df.withColumn("source_system", lit(self.source_system))
        except Exception as e:
            self.logger.warning(f"Could not inject metadata columns: {e}")
        return df

    def get_last_watermark(self, spark: SparkSession) -> Optional[str]:
        """Retrieves last recorded high-watermark value for incremental processing."""
        return get_watermark(spark, self.table_name)

    def extract(self, spark: SparkSession) -> DataFrame:
        """Step 1: Extract data from source table."""
        self.logger.info(f"Extracting from source table: {self.source_table}")
        return spark.table(self.source_table)

    def validate(self, df: DataFrame) -> None:
        """Step 2: Early data contract and quality validations."""
        if df.rdd.isEmpty():
            raise ValueError(f"Job Failed: Source table '{self.source_table}' is empty or unreadable!")

        if self.primary_key and self.primary_key in df.columns:
            null_count = df.filter(f"{self.primary_key} IS NULL").count()
            if null_count > 0:
                self.logger.warning(
                    f"Quality Warning: Found {null_count} rows with null PK '{self.primary_key}' in {self.source_table}"
                )

    def transform(self, df: DataFrame) -> DataFrame:
        """Step 3: Business cleansing, normalization, and aggregation logic. Defaults to pass-through."""
        return df

    def load_merge(self, spark: SparkSession, df: DataFrame) -> int:
        """
        Executes ACID Iceberg MERGE INTO for idempotent upserts.
        Matches on merge_keys and updates all matching rows or inserts new ones.
        """
        if not self.merge_keys:
            raise ValueError(
                f"WriteMode.MERGE requires 'primary_key' or 'merge_keys' for table {self.target_table}"
            )

        # Temporary view scoped to current job execution
        temp_view = f"staging_{self.table_name}_{int(datetime.now().timestamp())}"
        df.createOrReplaceTempView(temp_view)

        join_keys = list(dict.fromkeys(self.merge_keys))
        on_clause = " AND ".join([f"t.{k} = s.{k}" for k in join_keys])

        merge_sql = f"""
        MERGE INTO {self.target_table} t
        USING {temp_view} s
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        self.logger.info(f"Executing Iceberg MERGE INTO: {self.target_table} ON ({on_clause})")
        spark.sql(merge_sql)
        return df.count()

    def load_dynamic_overwrite(self, spark: SparkSession, df: DataFrame) -> int:
        """
        Dynamically overwrites only the partitions present in the current DataFrame,
        leaving unreferenced partitions untouched.
        """
        self.logger.info(f"Executing Iceberg DYNAMIC OVERWRITE on {self.target_table} (Partitions: {self.partition_by})")
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        writer = df.write.format("iceberg").mode("overwrite")
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)
        writer.saveAsTable(self.target_table)
        return df.count()

    def load(self, spark: SparkSession, df: DataFrame) -> int:
        """Step 4: Load transformed DataFrame into target Apache Iceberg table based on WriteMode."""
        self.logger.info(
            f"Loading into Iceberg: {self.target_table} | Mode: {self.write_mode.value} | Partitions: {self.partition_by}"
        )

        if self.write_mode == WriteMode.MERGE:
            return self.load_merge(spark, df)

        if self.write_mode == WriteMode.DYNAMIC_OVERWRITE:
            return self.load_dynamic_overwrite(spark, df)

        writer = df.write.format("iceberg")
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)

        if self.write_mode == WriteMode.APPEND:
            writer = writer.mode("append")
        else:
            writer = writer.mode("overwrite")

        writer.saveAsTable(self.target_table)
        return df.count()

    def run(self, spark: Optional[SparkSession] = None) -> int:
        """Executes full Template Method lifecycle."""
        should_stop_spark = False
        if spark is None:
            from pipeline.common.spark_session import get_spark_session
            spark = get_spark_session(f"{self.pipeline_layer.capitalize()}_{self.table_name}")
            should_stop_spark = True

        audit: PipelineAuditRecord = start_audit(
            pipeline_name=f"{self.pipeline_layer.capitalize()}Pipeline",
            stage=self.pipeline_layer,
            target_table=self.target_table,
        )

        try:
            df_in = self.extract(spark)
            audit.rows_read = df_in.count()

            self.validate(df_in)
            df_transformed = self.transform(df_in)
            df_out = self.inject_metadata(df_transformed)

            rows_written = self.load(spark, df_out)
            audit.mark_completed(rows_written)

            if self.watermark_col:
                update_watermark(
                    spark=spark,
                    table_name=self.table_name,
                    watermark_column=self.watermark_col,
                    last_watermark_value=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    status="SUCCESS",
                )

            self.logger.info(f"SUCCESS: Processed {rows_written:,} rows for {self.target_table}")
            return rows_written

        except Exception as e:
            audit.mark_failed(e)
            self.logger.error(f"FAILED: Error running {self.table_name}: {e}")
            raise
        finally:
            if should_stop_spark:
                spark.stop()

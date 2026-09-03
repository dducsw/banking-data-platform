"""
Silver Layer Demo: Standardized Data Cleansing & Deduplication on Apache Iceberg.
Demonstrates BaseIcebergJob inheritance with WriteMode.MERGE and automatic metadata lineage.
"""

from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_date, upper, trim, coalesce, lit

from pipeline.common.base_job import BaseIcebergJob, WriteMode
from pipeline.config.pipeline_config import TABLE_CONFIGS


class StageDimCustomersJob(BaseIcebergJob):
    """Silver Customer Dimension Job demonstrating SCD1 Iceberg Upsert."""

    def __init__(self):
        cfg = TABLE_CONFIGS["dim_customers"]
        super().__init__(
            pipeline_layer="silver",
            table_name="dim_customers",
            primary_key=cfg["primary_key"],
            merge_keys=[cfg["primary_key"]],
            source_table=cfg["source_table"],
            target_table=cfg["target_table"],
            write_mode=WriteMode.MERGE,
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """Applies business cleansing; metadata is injected automatically by BaseIcebergJob."""
        return (
            df.filter(col("customer_id").isNotNull())
            .dropDuplicates(["customer_id"])
            .withColumn("first_name", trim(col("first_name")))
            .withColumn("last_name", trim(col("last_name")))
            .withColumn("full_name", trim(col("first_name") + lit(" ") + col("last_name")))
            .withColumn("gender", upper(trim(col("gender"))))
            .withColumn("country", upper(trim(col("country"))))
            .withColumn("date_of_birth", to_date(col("date_of_birth")))
            .withColumn("customer_since", to_date(col("customer_since")))
            .withColumn("annual_income", col("annual_income").cast("decimal(18,2)"))
            .withColumn("is_active", coalesce(col("is_active"), lit(True)))
        )


def main(spark: Optional[SparkSession] = None) -> int:
    return StageDimCustomersJob().run(spark=spark)


if __name__ == "__main__":
    main()

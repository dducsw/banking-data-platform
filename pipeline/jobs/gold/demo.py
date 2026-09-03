"""
Gold Layer Demo: Business Aggregations & Marts on Apache Iceberg.
Demonstrates BaseIcebergJob inheritance with WriteMode.DYNAMIC_OVERWRITE and automatic metadata lineage.
"""

from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum as _sum, count as _count, when, lit

from pipeline.common.base_job import BaseIcebergJob, WriteMode
from pipeline.config.pipeline_config import TABLE_CONFIGS, CATALOG_NAME, NAMESPACE_SILVER


class GoldDailyBranchSummaryJob(BaseIcebergJob):
    """Daily Branch Performance Mart Job demonstrating Dynamic Partition Overwrite."""

    def __init__(self):
        cfg = TABLE_CONFIGS["daily_branch_summary"]
        super().__init__(
            pipeline_layer="gold",
            table_name="daily_branch_summary",
            source_table=cfg["source_table"],
            target_table=cfg["target_table"],
            partition_by=cfg["partition_by"],
            write_mode=WriteMode.DYNAMIC_OVERWRITE,
        )

    def extract(self, spark: SparkSession) -> DataFrame:
        """Joins Silver fact and dimension tables."""
        df_txns = spark.table(f"{CATALOG_NAME}.{NAMESPACE_SILVER}.fact_transactions")
        df_accounts = spark.table(f"{CATALOG_NAME}.{NAMESPACE_SILVER}.dim_accounts")
        return df_txns.join(df_accounts, "account_id")

    def transform(self, df: DataFrame) -> DataFrame:
        """Calculates branch aggregations; metadata is injected automatically by BaseIcebergJob."""
        return (
            df.groupBy("branch_code", "txn_date")
            .agg(
                _count("transaction_id").alias("total_transactions"),
                _sum(when(col("direction") == "CREDIT", col("amount")).otherwise(lit(0))).alias("total_inflow_amount"),
                _sum(when(col("direction") == "DEBIT", col("amount")).otherwise(lit(0))).alias("total_outflow_amount"),
                _sum(col("amount")).alias("total_gross_volume"),
                _sum(when(col("is_fraud") == True, lit(1)).otherwise(lit(0))).alias("fraud_txn_count"),
                _sum(when(col("is_disputed") == True, lit(1)).otherwise(lit(0))).alias("disputed_txn_count"),
            )
            .withColumn("net_flow_amount", col("total_inflow_amount") - col("total_outflow_amount"))
        )


def main(spark: Optional[SparkSession] = None) -> int:
    return GoldDailyBranchSummaryJob().run(spark=spark)


if __name__ == "__main__":
    main()

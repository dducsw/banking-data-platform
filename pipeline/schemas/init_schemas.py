"""
CLI utility to bootstrap Apache Iceberg namespaces and DDL tables via Spark SQL.
"""

import argparse
from typing import Optional
from pyspark.sql import SparkSession

from pipeline.schemas.bronze_schemas import BRONZE_ALL_DDLS, BRONZE_DB_NAME
from pipeline.schemas.silver_schemas import SILVER_ALL_DDLS, SILVER_DB_NAME
from pipeline.schemas.gold_schemas import GOLD_ALL_DDLS, GOLD_DB_NAME
from pipeline.common.logger import get_logger

logger = get_logger("InitSchemas")


def init_all_schemas(layer: str = "all", spark: Optional[SparkSession] = None) -> None:
    """Executes Spark SQL DDL statements for specified layer or all layers."""
    should_stop = False
    if spark is None:
        from pipeline.common.spark_session import get_spark_session
        spark = get_spark_session(app_name="InitLakehouseSchemas")
        should_stop = True

    databases_and_ddls = []

    if layer in ("all", "bronze"):
        databases_and_ddls.append((BRONZE_DB_NAME, BRONZE_ALL_DDLS))
    if layer in ("all", "silver"):
        databases_and_ddls.append((SILVER_DB_NAME, SILVER_ALL_DDLS))
    if layer in ("all", "gold"):
        databases_and_ddls.append((GOLD_DB_NAME, GOLD_ALL_DDLS))

    try:
        for db_name, ddls in databases_and_ddls:
            logger.info(f"Ensuring namespace exists: {db_name}")
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {db_name}")

            for ddl in ddls:
                first_line = [line.strip() for line in ddl.strip().split("\n") if line.strip()][0]
                logger.info(f"Executing: {first_line}")
                spark.sql(ddl)

        logger.info("All requested Iceberg schemas initialized successfully!")
    finally:
        if should_stop:
            spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Initialize Apache Iceberg Lakehouse DDL schemas via Spark SQL")
    parser.add_argument(
        "--layer",
        type=str,
        choices=["all", "bronze", "silver", "gold"],
        default="all",
        help="Target Lakehouse layer to initialize",
    )
    args = parser.parse_args()
    init_all_schemas(layer=args.layer)


if __name__ == "__main__":
    main()

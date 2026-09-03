"""
Example Script: Read and query Apache Iceberg Lakehouse tables via Gravitino REST Catalog.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum


def read_iceberg_tables(spark: SparkSession) -> None:
    print("=== Querying Iceberg Tables via Gravitino REST Catalog ===")

    # List tables in catalog
    print("\n--> Tables in default database:")
    spark.sql("SHOW TABLES IN lakehouse.banking").show()

    # Query customer table
    df_customers = spark.table("lakehouse.banking.customers")
    print(f"\n--> Schema of lakehouse.banking.customers:")
    df_customers.printSchema()

    print(f"\n--> Total rows: {df_customers.count()}")
    df_customers.show()

    # Query Iceberg Table Snapshots Metadata
    print("\n--> Iceberg Table Snapshots:")
    spark.sql("SELECT * FROM lakehouse.banking.customers.snapshots").show(truncate=False)

    # Query Iceberg Table History
    print("\n--> Iceberg Table History:")
    spark.sql("SELECT * FROM lakehouse.banking.customers.history").show(truncate=False)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("SparkReadIcebergExample").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    read_iceberg_tables(spark)

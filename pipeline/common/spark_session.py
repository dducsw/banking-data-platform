"""
SparkSession factory configured for Iceberg, Gravitino REST Catalog, PostgreSQL JDBC, and MinIO S3A.
"""

from pyspark.sql import SparkSession

from pipeline.config.settings import settings


def get_spark_session(app_name: str = "BankingLakehousePipeline", local_mode: bool = False) -> SparkSession:
    """
    Returns a configured SparkSession for Iceberg REST Catalog & S3A data lakehouse operations.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        # Iceberg SQL Extensions & Catalogs (Gravitino Iceberg REST)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{settings.spark.catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{settings.spark.catalog_name}.type", "rest")
        .config(f"spark.sql.catalog.{settings.spark.catalog_name}.uri", settings.gravitino.iceberg_rest_uri)
        .config(f"spark.sql.catalog.{settings.spark.catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{settings.spark.catalog_name}.s3.endpoint", settings.minio.endpoint)
        .config(f"spark.sql.catalog.{settings.spark.catalog_name}.s3.access-key-id", settings.minio.access_key)
        .config(f"spark.sql.catalog.{settings.spark.catalog_name}.s3.secret-access-key", settings.minio.secret_key)
        .config(f"spark.sql.catalog.{settings.spark.catalog_name}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{settings.spark.catalog_name}.client.region", settings.minio.region)
        .config("spark.sql.defaultCatalog", settings.spark.catalog_name)
        # S3A MinIO Storage
        .config("spark.hadoop.fs.s3a.endpoint", settings.minio.endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.minio.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # S3A Numeric Timeouts for Hadoop 3.4+ compatibility
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", 60)
        .config("spark.hadoop.fs.s3a.connection.timeout", 60000)
        .config("spark.hadoop.fs.s3a.multipart.purge.age", 86400)
    )

    if local_mode:
        builder = builder.master("local[*]")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

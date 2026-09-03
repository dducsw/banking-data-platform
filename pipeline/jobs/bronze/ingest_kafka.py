"""
Bronze Layer Pipeline: Real-Time Kafka Streaming -> Bronze Iceberg Tables.
Consumes CDC stream with checkpointing on MinIO S3.
Inherits configuration, table naming, and logging from BaseIcebergJob.
"""

from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DoubleType, BooleanType, TimestampType
)

from pipeline.common.base_job import BaseIcebergJob, WriteMode
from pipeline.config.pipeline_config import (
    CATALOG_NAME,
    NAMESPACE_BRONZE,
    TOPIC_TRANSACTIONS,
    BUCKET_CHECKPOINTS,
)
from pipeline.config.settings import settings


class BronzeKafkaStreamingJob(BaseIcebergJob):
    """
    Bronze Streaming Ingestion Job for Kafka CDC topics into Iceberg.
    """

    def __init__(
        self,
        topic: str = TOPIC_TRANSACTIONS,
        table_name: str = "streaming_transactions",
        checkpoint_location: Optional[str] = None,
    ):
        self.topic = topic
        self.checkpoint_location = checkpoint_location or f"s3://{BUCKET_CHECKPOINTS}/bronze_txns"
        super().__init__(
            pipeline_layer="bronze",
            table_name=table_name,
            source_table=f"kafka.{topic}",
            target_table=f"{CATALOG_NAME}.{NAMESPACE_BRONZE}.{table_name}",
            write_mode=WriteMode.APPEND,
            source_system="kafka_cdc_stream",
        )

    @staticmethod
    def get_transaction_schema() -> StructType:
        return StructType([
            StructField("transaction_id", LongType(), False),
            StructField("account_id", LongType(), False),
            StructField("customer_id", LongType(), False),
            StructField("card_id", LongType(), True),
            StructField("merchant_id", LongType(), True),
            StructField("branch_code", StringType(), True),
            StructField("txn_type", StringType(), False),
            StructField("direction", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("currency", StringType(), False),
            StructField("status", StringType(), False),
            StructField("channel", StringType(), False),
            StructField("location_city", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("is_fraud", BooleanType(), True),
            StructField("is_disputed", BooleanType(), True),
            StructField("timestamp", TimestampType(), False),
        ])

    def start_stream(self, spark: SparkSession):
        """Starts a Spark Structured Streaming query from Kafka into target Iceberg Bronze table."""
        from pyspark.sql.functions import col, from_json, current_timestamp, lit

        self.logger.info(
            f"Connecting to Kafka at {settings.kafka.bootstrap_servers}, subscribing to [{self.topic}]..."
        )
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{NAMESPACE_BRONZE}")

        raw_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", "latest")
            .load()
        )

        parsed_stream = (
            raw_stream
            .selectExpr("CAST(value AS STRING) as json_payload")
            .select(from_json(col("json_payload"), self.get_transaction_schema()).alias("data"))
            .select("data.*")
            .withColumn("_bronze_processed_at", current_timestamp())
            .withColumn("source_system", lit(self.source_system))
        )

        query = (
            parsed_stream.writeStream
            .format("iceberg")
            .outputMode("append")
            .trigger(processingTime="10 seconds")
            .option("checkpointLocation", self.checkpoint_location)
            .toTable(self.target_table)
        )

        self.logger.info(f"Streaming query started: [{query.id}], writing to [{self.target_table}].")
        return query


def start_streaming_ingest(spark: SparkSession, **kwargs):
    job = BronzeKafkaStreamingJob(**kwargs)
    return job.start_stream(spark)


if __name__ == "__main__":
    from pipeline.common.spark_session import get_spark_session
    spark_session = get_spark_session(app_name="BronzeKafkaStreamingIngestion")
    streaming_query = start_streaming_ingest(spark_session)
    streaming_query.awaitTermination()

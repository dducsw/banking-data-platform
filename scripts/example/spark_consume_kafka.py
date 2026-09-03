"""
Example Script: Spark Structured Streaming reading from Kafka.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def consume_kafka_stream(spark: SparkSession, kafka_servers: str, topic: str):
    print(f"=== Starting Kafka Stream Consumer on {kafka_servers}, topic: {topic} ===")

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    query = (
        df.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value", "timestamp")
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )

    print("Stream running. Press Ctrl+C to terminate.")
    query.awaitTermination()


if __name__ == "__main__":
    kafka_host = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka.kafka.svc.cluster.local:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "banking.public.transactions")

    spark = SparkSession.builder.appName("SparkConsumeKafkaExample").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    consume_kafka_stream(spark, kafka_host, kafka_topic)

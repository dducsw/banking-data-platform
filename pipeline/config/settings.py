"""
Centralized Configuration & Environment Settings.
Supports standard environment variable resolution across Docker/K8s/local without third-party dependencies.
"""

import os
from dataclasses import dataclass, field


@dataclass
class PostgresSettings:
    host: str = field(default_factory=lambda: os.getenv("PG_HOST", "postgres.postgres.svc.cluster.local"))
    port: int = field(default_factory=lambda: int(os.getenv("PG_PORT", "5432")))
    database: str = field(default_factory=lambda: os.getenv("PG_DB", "banking"))
    username: str = field(default_factory=lambda: os.getenv("PG_USER", "postgres"))
    password: str = field(default_factory=lambda: os.getenv("PG_PASSWORD", "postgres123"))

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"


@dataclass
class MinIOSettings:
    endpoint: str = field(default_factory=lambda: os.getenv("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000"))
    access_key: str = field(default_factory=lambda: os.getenv("MINIO_ACCESS_KEY", "admin"))
    secret_key: str = field(default_factory=lambda: os.getenv("MINIO_SECRET_KEY", "password123"))
    region: str = field(default_factory=lambda: os.getenv("AWS_REGION", "us-east-1"))
    warehouse_path: str = field(default_factory=lambda: os.getenv("WAREHOUSE_PATH", "s3://lakehouse/"))
    lakehouse_path: str = field(default_factory=lambda: os.getenv("LAKEHOUSE_PATH", "s3://lakehouse/"))


@dataclass
class GravitinoSettings:
    iceberg_rest_uri: str = field(default_factory=lambda: os.getenv("GRAVITINO_ICEBERG_REST_URI", "http://gravitino.gravitino.svc.cluster.local:9001/iceberg/"))
    web_uri: str = field(default_factory=lambda: os.getenv("GRAVITINO_WEB_URI", "http://gravitino.gravitino.svc.cluster.local:8090"))


@dataclass
class KafkaSettings:
    bootstrap_servers: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka.kafka.svc.cluster.local:9092"))


@dataclass
class SparkSettings:
    app_name: str = "BankingLakehousePipeline"
    master: str = field(default_factory=lambda: os.getenv("SPARK_MASTER", "local[*]"))
    catalog_name: str = "lakehouse"


@dataclass
class PlatformSettings:
    postgres: PostgresSettings = field(default_factory=PostgresSettings)
    minio: MinIOSettings = field(default_factory=MinIOSettings)
    gravitino: GravitinoSettings = field(default_factory=GravitinoSettings)
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    spark: SparkSettings = field(default_factory=SparkSettings)


# Global singleton
settings = PlatformSettings()

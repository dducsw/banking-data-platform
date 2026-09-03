from pipeline.config.settings import PlatformSettings, PostgresSettings, MinIOSettings, GravitinoSettings
from pipeline.config.pipeline_config import BUCKET_LAKEHOUSE, CATALOG_NAME as DEFAULT_CATALOG

def test_default_platform_settings():
    settings = PlatformSettings()
    assert settings.postgres.port == 5432
    assert "jdbc:postgresql://" in settings.postgres.jdbc_url
    assert settings.minio.warehouse_path == "s3://lakehouse/"
    assert "9001" in settings.gravitino.iceberg_rest_uri
    assert "8090" in settings.gravitino.web_uri
    assert settings.spark.catalog_name == "lakehouse"

def test_constants():
    assert BUCKET_LAKEHOUSE == "lakehouse"
    assert DEFAULT_CATALOG == "lakehouse"

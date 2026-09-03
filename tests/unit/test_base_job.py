"""
Unit tests for BaseIcebergJob lifecycle and template method execution.
"""

from unittest.mock import MagicMock, patch
import pytest

from pipeline.common.base_job import BaseIcebergJob, WriteMode


class DummyIcebergJob(BaseIcebergJob):
    """Dummy subclass for testing."""

    def __init__(self):
        super().__init__(
            pipeline_layer="silver",
            table_name="test_table",
            source_table="lakehouse.bronze.test",
            target_table="lakehouse.silver.test",
            primary_key="id",
            watermark_col="processed_at",
            write_mode=WriteMode.OVERWRITE,
        )

    def transform(self, df):
        return df


def test_base_job_initialization():
    job = DummyIcebergJob()
    assert job.pipeline_layer == "silver"
    assert job.table_name == "test_table"
    assert job.source_table == "lakehouse.bronze.test"
    assert job.target_table == "lakehouse.silver.test"
    assert job.primary_key == "id"
    assert job.write_mode == WriteMode.OVERWRITE
    assert job.watermark_col == "processed_at"


def test_base_job_validate_empty_raises():
    job = DummyIcebergJob()
    mock_df = MagicMock()
    mock_df.rdd.isEmpty.return_value = True

    with pytest.raises(ValueError, match="empty or unreadable"):
        job.validate(mock_df)


def test_base_job_run_lifecycle_success():
    job = DummyIcebergJob()
    mock_spark = MagicMock()

    mock_df = MagicMock()
    mock_df.rdd.isEmpty.return_value = False
    mock_df.columns = ["id", "val"]
    mock_df.count.return_value = 100

    job.extract = MagicMock(return_value=mock_df)
    job.validate = MagicMock()
    job.transform = MagicMock(return_value=mock_df)
    job.load = MagicMock(return_value=100)

    with patch("pipeline.common.base_job.update_watermark") as mock_watermark:
        rows = job.run(spark=mock_spark)

        assert rows == 100
        job.extract.assert_called_once_with(mock_spark)
        job.validate.assert_called_once_with(mock_df)
        job.transform.assert_called_once_with(mock_df)
        job.load.assert_called_once_with(mock_spark, mock_df)
        mock_watermark.assert_called_once()


def test_base_job_load_merge():
    job = DummyIcebergJob()
    job.write_mode = WriteMode.MERGE
    job.merge_keys = ["id"]

    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.count.return_value = 50

    rows = job.load(mock_spark, mock_df)

    assert rows == 50
    mock_df.createOrReplaceTempView.assert_called_once()
    mock_spark.sql.assert_called_once()
    sql_arg = mock_spark.sql.call_args[0][0]
    assert "MERGE INTO lakehouse.silver.test" in sql_arg
    assert "t.id = s.id" in sql_arg
    assert "WHEN MATCHED THEN" in sql_arg


def test_base_job_load_dynamic_overwrite():
    job = DummyIcebergJob()
    job.write_mode = WriteMode.DYNAMIC_OVERWRITE
    job.partition_by = ["txn_date"]

    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.count.return_value = 75

    rows = job.load(mock_spark, mock_df)

    assert rows == 75
    mock_spark.conf.set.assert_called_once_with("spark.sql.sources.partitionOverwriteMode", "dynamic")
    mock_df.write.format.assert_called_once_with("iceberg")


def test_base_job_inject_metadata():
    job = DummyIcebergJob()
    job.source_system = "core_banking"

    mock_df = MagicMock()
    mock_df.columns = ["id", "val"]
    mock_df.withColumn.return_value = mock_df

    with patch("pyspark.sql.functions.current_timestamp") as mock_ts, \
         patch("pyspark.sql.functions.lit") as mock_lit:
        mock_ts.return_value = "MOCK_TIMESTAMP"
        mock_lit.return_value = "MOCK_LIT"

        df_result = job.inject_metadata(mock_df)

        assert df_result is not None
        calls = [c[0][0] for c in mock_df.withColumn.call_args_list]
        assert "_silver_processed_at" in calls
        assert "source_system" in calls



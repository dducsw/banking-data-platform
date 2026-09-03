"""
Unit tests for Iceberg watermark state management.
"""

from unittest.mock import MagicMock

from pipeline.common.watermark import get_watermark, update_watermark, FULL_WATERMARK_TABLE


def test_get_watermark_returns_value():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.rdd.isEmpty.return_value = False
    mock_df.select.return_value.first.return_value = ["2026-09-03 12:00:00"]

    mock_spark.table.return_value.filter.return_value.sort.return_value = mock_df

    val = get_watermark(mock_spark, "fact_transactions")
    assert val == "2026-09-03 12:00:00"


def test_get_watermark_returns_none_when_empty():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.rdd.isEmpty.return_value = True

    mock_spark.table.return_value.filter.return_value.sort.return_value = mock_df

    val = get_watermark(mock_spark, "unknown_table")
    assert val is None


def test_update_watermark_executes_merge():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.createDataFrame.return_value = mock_df

    update_watermark(mock_spark, "dim_customers", "processed_at", "2026-09-03 15:30:00")

    mock_df.createOrReplaceTempView.assert_called_once_with("new_watermark")
    # Verify SQL merge was invoked on the watermark table
    called_sql = mock_spark.sql.call_args[0][0]
    assert "MERGE INTO" in called_sql
    assert FULL_WATERMARK_TABLE in called_sql

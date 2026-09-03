"""
Common utilities, lifecycle templates, and state management for pipelines.
"""

from pipeline.common.base_job import BaseIcebergJob, WriteMode
from pipeline.common.spark_session import get_spark_session
from pipeline.common.logger import get_logger
from pipeline.common.audit import start_audit, PipelineAuditRecord
from pipeline.common.watermark import get_watermark, update_watermark

__all__ = [
    "BaseIcebergJob",
    "WriteMode",
    "get_spark_session",
    "get_logger",
    "start_audit",
    "PipelineAuditRecord",
    "get_watermark",
    "update_watermark",
]

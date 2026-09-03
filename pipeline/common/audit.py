"""
Audit & Lineage Tracking for Data Pipelines.
Records pipeline run metadata, row counts, and execution metrics.
"""

from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any


@dataclass
class PipelineAuditRecord:
    pipeline_name: str
    stage: str                  # bronze / silver / gold
    target_table: str
    status: str                 # SUCCESS / FAILED / RUNNING
    rows_read: int = 0
    rows_written: int = 0
    start_time: str = ""
    end_time: Optional[str] = None
    duration_seconds: float = 0.0
    error_message: Optional[str] = None
    extra_metadata: Optional[Dict[str, Any]] = None

    def mark_completed(self, rows_written: int):
        self.status = "SUCCESS"
        self.rows_written = rows_written
        now = datetime.now(timezone.utc)
        self.end_time = now.isoformat()
        if self.start_time:
            start_dt = datetime.fromisoformat(self.start_time)
            self.duration_seconds = (now - start_dt).total_seconds()

    def mark_failed(self, error: Exception):
        self.status = "FAILED"
        self.error_message = str(error)
        now = datetime.now(timezone.utc)
        self.end_time = now.isoformat()
        if self.start_time:
            start_dt = datetime.fromisoformat(self.start_time)
            self.duration_seconds = (now - start_dt).total_seconds()

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def start_audit(pipeline_name: str, stage: str, target_table: str) -> PipelineAuditRecord:
    return PipelineAuditRecord(
        pipeline_name=pipeline_name,
        stage=stage,
        target_table=target_table,
        status="RUNNING",
        start_time=datetime.now(timezone.utc).isoformat()
    )

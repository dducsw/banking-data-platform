from pipeline.common.audit import start_audit

def test_audit_record_lifecycle():
    audit = start_audit(
        pipeline_name="test_banking_ingestion",
        stage="bronze",
        target_table="lakehouse.banking.transactions"
    )
    assert audit.status == "RUNNING"
    assert audit.target_table == "lakehouse.banking.transactions"
    assert audit.start_time != ""

    audit.mark_completed(rows_written=1500)
    assert audit.status == "SUCCESS"
    assert audit.rows_written == 1500
    assert audit.end_time is not None
    assert audit.duration_seconds >= 0.0

def test_audit_record_failure():
    audit = start_audit(
        pipeline_name="test_banking_ingestion",
        stage="silver",
        target_table="lakehouse.banking.customers"
    )
    audit.mark_failed(Exception("Connection timeout"))
    assert audit.status == "FAILED"
    assert "Connection timeout" in str(audit.error_message)

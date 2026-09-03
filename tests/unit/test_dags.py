"""
Unit Tests for Airflow DAGs Integrity and Task Structure.
Ensures DAGs are valid, importable, free of cycles, and adhere to production standards.
"""

import os
import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dag_bag():
    """Load all DAGs from pipeline/dags."""
    dag_folder = os.path.join(os.path.dirname(__file__), "..", "..", "pipeline", "dags")
    return DagBag(dag_folder=dag_folder, include_examples=False, read_dags_from_db=False)


def test_dagbag_no_import_errors(dag_bag):
    """Verify that no import or syntax errors exist across DAG files."""
    assert len(dag_bag.import_errors) == 0, f"DAG import failures: {dag_bag.import_errors}"


def test_expected_dags_present(dag_bag):
    """Verify all expected enterprise DAGs are loaded."""
    expected_dags = ["banking_medallion_pipeline"]
    for dag_id in expected_dags:
        assert dag_id in dag_bag.dags, f"Missing DAG: {dag_id}"


def test_banking_medallion_pipeline_structure(dag_bag):
    """Verify tasks and structure for banking_medallion_pipeline DAG."""
    dag = dag_bag.dags.get("banking_medallion_pipeline")
    assert dag is not None
    assert dag.default_args["retries"] == 2
    assert dag.catchup is False

    expected_tasks = [
        "bronze_postgres_ingest",
        "silver_transform_demo",
        "gold_aggregate_demo",
        "lakehouse_data_quality",
    ]
    task_ids = [t.task_id for t in dag.tasks]
    for task_id in expected_tasks:
        assert task_id in task_ids, f"Task {task_id} not found in {dag.dag_id}"

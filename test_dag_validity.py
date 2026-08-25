"""
DAG-level sanity checks. These don't test business logic (see test_transform.py
for that) — they catch the class of bug that breaks the DAG from loading at all:
import errors, cycles, missing default_args, retries misconfigured, etc.

Run with: pytest test_dag_validity.py -v

Note: requires Airflow to be installed in the test environment (same version
as production, ideally — pin it in requirements.txt / requirements-dev.txt).
"""
import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


def test_no_import_errors(dagbag):
    assert len(dagbag.import_errors) == 0, (
        f"DAG import errors found: {dagbag.import_errors}"
    )


def test_alpha_vantage_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="alphave_vantage")
    assert dag is not None, "alphave_vantage DAG failed to load"


def test_dag_has_no_cycles(dagbag):
    dag = dagbag.get_dag(dag_id="alphave_vantage")
    # DagBag.process_file already runs cycle detection on load; if a cycle
    # existed, the DAG wouldn't be in dagbag.dags at all. This test makes
    # that assumption explicit rather than relying on it silently.
    assert dag.dag_id in dagbag.dags


def test_expected_tasks_present(dagbag):
    dag = dagbag.get_dag(dag_id="alphave_vantage")
    task_ids = set(dag.task_ids)

    expected = {
        "create_table",
        "create_bucket",
        "is_api_available",
        "extract_data",
        "save_to_s3",
        "is_raw_data_save",
        "read_s3_data",
        "store_data",
    }
    missing = expected - task_ids
    assert not missing, f"Expected tasks missing from DAG: {missing}"


def test_retries_configured(dagbag):
    dag = dagbag.get_dag(dag_id="alphave_vantage")
    assert dag.default_args.get("retries", 0) > 0, (
        "retries should be > 0 — a pipeline with retries=0 doesn't "
        "actually benefit from the retry_delay configured alongside it"
    )


def test_catchup_disabled(dagbag):
    dag = dagbag.get_dag(dag_id="alphave_vantage")
    assert dag.catchup is False, (
        "catchup should be explicitly False unless a backfill is intended"
    )

"""
DAG-level sanity checks. These don't test business logic (see test_transform.py
for that) — they catch the class of bug that breaks the DAG from loading at all:
import errors, cycles, missing default_args, retries misconfigured, etc.

Run with: pytest test_dag_validity.py -v
"""

import pytest
from airflow.models import DagBag

# Centralize the DAG ID so you only have to change it in one place if it changes
DAG_ID = "alpha_vantage"


@pytest.fixture(scope="module")
def dagbag():
    # Airflow 3 requires an initialized DB backend even for some basic DagBag lookups.
    # We initialize a blank SQLite tracking DB locally for this test context.
    from airflow.utils.db import initdb

    initdb()

    return DagBag(dag_folder="dags/")


def test_no_import_errors(dagbag):
    """Ensure there are no syntax or import errors across the entire DAG folder."""
    assert (
        len(dagbag.import_errors) == 0
    ), f"DAG import errors found: {dagbag.import_errors}"


def test_alpha_vantage_dag_loaded(dagbag):
    """Verify that our specific DAG was found and parsed successfully."""
    # Airflow 3 fix: Pull from the in-memory collection instead of get_dag() database query
    dag = dagbag.dags.get(DAG_ID)
    assert dag is not None, f"DAG '{DAG_ID}' failed to load or does not exist."


def test_dag_has_no_cycles(dagbag):
    """Explicitly test the DAG topology for directed acyclic graph cycles."""
    dag = dagbag.dags.get(DAG_ID)
    assert dag is not None, f"DAG '{DAG_ID}' not found to check for cycles."

    # Airflow's built-in cycle detector will raise a CycleDetected Exception if it fails
    from airflow.utils.dag_cycle_tester import check_cycle

    try:
        check_cycle(dag)
    except Exception as e:
        pytest.fail(f"Cycle detected in DAG {DAG_ID}: {e}")


def test_expected_tasks_present(dagbag):
    """Ensure no tasks were accidentally deleted or renamed."""
    dag = dagbag.dags.get(DAG_ID)
    assert dag is not None

    task_ids = set(dag.task_ids)
    expected = {
        "create_table",
        "create_bucket",
        "is_api_available",
        "extract_data",
        "save_to_s3",
        "is_raw_data_save",
        "read_s3_data",
        "store_user",
    }
    missing = expected - task_ids
    assert not missing, f"Expected tasks missing from DAG: {missing}"


def test_retries_configured(dagbag):
    """Ensure a retry policy is enforced for production reliability."""
    dag = dagbag.dags.get(DAG_ID)
    assert dag is not None

    # Check default_args dictionary, fallback to checking the attribute directly on a task
    retries = dag.default_args.get("retries")
    if retries is None and dag.tasks:
        retries = dag.tasks[0].retries  # check if individual tasks inherited it

    assert (
        retries is not None and retries > 0
    ), f"Retries should be > 0. Found: {retries}"


def test_catchup_disabled(dagbag):
    """Prevent accidental historical backfills when deploying the DAG."""
    dag = dagbag.dags.get(DAG_ID)
    assert dag is not None
    assert (
        dag.catchup is False
    ), "catchup should be explicitly False unless a backfill is intended"

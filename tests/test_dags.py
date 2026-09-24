"""DAG integrity tests. Run inside the Airflow image (see `make test` and CI)."""
from pathlib import Path

import pytest
from airflow.models import DagBag

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="session")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_are_loaded(dagbag):
    assert {"spark_example", "spark_dbt_example"} <= set(dagbag.dag_ids)


def test_dbt_models_are_rendered_as_tasks(dagbag):
    dag = dagbag.get_dag("spark_dbt_example")
    task_ids = set(dag.task_ids)
    for model in ("stg_employees", "city_salary_summary", "salary_bands"):
        assert f"dbt_transform.{model}.run" in task_ids, sorted(task_ids)
        assert f"dbt_transform.{model}.test" in task_ids, sorted(task_ids)


def test_dbt_runs_after_spark_ingest(dagbag):
    dag = dagbag.get_dag("spark_dbt_example")
    dbt_tasks = [t for t in dag.tasks if t.task_id.startswith("dbt_transform.")]
    assert dbt_tasks
    for task in dbt_tasks:
        assert "spark_ingest" in task.get_flat_relative_ids(upstream=True), task.task_id
        assert "log_end" in task.get_flat_relative_ids(upstream=False), task.task_id

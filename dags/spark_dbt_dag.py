import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode, TestBehavior

DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt"))
DBT_EXECUTABLE_PATH = os.getenv("DBT_EXECUTABLE_PATH", "/opt/airflow/dbt_venv/bin/dbt")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="spark_dbt_example",
    default_args=default_args,
    description="Spark ingests raw Parquet, dbt transforms it on Spark via the Thrift Server",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "dbt", "example"],
) as dag:

    def log_start():
        print("Pipeline started: Spark ingest -> dbt models and tests on Spark Thrift Server.")

    def log_end(**context):
        print(f"Pipeline complete. Query the marts at jdbc:hive2://localhost:10000 (run: {context['run_id']})")

    task_start = PythonOperator(
        task_id="log_start",
        python_callable=log_start,
    )

    spark_ingest = SparkSubmitOperator(
        task_id="spark_ingest",
        application="/opt/airflow/dags/spark_ingest_job.py",
        conn_id="spark_default",
        name="airflow_spark_ingest",
        conf={"spark.cores.max": "1", "spark.executor.memory": "512m"},
        verbose=False,
    )

    # One Airflow task per dbt seed/model, each followed by its tests
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=ProfileConfig(
            profile_name="spark_demo",
            target_name="dev",
            profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
        ),
        # dbt lives in its own virtualenv, so Cosmos must call it as a subprocess
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
        render_config=RenderConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
            invocation_mode=InvocationMode.SUBPROCESS,
            test_behavior=TestBehavior.AFTER_EACH,
            # Tests that touch several models (relationships, singular tests) get their
            # own task that waits for all of those models
            should_detach_multiple_parents_tests=True,
        ),
        operator_args={"pool": "spark_thrift"},
    )

    task_end = PythonOperator(
        task_id="log_end",
        python_callable=log_end,
    )

    task_start >> spark_ingest >> dbt_transform >> task_end

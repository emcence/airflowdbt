from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spark_example",
    default_args=default_args,
    description="Simple Airflow + Spark pipeline",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "example"],
) as dag:

    def log_start():
        print("Pipeline started: reading CSV, aggregating by city, writing results.")

    def log_end(**context):
        ti = context["ti"]
        print(f"Pipeline complete. Task instance: {ti.task_id}, run: {context['run_id']}")

    task_start = PythonOperator(
        task_id="log_start",
        python_callable=log_start,
    )

    task_spark_job = SparkSubmitOperator(
        task_id="spark_aggregate_job",
        application="/opt/airflow/dags/spark_job.py",
        conn_id="spark_default",
        name="airflow_spark_example",
        verbose=True,
    )

    task_end = PythonOperator(
        task_id="log_end",
        python_callable=log_end,
        provide_context=True,
    )

    task_start >> task_spark_job >> task_end

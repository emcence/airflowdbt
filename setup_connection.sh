#!/bin/bash
# Run after docker-compose up to register the Spark connection in Airflow

podman compose exec airflow-scheduler airflow connections add spark_default \
  --conn-type spark \
  --conn-host spark://spark-master \
  --conn-port 7077

echo "Spark connection 'spark_default' created."

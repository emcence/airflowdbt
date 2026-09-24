FROM apache/airflow:2.11.0-python3.12

USER root
# git: required by `dbt debug` and `dbt deps`
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk-headless git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Symlink path — portable across amd64 and arm64 (Apple Silicon)
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
COPY --chown=airflow:root requirements/ /opt/airflow/requirements/

# Airflow providers, PySpark and Astronomer Cosmos go into Airflow's own environment
RUN pip install --no-cache-dir -r /opt/airflow/requirements/airflow.txt

# dbt gets its own virtualenv; Cosmos calls it via ExecutionConfig(dbt_executable_path=...)
RUN python -m venv /opt/airflow/dbt_venv \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir -r /opt/airflow/requirements/dbt.txt

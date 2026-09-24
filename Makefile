# Works with Docker or Podman: `make up COMPOSE="podman compose"`
COMPOSE ?= docker compose
DBT     := /opt/airflow/dbt_venv/bin/dbt
DBT_ARGS := --profiles-dir . --target-path /tmp/target --log-path /tmp/logs

.PHONY: up down reset build dbt-debug dbt-build dbt-docs trigger test query

build:
	$(COMPOSE) build

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

# Wipe lake, warehouse and metastore together (they must stay in sync)
reset: down
	rm -rf data/lake data/warehouse data/metastore data/dbt-docs

dbt-debug:
	$(COMPOSE) exec airflow-scheduler bash -c "cd /opt/airflow/dbt && $(DBT) debug --profiles-dir . --log-path /tmp/logs"

# Runs the whole dbt project in one go, outside Airflow (needs spark_ingest to have run once)
dbt-build:
	$(COMPOSE) exec airflow-scheduler bash -c "cd /opt/airflow/dbt && $(DBT) build $(DBT_ARGS)"

dbt-docs:
	$(COMPOSE) exec airflow-scheduler bash -c "cd /opt/airflow/dbt && $(DBT) docs generate --profiles-dir . --target-path /opt/airflow/data/dbt-docs --log-path /tmp/logs"
	@echo "Open data/dbt-docs/index.html (serve it: python3 -m http.server -d data/dbt-docs 8088)"

trigger:
	$(COMPOSE) exec airflow-scheduler airflow dags test spark_dbt_example

test:
	$(COMPOSE) exec airflow-scheduler bash -c "pip install --quiet pytest && DBT_PROJECT_DIR=/opt/airflow/dbt python -m pytest /opt/airflow/tests -v -p no:cacheprovider"

query:
	$(COMPOSE) exec -T spark-thrift /opt/bitnami/spark/bin/beeline -u jdbc:hive2://localhost:10000 --silent=true \
		-e "select * from analytics.city_salary_summary order by city"

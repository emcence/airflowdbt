# Airflow + Spark + dbt Example

A small local data platform that shows how the three tools fit together:

- **Spark** ingests raw data into a Parquet "lake".
- **dbt** transforms it with SQL that runs **on the same Spark cluster**. dbt connects through the Spark Thrift Server with `dbt-spark[PyHive]`.
- **Airflow** orchestrates everything. **Astronomer Cosmos** turns each dbt model, seed and test into its own Airflow task.

The repo has two pipelines:

| DAG | What it shows |
| --- | --- |
| `spark_example` | The original minimal pipeline: one PySpark job reads a CSV, aggregates it and writes CSV. |
| `spark_dbt_example` | **The dbt sample.** Spark ingests the data, dbt builds staging and mart tables on Spark, and tests run after each model. |

---

## Architecture

```
                       ┌──────────────────────── Airflow (LocalExecutor) ─────────────────────────┐
                       │                                                                          │
 data/sample_data.csv  │  log_start ─► spark_ingest ─► dbt_transform (Cosmos task group) ─► log_end │
         │             │                   │                     │                                │
         │             └───────────────────┼─────────────────────┼────────────────────────────────┘
         │                   spark-submit  │                     │  dbt-spark, method: thrift (PyHive)
         ▼                                 ▼                     ▼
 ┌──────────────────────────────────────────────┐    ┌───────────────────────────────┐
 │ Spark master + worker (spark://spark-master) │◄───│ spark-thrift (HiveServer2 API) │
 │  executors read/write /opt/airflow/data      │    │  :10000, Derby metastore       │
 └──────────────────────────────────────────────┘    └───────────────────────────────┘
                         │
                         ▼
   data/lake/raw/employees/   (Parquet, written by Spark)
   data/warehouse/            (analytics.* tables, written by dbt through Spark)
   data/metastore/            (table catalog used by the Thrift Server)
```

- The `./data` folder is bind-mounted at `/opt/airflow/data` in **every** container: Airflow, the Spark master, the worker and the Thrift Server. Any path therefore means the same thing everywhere.
- dbt never starts Spark itself. It sends SQL to the long-running Thrift Server, and the Thrift Server runs that SQL as Spark jobs on the cluster. Managed platforms like Databricks SQL, EMR and Kyuubi work the same way.

---

## Stack

| Service | Image | URL / port | Purpose |
| --- | --- | --- | --- |
| `airflow-webserver` | built from `Dockerfile` | http://localhost:8080 (admin / admin) | Airflow UI |
| `airflow-scheduler` | built from `Dockerfile` | – | Runs the tasks (LocalExecutor) |
| `airflow-init` | built from `Dockerfile` | – | One-off job: migrates the DB, creates the `spark_thrift` pool and the admin user |
| `postgres` | `postgres:15` | localhost:5432 | Airflow metadata DB |
| `spark-master` | `bitnamilegacy/spark:3.5.3` | http://localhost:8081, `spark://localhost:7077` | Spark standalone master |
| `spark-worker` | `bitnamilegacy/spark:3.5.3` | – | 2 cores / 2 GB |
| `spark-thrift` | `bitnamilegacy/spark:3.5.3` | `jdbc:hive2://localhost:10000`, UI http://localhost:4040 | Spark Thrift Server, the SQL endpoint dbt connects to |

### What's in the Airflow image

| Environment | Packages | Pinned in |
| --- | --- | --- |
| Airflow's Python | `apache-airflow==2.11.0`, `apache-airflow-providers-apache-spark==5.0.0`, `pyspark==3.5.3`, `astronomer-cosmos==1.15.1` | [requirements/airflow.txt](requirements/airflow.txt) |
| `/opt/airflow/dbt_venv` (separate virtualenv) | `dbt-core==1.11.15`, `dbt-spark[PyHive]==1.11.0` | [requirements/dbt.txt](requirements/dbt.txt) |
| System | OpenJDK (`default-jdk-headless`), needed by `spark-submit`; `git`, needed by `dbt debug` and `dbt deps` | [Dockerfile](Dockerfile) |

dbt lives in its own virtualenv because its dependencies (protobuf, jinja, …) regularly conflict with Airflow's. Cosmos calls it as a subprocess, which is why the DAG sets `InvocationMode.SUBPROCESS`.

---

## Quick start

Prerequisites: Docker with Compose v2, **or** Podman with `podman-compose` installed (`brew install podman-compose` or `pip install podman-compose`), because `podman compose` only forwards to it. Give the VM at least 4 CPUs and 6 GB RAM.

Tested on macOS (Apple Silicon) with Podman 5.5.2 and podman-compose 1.6.0 (4 CPUs, 6 GB VM). The first image build takes a few minutes, and a full `spark_dbt_example` run takes 30–45 seconds.

```bash
# 1. Build the Airflow image (JDK + providers + Cosmos + dbt virtualenv)
docker compose build              # or: podman compose build

# 2. Start everything (airflow-init migrates the DB and creates the pool and the admin user)
docker compose up -d

# 3. Check that dbt can reach the Thrift Server
make dbt-debug                    # should end with "All checks passed!"

# 4. Open http://localhost:8080, unpause and trigger "spark_dbt_example"
#    ...or run it from the command line:
make trigger

# 5. Look at the results
make query
```

On Linux, run `echo "AIRFLOW_UID=$(id -u)" > .env` before step 2. Airflow then writes to `./logs` and `./data` as your user.

Every `make` target accepts `COMPOSE="podman compose"`, for example `make dbt-debug COMPOSE="podman compose"`.

The Spark connection `spark_default` comes from the `AIRFLOW_CONN_SPARK_DEFAULT` environment variable in [docker-compose.yml](docker-compose.yml). [setup_connection.sh](setup_connection.sh) is only needed if you removed that variable.

---

## The `spark_dbt_example` pipeline

```
log_start → spark_ingest → dbt_transform ─────────────────────────────────────────────────► log_end
                            ├─ city_region.seed → city_region.test ─┐
                            ├─ stg_employees.run → stg_employees.test ┼─► city_salary_summary.run → .test ─┐
                            │                                        ├─► salary_bands.run → .test          │
                            │                                        └─► relationships_…_test              │
                            └──────────────────────────────────────────── assert_summary_covers_all_employees_test
```

1. **`spark_ingest`** runs `SparkSubmitOperator` → [dags/spark_ingest_job.py](dags/spark_ingest_job.py).
   - Reads `data/sample_data.csv` with an explicit schema and adds an `ingested_at` timestamp.
   - Overwrites Parquet in `data/lake/raw/employees/`.
   - Does no business logic; that all lives in dbt.
2. **`dbt_transform`** is a Cosmos `DbtTaskGroup` over [dbt/](dbt/).
   - One Airflow task per seed and model (`dbt seed` / `dbt run --select <node>`), each followed by a `dbt test --select <node>` task (`TestBehavior.AFTER_EACH`).
   - Tests that span several models have their own task, which waits for all of those models (`should_detach_multiple_parents_tests=True`). These are the `relationships` test and the singular test.
   - Every dbt task uses the Airflow pool **`spark_thrift`** (2 slots), so dbt can't flood the single Thrift Server.
3. **`log_end`** logs completion.

A failing model or test fails only its own task. You can clear and retry that task in the Airflow UI without re-running the whole project.

### Expected output

`analytics.city_salary_summary` (same numbers as the old `spark_example` job, now enriched with state and region):

| city | state | region | employee_count | avg_salary | max_salary | min_age |
| --- | --- | --- | --- | --- | --- | --- |
| Chicago | IL | Midwest | 2 | 90000.00 | 95000.00 | 35 |
| Los Angeles | CA | West | 3 | 76666.67 | 90000.00 | 25 |
| New York | NY | Northeast | 3 | 70000.00 | 75000.00 | 27 |

`analytics.salary_bands` puts each of the 8 employees in a `low` (< 70k), `mid` (< 85k) or `high` band.

---

## The dbt project ([dbt/](dbt/))

```
dbt/
├── dbt_project.yml                  # project config, vars and on-run-start hooks
├── profiles.yml                     # spark / thrift connection, read from env vars
├── macros/
│   └── create_raw_employees_table.sql
├── models/
│   ├── sources.yml                  # source: raw.employees
│   ├── staging/
│   │   ├── stg_employees.sql        # view: typed, trimmed, renamed
│   │   └── stg_employees.yml        # unique / not_null / range / relationships tests
│   └── marts/
│       ├── city_salary_summary.sql  # table: per-city stats + region (joins the seed)
│       ├── salary_bands.sql         # table: low / mid / high band per employee
│       └── marts.yml
├── seeds/
│   ├── city_region.csv              # city → state, region lookup
│   └── seeds.yml
└── tests/
    ├── assert_summary_covers_all_employees.sql   # singular test
    └── generic/value_between.sql                 # custom generic test (no packages needed)
```

| Layer | Materialization | Schema | Storage |
| --- | --- | --- | --- |
| Source `raw.employees` | external table (created by a hook) | `raw` | `data/lake/raw/employees/` (Parquet from Spark) |
| Seeds | table | `analytics` | `data/warehouse/analytics.db/…` (Parquet) |
| `staging/` | view | `analytics` | metastore only |
| `marts/` | table | `analytics` | `data/warehouse/analytics.db/…` (Parquet) |

### How dbt finds the raw data

Spark writes plain Parquet files, but dbt reads tables from the metastore. Three `on-run-start` hooks in [dbt_project.yml](dbt/dbt_project.yml) bridge that gap, and they run before every dbt command:

1. `create database if not exists raw`
2. `create table if not exists raw.employees (...) using parquet location '/opt/airflow/data/lake/raw/employees'`. The schema is explicit, so this works even before the first ingest.
3. `refresh table raw.employees`. The Thrift Server is long-lived and caches file listings. Without the refresh, it would still look for the Parquet files that the latest ingest overwrote.

The raw path and schema are dbt vars (`raw_employees_path`, `raw_schema`), so you can point dbt somewhere else with `--vars`.

### Connection: `dbt-spark[PyHive]` + `method: thrift`

[dbt/profiles.yml](dbt/profiles.yml) is checked in because it contains no secrets. Every value comes from an environment variable:

| Env var | Default | Set in compose to |
| --- | --- | --- |
| `SPARK_THRIFT_HOST` | `localhost` | `spark-thrift` |
| `SPARK_THRIFT_PORT` | `10000` | `10000` |
| `DBT_SCHEMA` | `analytics` | – |
| `DBT_TARGET` | `dev` | – |

`PyHive` is the client library, and `method: thrift` is the dbt setting that uses it. Together they talk to a HiveServer2-compatible endpoint, which here is the Spark Thrift Server.

Other environment variables the DAG reads: `DBT_PROJECT_DIR` (default `/opt/airflow/dbt`) and `DBT_EXECUTABLE_PATH` (default `/opt/airflow/dbt_venv/bin/dbt`).

---

## Working with the stack

| Command | What it does |
| --- | --- |
| `make build` / `make up` / `make down` | Build the image, start the stack, stop it |
| `make dbt-debug` | `dbt debug`: checks the Thrift connection |
| `make dbt-build` | `dbt build` for the whole project in one go, outside Airflow. `spark_ingest` must have run at least once. |
| `make dbt-docs` | Generates dbt docs into `data/dbt-docs/` |
| `make trigger` | `airflow dags test spark_dbt_example`: runs the DAG once in the foreground |
| `make test` | DAG integrity tests ([tests/test_dags.py](tests/test_dags.py)) inside the scheduler container |
| `make query` | Queries `analytics.city_salary_summary` with beeline |
| `make reset` | Stops the stack and deletes the lake, warehouse, metastore and docs |

Running dbt by hand:

```bash
docker compose exec airflow-scheduler bash
cd /opt/airflow/dbt
/opt/airflow/dbt_venv/bin/dbt build --profiles-dir . --target-path /tmp/target
/opt/airflow/dbt_venv/bin/dbt run --profiles-dir . --target-path /tmp/target --select salary_bands
```

Querying with any SQL client. Beeline ships in the Spark image:

```bash
docker compose exec spark-thrift /opt/bitnami/spark/bin/beeline -u jdbc:hive2://localhost:10000 \
  -e "show tables in analytics"
```

DBeaver, DataGrip and other JDBC tools can use the "Apache Spark" or "Hive" driver with `jdbc:hive2://localhost:10000` and no authentication.

Viewing the dbt docs after `make dbt-docs`:

```bash
python3 -m http.server -d data/dbt-docs 8088   # then open http://localhost:8088
```

---

## Continuous integration

[.github/workflows/ci.yml](.github/workflows/ci.yml) runs on every push to `main`/`master`, on every pull request, and on manual dispatch (**Actions → CI → Run workflow**).

| Job | Steps | Estimated time |
| --- | --- | --- |
| **`validate`** | Builds the Airflow image (layers cached in GitHub Actions). Runs `dbt parse`. Runs the DAG integrity tests with pytest: no import errors, every dbt model is rendered as `run` + `test` tasks, and every dbt task runs after `spark_ingest`. | ~3–5 min (cached) |
| **`e2e`** | Starts Postgres, the Spark master and worker and the Thrift Server. Runs `airflow-init` and `dbt debug`. Runs `airflow dags test spark_dbt_example` and checks that the run succeeded. Queries both marts. | ~8–12 min |

Both jobs run inside the same Docker image as the local stack, so CI tests exactly what you run. The times are estimates until the workflow has run on GitHub.

What the run leaves behind:

- **Job summary:** the `e2e` job writes `city_salary_summary` and `salary_bands` into the run's summary page, so anyone can see the pipeline's output without running it.
- **`dbt-docs` artifact:** download it, unzip it and serve it with `python3 -m http.server`. It holds the dbt documentation site with the lineage graph.
- **`logs` artifact:** Airflow task logs plus `docker compose logs` from every container, uploaded even when the job fails.

On the runner, Airflow runs as the runner's own uid (`AIRFLOW_UID=$(id -u)`, which is 1001, the same uid Bitnami Spark uses). That lets every container write to the shared bind mounts.

---

## Resource sizing

The worker has 2 cores and 2 GB of RAM. Both Spark applications are capped, so they fit side by side:

| Spark application | Cores | Executor memory | Lifetime |
| --- | --- | --- | --- |
| `spark-thrift-server` | 1 (`spark.cores.max=1`) | 512m | Always running (holds its executor) |
| `airflow_spark_ingest` | 1 (`spark.cores.max=1`) | 512m | Only during the `spark_ingest` task |
| `airflow_spark_example` (old DAG) | whatever is free | 1g | Only during the task |

If you add more Spark jobs, raise `SPARK_WORKER_CORES` and `SPARK_WORKER_MEMORY`. Otherwise new jobs wait in `WAITING` on the Spark UI because the Thrift Server never releases its core.

---

## Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| dbt tasks stay in **scheduled** forever | The `spark_thrift` pool is missing. Re-run `docker compose up airflow-init`, or create it: `airflow pools set spark_thrift 2 "dbt"`. |
| `dbt debug`: `Could not connect … 10000` | The Thrift Server is still starting (about 30 s) or crashed. Check `docker compose ps` and `docker compose logs spark-thrift`. |
| `spark_ingest` hangs, Spark UI shows the app as `WAITING` | The worker has no free cores. See [Resource sizing](#resource-sizing). |
| `Can not create the managed table … location already exists` | The warehouse files and the metastore got out of sync, for example after deleting only one of them. Run `make reset`. |
| `FileNotFoundException … part-0000…parquet` | Stale file listing on the Thrift Server. The `refresh table` hook normally prevents this; `refresh table raw.employees` in beeline fixes it by hand. |
| Permission denied writing `./logs` or `./data` (Linux) | Run `echo "AIRFLOW_UID=$(id -u)" > .env` and restart. |
| Stale container ID errors with Podman | `airflow-webserver` and `airflow-scheduler` deliberately don't depend on `airflow-init`, only on a healthy Postgres. Delete this project's containers and run `up` again. `airflow-init` must have completed at least once on a fresh setup. |
| `podman compose up` hangs on `podman wait --condition=running …`, or `bind: address already in use` for 5432 / 7077 / 8080 / 8081 | A port is still held, often by an orphaned `gvproxy` (podman's port forwarder) left over from an earlier VM session. `lsof -nP -iTCP:7077 -sTCP:LISTEN` shows its PID. If it isn't the `gvproxy` of the running machine (`pgrep -fl gvproxy`), kill it, then run `up` again. |
| `podman compose exec … beeline` hangs when run from a script | Without a terminal, `exec` needs `-T` (no TTY). `make query` already passes it. |
| DAG import error `RenderConfig.dbt_executable_path … DBT_RUNNER` | Cosmos has to use `InvocationMode.SUBPROCESS` because dbt lives in its own virtualenv. Keep that setting in [dags/spark_dbt_dag.py](dags/spark_dbt_dag.py). |

---

## Project layout

```
.
├── .github/workflows/ci.yml     # CI: validate + end-to-end run
├── dags/
│   ├── spark_example_dag.py     # original DAG (CSV → Spark aggregate → CSV)
│   ├── spark_job.py             # PySpark job used by spark_example
│   ├── spark_dbt_dag.py         # dbt sample DAG (Spark ingest → Cosmos dbt task group)
│   └── spark_ingest_job.py      # PySpark job: CSV → Parquet raw layer
├── dbt/                         # dbt project (see above)
├── data/
│   ├── sample_data.csv          # input data
│   ├── output/                  # spark_example output
│   ├── lake/ warehouse/ metastore/   # created by spark_dbt_example (git-ignored)
├── requirements/
│   ├── airflow.txt              # Airflow env pins
│   └── dbt.txt                  # dbt virtualenv pins
├── tests/test_dags.py           # DAG integrity tests
├── Dockerfile
├── docker-compose.yml
├── Makefile
└── setup_connection.sh
```

---

## Design notes and next steps

This sample follows the "SQL endpoint + orchestrator" pattern. dbt talks to a long-running Thrift Server, and Airflow only orchestrates. Each dbt node is its own retryable task. For a production setup, the next steps would be:

1. **Postgres-backed Hive metastore** instead of embedded Derby. Derby is single-user and lives on a local disk.
2. **Profile from an Airflow Connection.** Cosmos's `SparkThriftProfileMapping` can replace the checked-in `profiles.yml`.
3. **Pre-built dbt manifest.** Run `dbt parse` at image build time and use `LoadMode.DBT_MANIFEST`, so Cosmos doesn't run `dbt ls` on every DAG parse.
4. **Table format.** Switch `+file_format: parquet` to Delta or Iceberg for atomic `create or replace`, incremental `merge` models and time travel.

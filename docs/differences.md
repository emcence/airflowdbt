**The Thrift + Cosmos version follows best practices more closely.** The session version is fine for a quick demo, but it's the kind of setup teams usually replace before production.

## Why Thrift + Cosmos is the better practice

| Practice                                                                              | Thrift + Cosmos                                                     | Session + single task                                                                  |
| ------------------------------------------------------------------------------------- | ------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| dbt talks to a **long-running SQL endpoint** instead of starting its own Spark engine | ✅ This is the same pattern as Databricks SQL, EMR, Kyuubi or Trino | ❌ Every run starts its own Spark driver inside Airflow                                |
| dbt's own recommendation                                                              | `thrift` is a standard production method                            | dbt's docs describe `session` as meant for advanced users and experimental development |
| Airflow runs orchestration, not the heavy compute                                     | ✅ Airflow only sends SQL                                           | ❌ The Spark driver runs inside the Airflow scheduler container and uses its memory    |
| **Visibility and retries per model**                                                  | ✅ Each model and test is its own task, and you can retry just one  | ❌ One task: a failure means reading logs and rerunning everything                     |
| **One shared metastore** that every client sees                                       | ✅ One place for all table definitions                              | ❌ Local Derby file, so only one dbt run can happen at a time                          |
| Safe to run in parallel                                                               | ✅                                                                  | ❌ Needs `max_active_runs=1`                                                           |

## Good practices both versions already follow

- dbt is installed in its own virtualenv, so its dependencies don't clash with Airflow's.
- Spark handles ingestion and dbt handles the SQL transformations, with Parquet files in between.
- Tests are part of the project, and `dbt build` runs them right after each model.
- Profile values come from `env_var()`, not hard-coded settings.

## What would make the Thrift version fully "best practice"

I'd add these to that plan:

1. **Swap Derby for a Postgres-backed Hive metastore.** Derby is a single-user store meant for testing; a real metastore can sit on the existing `postgres` service.
2. **Pull the profile from an Airflow Connection.** Cosmos's Spark Thrift profile mapping can build the dbt profile from a connection like `spark_thrift_default`, so connection details live in Airflow like the existing `spark_default`.
3. **Generate the dbt manifest when the image is built.** Running `dbt deps && dbt parse` in the Dockerfile lets Cosmos use `LoadMode.DBT_MANIFEST`. Otherwise every DAG parse runs `dbt ls`, which is slow.
4. **Add CI.** Run `dbt parse` and `dbt build --target ci` on each pull request, plus a check that every DAG imports cleanly.
5. **Size resources explicitly.** Cap what the Thrift server takes (`spark.cores.max`, `spark.executor.memory`) and give the dbt tasks an Airflow pool, so dbt can't starve other Spark jobs.

**My recommendation:** go with Thrift + Cosmos, plus items 1–3. Items 4–5 can come later. Use the session version only if you want the smallest possible local demo.

Should I update the Thrift plan with these additions, or go ahead and implement it?

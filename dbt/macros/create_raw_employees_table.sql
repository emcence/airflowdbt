{#
  Registers the Parquet files landed by Spark as an external table.
  The schema is explicit so the table can be created before any files exist
  and matches the schema written by dags/spark_ingest_job.py.
#}
{% macro create_raw_employees_table() %}
create table if not exists {{ var('raw_schema') }}.employees (
    id          int,
    name        string,
    age         int,
    city        string,
    salary      int,
    ingested_at timestamp
)
using parquet
location '{{ var("raw_employees_path") }}'
{% endmacro %}

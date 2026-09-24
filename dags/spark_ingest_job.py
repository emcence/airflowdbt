"""Land data/sample_data.csv as Parquet in the raw layer of the lake.

Submitted to the Spark cluster by the spark_dbt_example pipeline. All
transformations happen afterwards in dbt; this job only types the columns
and stamps the load time.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SOURCE_PATH = "/opt/airflow/data/sample_data.csv"
TARGET_PATH = "/opt/airflow/data/lake/raw/employees"

# Must match the table definition in dbt/macros/create_raw_employees_table.sql
SCHEMA = "id INT, name STRING, age INT, city STRING, salary INT"

spark = SparkSession.builder.appName("SparkIngestEmployees").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = (
    spark.read.csv(SOURCE_PATH, header=True, schema=SCHEMA)
    .withColumn("ingested_at", F.current_timestamp())
)

print(f"Ingesting {df.count()} rows from {SOURCE_PATH}")
df.show()

df.write.mode("overwrite").parquet(TARGET_PATH)
print(f"Raw Parquet written to {TARGET_PATH}")

spark.stop()

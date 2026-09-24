from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("AirflowSparkExample").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv("../data/sample_data.csv", header=True, inferSchema=True)

print("=== Raw data ===")
df.show()
print(f"Total rows: {df.count()}")

result = (
    df.groupBy("city")
    .agg(
        F.count("id").alias("employee_count"),
        F.avg("salary").alias("avg_salary"),
        F.max("salary").alias("max_salary"),
        F.min("age").alias("min_age"),
    )
    .orderBy("city")
)

print("=== Aggregation by city ===")
result.show()

output_path = "../data/output"
result.write.mode("overwrite").csv(output_path, header=True)
print(f"Results written to {output_path}")

spark.stop()

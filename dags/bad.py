from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ErrorHandling").getOrCreate()

# with _corrupt_record to capture bad rows.. Define schema 
schema = "id INT, name STRING, amount DOUBLE, _corrupt_record STRING"

# Read CSV in PERMISSIVE mode
df = spark.read.option("mode", "PERMISSIVE").schema(schema).csv("../data/bad.csv")

# divide good and bad records
bad_df = df.filter(col("_corrupt_record").isNotNull())
good_df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

# trace corrupt records.. if dataset is big bad_df.write.mode("overwrite").csv("logs/corrupt_records")
bad_records = bad_df.select("_corrupt_record").collect()
if bad_records:
    print(f"Found {len(bad_records)} corrupt records: {[row._corrupt_record for row in bad_records]}")
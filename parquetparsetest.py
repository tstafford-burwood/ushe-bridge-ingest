import pyspark
from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Parquet to PySpark DataFrame") \
    .getOrCreate()

# Path to your Parquet file
parquet_file_path = "synthetic_data 1.parquet"

# Load the Parquet file into a PySpark DataFrame
df = spark.read.parquet(parquet_file_path)

# Show the DataFrame content
df.show()

# Optionally, you can print the schema to understand the structure
df.printSchema()

# Stop the Spark session when done
spark.stop()
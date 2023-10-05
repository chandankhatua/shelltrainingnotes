
from pyspark.sql import SparkSession
# create a Spark session
spark = SparkSession.builder.appName('App1').getOrCreate()

# load a csv file into a DataFrame
df = spark.read.csv("input_data.csv", header=True, inferSchema=True)

# Perform transformation (filter, aggregation, ) on the df
df = df.filter(df["Day"].isin("Sat","Sun"))

# Save the result in a csv file
df.write.parquet("output_data.parquet")

# stop the spark session
spark.stop()

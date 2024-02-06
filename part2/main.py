from pyspark.sql import SparkSession
import sys

# The entry point into all functionality in Spark is the SparkSession class.
spark = (SparkSession
	.builder
        .appName("part2")
	.getOrCreate())

# You can read the data from a file into DataFrames
df = spark.read.option("header", True).csv(sys.argv[1])

sorted_df = df.sort(["cca2", "timestamp"])

sorted_df.show()

sorted_df.write.option("overwrite", True).csv(sys.argv[2])

spark.stop()

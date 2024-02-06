from pyspark.sql import SparkSession
from operator import add
import sys


# Create a SparkSession
spark = SparkSession\
    .builder\
    .appName("PythonPageRank")\
    .getOrCreate()

# Read data into a DataFrame, convert it to an RDD, and filter out lines starting with '#'
lines = spark.read.text(sys.argv[1]).rdd.map(lambda line:line[0]).filter(lambda line: not line.startswith("#"))

# Map each line to a tuple of two integers, remove duplicates, group by key, and cache the result
edges = lines.map(lambda x: x.split("\t")).map(lambda x: (int(x[0]), int(x[1]))).distinct().groupByKey().cache()

# Initialize PageRank values for each URL
pageranks = edges.map(lambda x: (x[0], 1.0))

# Set the damping factor (beta)
beta = 0.85

# Perform a single iteration of PageRank algorithm
for iteration in range(1):
    # Calculate contributions from each URL to the rank of other URLs
    contributions = edges.join(pageranks).flatMap(lambda url_urls_rank: ((url, url_urls_rank[1][1] / len(url_urls_rank[1][0])) for url in url_urls_rank[1][0]))

    # Re-calculate URL ranks based on neighbor contributions
    pageranks = contributions.reduceByKey(add).mapValues(lambda rank: rank * beta + 1 - beta)

# Save the output file
outputDF = pageranks.toDF()
outputDF.write.option("overwrite", True).text(sys.argv[2])

# Stop the SparkSession
spark.stop()

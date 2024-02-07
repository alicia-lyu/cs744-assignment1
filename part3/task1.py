from pyspark.sql import SparkSession
from operator import add
import sys

def calculate_contributions(edges_with_rank):
    node, (neighbors, rank) = edges_with_rank
    contributions = [(neighbor, rank / len(neighbors)) for neighbor in neighbors]
    return contributions

# Create a SparkSession
if (sys.argv[1].endswith(".txt")):
    data_name = "web"
else:
    data_name = "wiki"

spark = SparkSession.builder.appName("PageRank-Task1-%s" % data_name).getOrCreate()
data_frame = spark.read.text(sys.argv[1])

# Convert to an RDD, and filter out lines starting with '#'
lines = data_frame.rdd.map(lambda line:line[0]).filter(lambda line: not line.startswith("#"))

# Map each line to a tuple of two integers, remove duplicates, group by key
pairs = lines.map(lambda x: x.split("\t")).map(lambda x: (x[0], x[1])).distinct()
edges = pairs.groupByKey() # (node, [neighbors])

# Initialize PageRank values for each node
ranks = edges.map(lambda x: (x[0], 1.0))

# Set the damping factor (beta)
beta = 0.85

# Perform a single iteration of PageRank algorithm
for iteration in range(10):
    # Calculate contributions from each node to the rank of neighbors
    edges_with_rank = edges.join(ranks)
    contributions = edges_with_rank.flatMap(calculate_contributions)
    # (neighbor, the contribution of node to the rank of each neighbor)
    # Re-calculate node ranks based on neighbor contributions
    ranks = contributions.reduceByKey(add).mapValues(lambda rank: rank * beta + 1 - beta)

# Save the output file
outputDF = ranks.map(lambda x: (x[0], str(x[1]))).toDF(["node", "rank"])
outputDF.write.option("overwrite", True).text(sys.argv[2])

# Stop the SparkSession
spark.stop()

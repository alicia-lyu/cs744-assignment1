from pyspark.sql import SparkSession
from operator import add
import sys

def get_contribution_per_edge(edge_with_params):
    node, (neighbor, params) = edge_with_params
    out_degree, rank = params
    return (neighbor, rank / out_degree)

def pretreat(line):
    line = line[0]
    words = line.split()
    if len(words) == 1:
        edge = ("#", words[0])
    edge = (words[0], words[1])
    return edge

# Create a SparkSession
if (sys.argv[1].endswith(".txt")):
    data_name = "web"
else:
    data_name = "wiki"

spark = SparkSession.builder.appName("PageRank-Task1-%s" % data_name).getOrCreate()
rdd = spark.read.text(sys.argv[1]).rdd

# Convert to an RDD, and filter out lines starting with '#'
edges = rdd.map(pretreat).filter(lambda x: not x[0] == "#")
nodes = edges.flatMap(lambda edge: [edge[0], edge[1]]).distinct()

ranks = nodes.map(lambda x: (x, 1.0)) # (node, rank=1.0)
out_degrees = edges.map(lambda x: (x[0], 1)).reduceByKey(add) # (node, number of neighbors)

# Set the damping factor (beta)
beta = 0.85

# Perform a single iteration of PageRank algorithm
for iteration in range(10):
    params = out_degrees.join(ranks) # (node, (out_degree, newest_rank))
    edges_with_params = edges.join(params) # (node, (neighbor, (out_degree, rank)))
    contribution_per_edge = edges_with_params.map(get_contribution_per_edge)
    contribution_sum = contribution_per_edge.reduceByKey(add)
    ranks = contribution_sum.mapValues(lambda rank: rank * beta + 1 - beta)

# Save the output file
outputDF = ranks.map(lambda x: (x[0], str(x[1]))).toDF(["node", "rank"])
outputDF.write.option("overwrite", True).csv(sys.argv[2])

# Stop the SparkSession
spark.stop()

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
    if len(words) < 2:
        edge = ("#", words[0])
    edge = (words[0], words[1])
    return edge

# Create a SparkSession and read the data into an RDD
if (sys.argv[1].endswith(".txt")):
    data_name = "web"
    partition_edges = 6
    partition_nodes = 1
else:
    data_name = "wiki"
    partition_edges = 24
    partition_nodes = 3

spark = SparkSession.builder.appName("PageRank-Task1-%s" % data_name).getOrCreate()
rdd = spark.read.text(sys.argv[1]).rdd.repartition(partition_edges)

# Convert lines into edges and nodes
edges = rdd.map(pretreat).filter(lambda x: not x[0] == "#").cache()
nodes = edges.flatMap(lambda edge: [edge[0], edge[1]]).distinct().repartition(partition_nodes)

# Initialize the ranks
ranks = nodes.map(lambda x: (x, 1.0)) # (node, rank=1.0)
# Calculate the out-degree of each node
out_degrees = edges.map(lambda x: (x[0], 1)).reduceByKey(add).cache() # (node, number of neighbors)

# Set the damping factor for pagerank update
beta = 0.85

for iteration in range(10):
    # Add the rank and out_degree of node to each edge
    params = out_degrees.join(ranks) # (node, (out_degree, newest_rank))
    edges_with_params = edges.join(params) # (node, (neighbor, (out_degree, rank)))
    # Compute the contribution of each edge to the rank of the neighbor
    contribution_per_edge = edges_with_params.map(get_contribution_per_edge) # (neighbor, contribution)
    # Sum the contributions for each neighbor
    contribution_sum = contribution_per_edge.reduceByKey(add)
    ranks = contribution_sum.mapValues(lambda contribution_sum: contribution_sum * beta + 1 - beta)

# Save the output file
outputDF = ranks.map(lambda x: (x[0], str(x[1]))).toDF(["node", "rank"])
outputDF.write.mode("overwrite").option("header", True).csv(sys.argv[2])

# Stop the SparkSession
spark.stop()

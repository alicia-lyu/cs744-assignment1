from pyspark.sql import SparkSession
from operator import add
import sys
from pyspark.sql.functions import broadcast

def get_contribution_per_edge(edge_with_params):
    node, (neighbor, params) = edge_with_params
    out_degree, rank = params
    return (neighbor, rank / out_degree)

def pretreat(line):
    line = line[0]
    words = line.split()
    if len(words) < 2:
        return (None, words[0])
    elif words[0] == "#":
        return (None, words[0])
    return (words[0], words[1])

# Create a SparkSession and read the data into an RDD
data_file_name = sys.argv[1]
if (data_file_name.endswith(".txt")):
    data_name = "web"
else:
    data_name = "wiki"

spark = SparkSession.builder.appName("PageRank-Task1-%s" % data_name).getOrCreate()
rdd = spark.read.text(data_file_name).rdd

# Convert lines into edges and nodes
edges = rdd.map(pretreat).filter(lambda x: not x[0] == None)
nodes = edges.flatMap(lambda edge: [edge[0], edge[1]]).distinct().toDF()

# Initialize the ranks 
ranks = nodes.map(lambda x: (x, 1.0)).toDF() # (node, rank=1.0)
# Calculate the out-degree of each node
out_degrees = edges.map(lambda x: (x[0], 1)).reduceByKey(add).toDF() # (node, number of neighbors)

# Set the damping factor for pagerank update
beta = 0.85

for iteration in range(10):
    # Add the rank and out_degree of node to each edge
    params = out_degrees.join(ranks) # (node, (out_degree, newest_rank))
    edges_with_params = edges.join(broadcast(params)) # (node, (neighbor, (out_degree, rank)))
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

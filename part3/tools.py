from operator import add
import datetime

def get_contribution_per_edge(neighbor_lists_with_ranks):
    node, (neighbors, rank) = neighbor_lists_with_ranks
    return [(neighbor, rank / len(neighbors)) for neighbor in neighbors]

def pretreat(line):
    line = line[0]
    words = line.split()
    if len(words) < 2:
        return (None, words[0])
    elif words[0] == "#":
        return (None, words[0])
    return (words[0], words[1])

def page_rank(rdd, task_num, partition_edges, output_dir, iteration_num):
    if task_num in [1, 5]:
        print("Default number of partitions: %d" % rdd.getNumPartitions())
        try:
            with open("./default_partitions.txt", "a") as f:
                f.write("Default number of partitions: " + str(rdd.getNumPartitions()) \
                        + ". At" + str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")
        except:
            print("Failed to write to default_partitions.txt")
    elif task_num in [2, 3]:
        if partition_edges > 3**6: # Task 3 Experiment 2
            pass
        else:
            rdd = rdd.repartition(partition_edges)

    # Convert lines into edges and neighbor_lists
    if task_num < 5:
        edges = rdd.map(pretreat).filter(lambda x: not x[0] == None).distinct()
        neighbor_lists = edges.groupByKey() # (node, [neighbors])
    else: # task_num == 5
        # Optimization: remove duplicate edges within the neighbor_list of each node
        edges = rdd.map(pretreat).filter(lambda x: not x[0] == None)
        neighbor_lists = edges.groupByKey()
        neighbor_lists = neighbor_lists.map(lambda x: (x[0], list(set(x[1]))))
        neighbor_lists = neighbor_lists.partitionBy(None, partitionFunc=lambda x: hash(x))

    # OPTIMIZATION: neighbor_lists is a hot spot
    if task_num == 3:
        neighbor_lists.cache()
    # Initialize the ranks
    ranks = neighbor_lists.map(lambda x: (x[0], 1.0)) # (node, rank=1.0)
    if task_num == 5:
        ranks = ranks.partitionBy(None, partitionFunc=lambda x: hash(x))
    if task_num == 3:
        ranks.cache()

    # Set the damping factor for pagerank update
    beta = 0.85

    for iteration in range(iteration_num):
        # Add the rank to neighbor_lists for contribution calculation
        neighbor_lists_with_ranks = neighbor_lists.join(ranks) # (node, ([neighbors], rank))
        # Compute the contribution of each edge to the rank of the neighbor
        contribution_per_edge = neighbor_lists_with_ranks.flatMap(get_contribution_per_edge) # (neighbor, contribution)
        # Sum the contributions for each neighbor
        contribution_sum = contribution_per_edge.reduceByKey(add)
        if task_num == 3:
            ranks.unpersist()
        ranks = contribution_sum.mapValues(lambda contribution_sum: contribution_sum * beta + 1 - beta)
        if task_num == 3:
            ranks.cache()

    # Save the output file
    outputDF = ranks.map(lambda x: (x[0], str(x[1]))).toDF(["node", "rank"])
    outputDF.write.mode("overwrite").option("header", True).csv(output_dir)
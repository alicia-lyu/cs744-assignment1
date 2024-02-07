from pyspark.sql import SparkSession
from func import page_rank

def task(task_num, data_file_name, output_dir):

    # Create a SparkSession and read the data into an RDD
    partition_nodes = 1
    if (data_file_name.endswith(".txt")):
        data_name = "web"
        partition_edges = 6
    else:
        data_name = "wiki"
        partition_edges = 24

    spark = SparkSession.builder.appName("PageRank-Task%d-%s" % (task_num, data_name)).getOrCreate()
    rdd = spark.read.text(data_file_name).rdd
    
    # Run the page rank algorithm
    page_rank(rdd, task_num, partition_edges, partition_nodes, output_dir)

    # Stop the SparkSession
    spark.stop()

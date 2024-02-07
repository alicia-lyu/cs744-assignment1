from pyspark.sql import SparkSession
from tools import page_rank

def task(task_num, data_file_name, output_dir):

    # Create a SparkSession and read the data into an RDD
    if (data_file_name.endswith(".txt")):
        data_name = "web"
        partition_edges = 15
    else:
        data_name = "wiki"
        partition_edges = 15**2

    spark = SparkSession.builder.appName("PageRank-Task%d-%s" % (task_num, data_name)).getOrCreate()
    rdd = spark.read.text(data_file_name).rdd
    
    # Run the page rank algorithm
    page_rank(rdd, task_num, partition_edges, output_dir)

    # Stop the SparkSession
    spark.stop()

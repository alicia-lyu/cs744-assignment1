from pyspark.sql import SparkSession
from tools import page_rank

def task_big(task_num, experiment_num, output_dir):

    if task_num == 2:
        partition_edges = 3**(experiment_num+1)
    elif task_num == 3:
        partition_edges = 3**(6*experiment_num) # 3**6; or 3**12, but will not be used
    else:
        partition_edges = 3**6 # Will not be used
    
    # Create a SparkSession and read the data into an RDD
    spark = SparkSession.builder.appName("PageRank-wiki-Task%d-Experiment%d" % (task_num, experiment_num)).getOrCreate()
    rdd = spark.read.text("hdfs://10.10.1.1:9000/data/enwiki-pages-articles").rdd

    if task_num == 4:
        task_num = 1 # Run the same program as task 1, only to drop a worker
    
    # Run the page rank algorithm
    page_rank(rdd, task_num, partition_edges, output_dir, 3)

    # Stop the SparkSession
    spark.stop()

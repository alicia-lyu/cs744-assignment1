#!/bin/bash

# $1: task_num
# $2: experiment_num

# Make sure /data exists
if ! hadoop fs -test -d /data ; then
    hadoop fs -mkdir /data
    echo "hadoop fs mkdir data"
fi

# Make sure enwiki-pages-articles exists
if ! hadoop fs -test -d /data/enwiki-pages-articles ; then
    hadoop fs -copyFromLocal /mnt/data/datasets/enwiki-pages-articles /data/enwiki-pages-articles
fi

# Make sure /big exists
if ! hadoop fs -test -d /big ; then
    hadoop fs -mkdir /big
    echo "hadoop fs mkdir big"
fi

# Make sure /big/task$1 exists
if ! hadoop fs -test -d /big/task$1 ; then
    hadoop fs -mkdir /big/task$1
    echo "hadoop fs mkdir task$1"
fi

# Make sure /big/task$1/experiment$2 exists
if ! hadoop fs -test -d /big/task$1/experiment$2 ; then
    hadoop fs -mkdir /big/task$1/experiment$2
    echo "hadoop fs mkdir experiment$2"
fi

# spark-submit
/mnt/data/spark-3.3.4-bin-hadoop3/bin/spark-submit \
  --py-files ./task_big.py,./tools.py \
  ./main_big.py \
  $1 $2 "hdfs://10.10.1.1:9000/part3/big/task$1/experiment$2" 
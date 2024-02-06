#!/bin/bash

# $1: task_num
# $2: data file/dir name, either  or enwiki-pages-articles
# Make sure /part3 exists
if ! hadoop fs -test -d /part3 ; then
    hadoop fs -mkdir /part3
    echo "hadoop fs mkdir part3"
fi

# Make sure /part3/datadir exists

if [ "$2" = "web-BerkStan.txt" ]; then
    dir_by_data="/part3/web"
elif [ "$2" = "enwiki-pages-articles" ]; then
    dir_by_data="/part3/wiki"
else
    echo "Invalid data"
    exit 1
fi

if ! hadoop fs -test -d $dir_by_data ; then
    hadoop fs -mkdir $dir_by_data
    echo "hadoop fs mkdir $dir_by_data"
fi

# Make sure /part3/datadir/task$1 exists
if ! hadoop fs -test -d $dir_by_data/task$1 ; then
    hadoop fs -mkdir $dir_by_data/task$1
    echo "hadoop fs mkdir task$1"
fi

# Make sure data file is in hdfs
if ! hadoop fs -test -e /part3/task1/$2 ; then
	hadoop fs -copyFromLocal /mnt/data/datasets/$2 /data/
	echo "hadoop fs copyFromLocal"
fi

# spark-submit
/mnt/data/spark-3.3.4-bin-hadoop3/bin/spark-submit \
  task$1.py \
  "hdfs://10.10.1.1:9000/data/$2" "hdfs://10.10.1.1:9000/$dir_by_data/task$1/output" 
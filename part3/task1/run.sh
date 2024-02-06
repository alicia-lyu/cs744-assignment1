#!/bin/bash
if ! hadoop fs -test -d /part3 ; then
	hadoop fs -mkdir /part3
	echo "hadoop fs mkdir part3"
fi

if ! hadoop fs -test -d /part3/task1 ; then
	hadoop fs -mkdir /part3/task1
	echo "hadoop fs mkdir task1"
fi

if ! hadoop fs -test -e /part3/task1/web-BerkStan.txt ; then
	hadoop fs -copyFromLocal /mnt/data/datasets/web-BerkStan.txt /part3/task1/
	echo "hadoop fs copyFromLocal"
fi

/mnt/data/spark-3.3.4-bin-hadoop3/bin/spark-submit \
  main.py \
  "hdfs://10.10.1.1:9000/part3/task1/web-BerkStan.txt" "hdfs://10.10.1.1:9000/part3/task1/output" 

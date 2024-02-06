#!/bin/bash
if ! hadoop fs -test -d /part2 ; then
	hadoop fs -mkdir /part2
	echo "hadoop fs mkdir"
fi

if ! hadoop fs -test -e /part2/export.csv ; then
	hadoop fs -copyFromLocal /mnt/data/datasets/export.csv /part2
	echo "hadoop fs copyFromLocal"
fi

/mnt/data/spark-3.3.4-bin-hadoop3/bin/spark-submit \
  main.py \
  "hdfs://10.10.1.1:9000/part2/export.csv" "hdfs://10.10.1.1:9000/part2/output" 

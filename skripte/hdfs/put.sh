#!/bin/bash

echo "Loading data into HDFS"

docker exec -it namenode bash -c "hadoop fs -mkdir /data"
docker exec -it namenode bash -c "hadoop fs -put /data/US_Accidents_March23.csv /data/"
docker exec -it namenode bash -c "hadoop fs -ls /data"

echo "Data loaded into HDFS"
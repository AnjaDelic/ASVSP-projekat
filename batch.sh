#!/bin/bash

#Za mongo i metabase pogledati i odraditi ./skripte/monogo/mongo.sh

echo "HDFS - put.sh"
./skripte/hdfs/put.sh

echo "SPARK - batch.sh"
./skripte/spark/batch/batch.sh
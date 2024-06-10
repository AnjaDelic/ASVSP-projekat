#!/bin/bash

echo "Creating Kafka topics"

docker exec -it kafka1 bash -c "/usr/bin/kafka-topics --create --topic accidents_data --partitions 1 --replication-factor 1 --zookeeper zookeeper:2181"
docker exec -it kafka1 bash -c "/usr/bin/kafka-topics --list --zookeeper zookeeper:2181"

echo "Kafka topics created"
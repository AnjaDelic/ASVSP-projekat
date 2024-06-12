#!/bin/bash

echo "Running main.py in ./skripte/stvaranje_podataka/"
cd ./skripte/stvaranje_podataka/
python3 main.py

echo "Kafka - kafka.sh"
./skripte/kafka/kafka.sh

echo "SPARK - batch.sh"
./skripte/spark/streaming/streaming.sh
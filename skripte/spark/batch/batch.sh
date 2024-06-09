#!/bin/bash
docker cp /home/anja/Downloads/mongo-spark-connector_2.12:3.0.2.jar spark-master:/spark/jars/


echo "Preparing data"
docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 /spark/paketna_obrada/predobrada.py"


echo "Executing Spark batch queries"
sleep 1

for ((i=1; i<=10; i++)); do
    echo "Executing Query $i"
    sleep 1
    docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 /spark/paketna_obrada/query$i.py"
    echo "Finished Query $i"
    echo "Press any key to continue"
    read -n 1 -s
done

echo "Spark batch queries executed"
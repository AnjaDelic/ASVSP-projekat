
#!/bin/bash
docker cp /home/anja/Downloads/mongo-spark-connector_2.12:3.0.2.jar spark-master:/spark/jars/
docker exec -it spark-master bash
./spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 /spark/paketna_obrada/upit1.py 
./spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 /spark/paketna_obrada/upit2.py 
./spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 /spark/paketna_obrada/upit3.py 
./spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 /spark/paketna_obrada/upit4.py 
./spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 /spark/paketna_obrada/upit5.py 



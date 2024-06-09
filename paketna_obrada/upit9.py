#Prikazati grad u kojem se najviše nezgoda dogodilo između 2017. i 2022. godine i koji je to udeo od ukupnog broja nezgoda, izraziti u procentima. 

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, max, lit, expr
from pyspark.sql.types import *

# Logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

input_uri = "mongodb://mongodb:27017/accidents.accidents_data"
output_uri = "mongodb://mongodb:27017/city_with_most_accidents_2017_2022"

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("City with Most Accidents and Percentage") \
    .master('local')\
    .config("spark.mongodb.input.uri", input_uri) \
    .config("spark.mongodb.output.uri", output_uri) \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
    .getOrCreate()

quiet_logs(spark)

# Define HDFS namenode
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# Read the JSON file
df = spark.read.json(HDFS_NAMENODE + "/data/US_Accidents_March23_cleaned.json")

# Filter accidents between 2017 and 2022
df_filtered = df.filter((col("Start_Time").substr(1, 4).cast("int") >= 2017) & 
                        (col("Start_Time").substr(1, 4).cast("int") <= 2022))

# Group by city to count the number of accidents per city
city_accidents = df_filtered.groupBy("City").agg(count("ID").alias("Accident_Count"))

# Calculate the total number of accidents
total_accidents = df_filtered.select(count("ID").alias("Total_Accidents")).collect()[0][0]

# Find the city with the most accidents
max_accidents_city = city_accidents.orderBy(col("Accident_Count").desc()).limit(1)

# Calculate the percentage share of the total number of accidents
result = max_accidents_city.withColumn("Percentage_Share", (col("Accident_Count") / lit(total_accidents)) * 100)

# Show the results
result.show()

# Save the results to MongoDB
result.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri) \
    .option("database", "accidents") \
    .option("collection", "city_with_most_accidents_2017_2022") \
    .save()



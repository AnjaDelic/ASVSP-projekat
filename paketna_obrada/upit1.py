#!/usr/bin/python
#U kojih 5 država su najučestalije saobraćajne nesreće, koliko puta su zabeležene u svakoj od tih država i koji je to procenat od ukupnog broja zabeleženih nesreća?

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

input_uri = "mongodb://mongodb:27017/accidents.accidents_data"
output_uri = "mongodb://mongodb:27017/accidents.top_states"

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("Top 5 States") \
    .master('local')\
    .config("spark.mongodb.input.uri", input_uri) \
    .config("spark.mongodb.output.uri", output_uri) \
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
    .getOrCreate()

quiet_logs(spark)


# Define HDFS namenode
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# Read the CSV file
df = spark.read.json(HDFS_NAMENODE + "/data/US_Accidents_March23_cleaned.json")

# Get the total number of accidents
total_accidents = df.count()

# Define a window specification partitioned by State
windowSpec = Window.partitionBy("State")

# Calculate the count of accidents for each state
state_counts = df.withColumn("AccidentCount", F.count("ID").over(windowSpec))

# Get the top 5 states with the highest number of accidents
top_states = state_counts.select("State", "AccidentCount") \
    .distinct() \
    .groupBy("State") \
    .agg(F.sum("AccidentCount").alias("TotalAccidents")) \
    .orderBy(F.col("TotalAccidents").desc()) \
    .limit(5)

# Calculate the percentage of accidents for each of the top 5 states
top_states = top_states.withColumn("Percentage", (F.col("TotalAccidents") / total_accidents) * 100)

# Show the results
top_states.show()

# Save the results to MongoDB
top_states.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri) \
    .option("database", "accidents") \
    .option("collection", "top_states") \
    .save()

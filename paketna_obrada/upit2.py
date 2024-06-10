#!/usr/bin/python
#Za svaku državu izlistati prosečan broj nezgoda za svaku godinu i za svaki mesec u toj godini.

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, count, avg
from pyspark.sql.window import Window

# Logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

input_uri = "mongodb://mongodb:27017/accidents.accidents_data"
output_uri = "mongodb://mongodb:27017/accidents.avg_month_year"

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("Average Accidents per Year and Month per State") \
    .master('local')\
    .config("spark.mongodb.input.uri", input_uri) \
    .config("spark.mongodb.output.uri", output_uri) \
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
    .getOrCreate()

quiet_logs(spark)

# Define HDFS namenode
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# Read the JSON file
location_df = spark.read.json(HDFS_NAMENODE + "/data/location_df.json")
accident_df = spark.read.json(HDFS_NAMENODE + "/data/accident_df.json")

# Define Window specification
windowSpec = Window.partitionBy("State", year("Start_Time"), month("Start_Time"))

# Join location_df and accident_df on ID
joined_df = location_df.join(accident_df, "ID")

# Add a column with the count of accidents per state, year, and month
joined_df_with_counts = joined_df.withColumn("AccidentCount", count("ID").over(windowSpec))

# Calculate the average number of accidents per state, year, and month
avg_accidents_per_month_per_state = joined_df_with_counts \
    .groupBy("State", year("Start_Time").alias("Year"), month("Start_Time").alias("Month")) \
    .agg(avg("AccidentCount").alias("AvgAccidentsPerMonth")) \
    .orderBy("State", "Year", "Month")

# Calculate the average number of accidents per state and year
avg_accidents_per_year_per_state = avg_accidents_per_month_per_state \
    .groupBy("State", "Year") \
    .agg(avg("AvgAccidentsPerMonth").alias("AvgAccidentsPerYear"))

# Show the results with the Month column
final_result = avg_accidents_per_year_per_state.join(avg_accidents_per_month_per_state, on=["State", "Year"], how="inner")

final_result.show()

# Save the results to MongoDB
final_result.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri) \
    .option("database", "accidents") \
    .option("collection", "avg_month_year") \
    .save()

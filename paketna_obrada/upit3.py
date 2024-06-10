#!/usr/bin/python

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when
from pyspark.sql.window import Window

# Logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

input_uri = "mongodb://mongodb:27017/accidents.accidents_data"
output_uri = "mongodb://mongodb:27017/accidents.high_impact_windy_states"

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("High Impact Windy States") \
    .master('local')\
    .config("spark.mongodb.input.uri", input_uri) \
    .config("spark.mongodb.output.uri", output_uri) \
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
    .getOrCreate()
quiet_logs(spark)

# Define HDFS namenode
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# Read the JSON files
location_df = spark.read.json(HDFS_NAMENODE + "/data/location_df.json")
weather_df = spark.read.json(HDFS_NAMENODE + "/data/weather_df.json")
accident_df = spark.read.json(HDFS_NAMENODE + "/data/accident_df.json")

# Join location_df, weather_df, and accident_df on ID
joined_df = location_df.join(weather_df, "ID").join(accident_df, "ID")

# Filter accidents during the day and with wind speed greater than average
filtered_df = joined_df.filter(
    (col("Sunrise_Sunset") == "Day") &
    (col("Wind_Speed(kmh)") > col("Wind_Speed(kmh)").cast("float"))  
)

# Define Window specification
windowSpec = Window.partitionBy("State")

# Add a column with the count of significant impact accidents per state
joined_df_with_counts = filtered_df.withColumn("SignificantImpactAccidentsCount", 
                                               count(when(col("Severity") >= 3, True)).over(windowSpec))

# Select the state with the highest number of significant impact accidents
top_state_high_impact_accidents = joined_df_with_counts.select("State", 
                                                               "SignificantImpactAccidentsCount") \
                                                      .distinct() \
                                                      .orderBy(col("SignificantImpactAccidentsCount").desc()) \
                                                      .limit(1)

# Show the result
top_state_high_impact_accidents.show()

# Save the results to MongoDB
top_state_high_impact_accidents.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri) \
    .option("database", "accidents") \
    .option("collection", "high_impact_windy_states") \
    .save()

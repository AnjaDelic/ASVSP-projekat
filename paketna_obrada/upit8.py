import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, year, dayofweek, hour
from pyspark.sql.window import Window
from pyspark.sql.types import *

# Logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

input_uri = "mongodb://mongodb:27017/accidents.accidents_data"
output_uri = "mongodb://mongodb:27017/avg_accidents_per_day_hour_2022"

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("Average Accidents per Day and Hour in 2022") \
    .master('local')\
    .config("spark.mongodb.input.uri", input_uri) \
    .config("spark.mongodb.output.uri", output_uri) \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
    .getOrCreate()

quiet_logs(spark)

# Define HDFS namenode
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# Read the JSON files
location_df = spark.read.json(HDFS_NAMENODE + "/data/location_df.json")
accident_df = spark.read.json(HDFS_NAMENODE + "/data/accident_df.json")

# Join accident_df with location_df to get city information
accident_with_city_df = accident_df.join(location_df.select("ID", "City"), on="ID")

# Filter accidents for the year 2022
df_2022 = accident_with_city_df.filter(year(col("Start_Time")) == 2022)

# Extract day of the week and hour from Start_Time
df_2022 = df_2022.withColumn("DayOfWeek", dayofweek(col("Start_Time"))) \
                 .withColumn("Hour", hour(col("Start_Time")))

# Group by city, day of the week, and hour to calculate the number of accidents
city_day_hour_accidents = df_2022.groupBy("City", "DayOfWeek", "Hour") \
                                 .agg(count("ID").alias("Accident_Count"))

# Calculate the average number of accidents per day and hour for each city
result = city_day_hour_accidents.withColumn("AvgAccidentsPerDayHour", avg("Accident_Count").over(Window.partitionBy("City", "DayOfWeek", "Hour")))

# Select distinct results
result = result.select("City", "DayOfWeek", "Hour", "AvgAccidentsPerDayHour").distinct()

# Show the results
result.show(100)  # Adjust the number of rows to display as needed

# Save the results to MongoDB
result.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri) \
    .option("database", "accidents") \
    .option("collection", "avg_accidents_per_day_hour_2022") \
    .save()

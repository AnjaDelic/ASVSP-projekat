import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofweek, hour, count, avg
from pyspark.sql.window import Window

# Logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

input_uri = "mongodb://mongodb:27017/accidents.accidents_data"
output_uri = "mongodb://mongodb:27017/accidents.avg_day_hour_chicago"

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("Average Accidents per Day and Hour for Chicago ") \
    .master('local') \
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

# Filter accidents for Chicago
location_df2 = location_df.filter(col("City") == "Chicago")
chicago_df = accident_df.join(location_df2.select("ID", "City"), on="ID")


# Define Window specification
windowSpec = Window.partitionBy("City", dayofweek("Start_Time"), hour("Start_Time"))

# Add a column for the count of accidents per hour
chicago_accidents_with_counts = chicago_df.withColumn("AccidentCount", count("ID").over(windowSpec))

# Group by city, day, and hour, calculating the average accidents per hour
avg_accidents_per_hour = chicago_accidents_with_counts \
    .groupBy("City", dayofweek("Start_Time").alias("Day"), hour("Start_Time").alias("Hour")) \
    .agg(avg("AccidentCount").alias("AvgAccidentsPerHour")) \
    .orderBy("City", "Day", "Hour")

# Group by city and day, calculating the average accidents per day
avg_accidents_per_day = avg_accidents_per_hour \
    .groupBy("City", "Day") \
    .agg(avg("AvgAccidentsPerHour").alias("AvgAccidentsPerDay"))

# Join the hourly and daily averages
final_result = avg_accidents_per_day.join(avg_accidents_per_hour, on=["City", "Day"], how="inner")

final_result.show()

# Save the results to MongoDB
output_uri = "mongodb://mongodb:27017/accidents.avg_day_hour_chicago"
final_result.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri) \
    .option("database", "accidents") \
    .option("collection", "avg_day_hour_chicago") \
    .save()
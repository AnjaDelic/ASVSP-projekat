import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count
from pyspark.sql.types import *
#************************************************
# Logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

input_uri = "mongodb://mongodb:27017/accidents.accidents_data"
output_uri_avg_temp = "mongodb://mongodb:27017/avg_temp_city"

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("Accidents Analysis") \
    .master('local') \
    .config("spark.mongodb.input.uri", input_uri) \
    .config("spark.mongodb.output.uri", output_uri_avg_temp) \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
    .getOrCreate()

quiet_logs(spark)

# Define HDFS namenode
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# Read the JSON files
location_df = spark.read.json(HDFS_NAMENODE + "/data/location_df.json")
weather_df = spark.read.json(HDFS_NAMENODE + "/data/weather_df.json")
accident_df = spark.read.json(HDFS_NAMENODE + "/data/accident_df.json")

# Join accident_df with location_df to get city information
accident_with_city_df = accident_df.join(location_df.select("ID", "City"), on="ID")

# Join accident_with_city_df with weather_df to get temperature information in Fahrenheit
accident_weather_city_df = accident_with_city_df.join(weather_df.select("ID", "Temperature(C)"), on="ID")

# Group by city to count the number of accidents per city and calculate the average temperature per city in Fahrenheit
city_accidents_temp = accident_weather_city_df.groupBy("City") \
                                              .agg(
                                                  count("ID").alias("Accident_Count"),
                                                  avg("Temperature(C)").alias("AverageTemperature(C)")
                                              )

# Calculate the average number of accidents across all cities
average_accidents = city_accidents_temp.select(avg("Accident_Count").alias("Avg_Accidents")).collect()[0][0]

# Filter cities with below average accidents
below_avg_accidents = city_accidents_temp.filter(col("Accident_Count") < average_accidents)

# Show the results
below_avg_accidents.show()

# Save the results to MongoDB
below_avg_accidents.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri_avg_temp) \
    .option("database", "accidents") \
    .option("collection", "avg_temp_city") \
    .save()

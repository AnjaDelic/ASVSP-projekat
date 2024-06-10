import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc
from pyspark.sql.window import Window
from pyspark.sql.types import *

# Logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

input_uri = "mongodb://mongodb:27017/accidents.accidents_data"
output_uri = "mongodb://mongodb:27017/top_15_cities_mostly_cloudy_high_humidity"

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("Accidents in Mostly Cloudy Weather") \
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
weather_df = spark.read.json(HDFS_NAMENODE + "/data/weather_df.json")

# Calculate the average humidity
average_humidity = weather_df.select(avg("Humidity(%)").alias("Avg_Humidity")).collect()[0][0]

# Filter accidents that occurred in mostly cloudy weather and with humidity greater than the average
filtered_df = weather_df.filter(
    (col("Weather_Condition") == "Mostly Cloudy") &
    (col("Humidity(%)") > average_humidity)
)

# Join with location_df to get city information
filtered_df = filtered_df.join(location_df, "ID")

# Define window specification to count accidents per city
windowSpec = Window.partitionBy("City")

# Use window function to count accidents per city
city_accidents = filtered_df.withColumn("Accident_Count", count("ID").over(windowSpec))

# Order by the number of accidents in descending order and select the top 15 cities
top_15_cities = city_accidents.select("City", "Accident_Count") \
                               .distinct() \
                               .orderBy(desc("Accident_Count")) \
                               .limit(15)

# Show the results
top_15_cities.show()

# Save the results to MongoDB
top_15_cities.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri) \
    .option("database", "accidents") \
    .option("collection", "top_15_cities_mostly_cloudy_high_humidity") \
    .save()

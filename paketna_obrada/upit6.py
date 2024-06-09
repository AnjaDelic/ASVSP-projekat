#Izlistati gradove u kojima je zabeležen broj nezgoda manji od prosečnog broja zabeleženih nesreća i u njima izračunati prosečnu temperaturu u stepenima celzijusa. 
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, avg, count, lit
from pyspark.sql.window import Window

# Logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

input_uri = "mongodb://mongodb:27017/accidents.accidents_data"
output_uri = "mongodb://mongodb:27017/temp"

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("Average Temperature and Accidents") \
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

# Group by city to count the number of accidents per city
city_accidents = df.groupBy("City").agg(count("ID").alias("Accident_Count"))

# Calculate the average number of accidents across all cities
average_accidents = city_accidents.select(avg("Accident_Count").alias("Avg_Accidents")).collect()[0][0]

# Convert the temperature to Celsius
df = df.withColumn("Temperature(C)", (col("Temperature(F)") - 32) * 5 / 9)

# Join the accident counts with the temperature data
city_data = city_accidents.join(df, on="City")

# Use window function to calculate the average temperature per city
windowSpec = Window.partitionBy("City")
result = city_data.withColumn("AverageTemperature(C)", avg("Temperature(C)").over(windowSpec)) \
                  .withColumn("OverallAverageAccidents", lit(average_accidents)) \
                  .withColumn("BelowAverageAccidents", col("Accident_Count") < col("OverallAverageAccidents")) \
                  .select("City", "AverageTemperature(C)", "Accident_Count", "OverallAverageAccidents", "BelowAverageAccidents") \
                  .distinct() \
                  .filter(col("BelowAverageAccidents") == True)

# Calculate the overall average temperature in Celsius
overall_avg_temp = df.select(avg("Temperature(C)").alias("OverallAverageTemperature(C)")).collect()[0][0]

# Show the results
result.show()
print(f"Overall Average Temperature (C): {overall_avg_temp}")

# Save the results to MongoDB
result.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri) \
    .option("database", "accidents") \
    .option("collection", "temp") \
    .save()



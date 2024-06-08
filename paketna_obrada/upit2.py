#!/usr/bin/python

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, year, month
from pyspark.sql.types import *

# Logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

input_uri = "mongodb://mongodb:27017/accidents.accidents_data"
output_uri = "mongodb://mongodb:27017/accidents.avg_accidents_per_year_month_state"

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("Average Accidents Per Year and Month by State") \
    .master('local')\
    .config("spark.mongodb.input.uri", input_uri) \
    .config("spark.mongodb.output.uri", output_uri) \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
    .getOrCreate()

quiet_logs(spark)

# Define the schema for the accident information
accident_schema = StructType([
    StructField("ID", StringType(), True),
    StructField("Source", StringType(), True),
    StructField("Severity", IntegerType(), True),
    StructField("Start_Time", TimestampType(), True),
    StructField("End_Time", TimestampType(), True),
    StructField("Start_Lat", DoubleType(), True),
    StructField("Start_Lng", DoubleType(), True),
    StructField("End_Lat", DoubleType(), True),
    StructField("End_Lng", DoubleType(), True),
    StructField("Distance(mi)", DoubleType(), True),
    StructField("Description", StringType(), True),
    StructField("Street", StringType(), True),
    StructField("City", StringType(), True),
    StructField("County", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Zipcode", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Timezone", StringType(), True),
    StructField("Airport_Code", StringType(), True),
    StructField("Weather_Timestamp", TimestampType(), True),
    StructField("Temperature(F)", DoubleType(), True),
    StructField("Wind_Chill(F)", DoubleType(), True),
    StructField("Humidity(%)", DoubleType(), True),
    StructField("Pressure(in)", DoubleType(), True),
    StructField("Visibility(mi)", DoubleType(), True),
    StructField("Wind_Direction", StringType(), True),
    StructField("Wind_Speed(mph)", DoubleType(), True),
    StructField("Precipitation(in)", DoubleType(), True),
    StructField("Weather_Condition", StringType(), True),
    StructField("Amenity", BooleanType(), True),
    StructField("Bump", BooleanType(), True),
    StructField("Crossing", BooleanType(), True),
    StructField("Give_Way", BooleanType(), True),
    StructField("Junction", BooleanType(), True),
    StructField("No_Exit", BooleanType(), True),
    StructField("Railway", BooleanType(), True),
    StructField("Roundabout", BooleanType(), True),
    StructField("Station", BooleanType(), True),
    StructField("Stop", BooleanType(), True),
    StructField("Traffic_Calming", BooleanType(), True),
    StructField("Traffic_Signal", BooleanType(), True),
    StructField("Turning_Loop", BooleanType(), True),
    StructField("Sunrise_Sunset", StringType(), True),
    StructField("Civil_Twilight", StringType(), True),
    StructField("Nautical_Twilight", StringType(), True),
    StructField("Astronomical_Twilight", StringType(), True)
])

# Define HDFS namenode
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# Read the CSV file
df = spark.read.csv(HDFS_NAMENODE + "/data/US_Accidents_March23.csv", header=True, schema=accident_schema)


# Extract year and month from the Start_Time
df = df.withColumn("Year", year(col("Start_Time"))) \
       .withColumn("Month", month(col("Start_Time")))

# Group by State, Year, and calculate the average number of accidents for each state and year
avg_accidents_state_year = df.groupBy("State", "Year") \
    .agg(count("ID").alias("Accidents")) \
    .groupBy("State") \
    .agg(avg("Accidents").alias("AvgAccidentsPerYear"))

# Group by State, Year, and Month and calculate the average number of accidents for each state, year, and month
avg_accidents_state_year_month = df.groupBy("State", "Year", "Month") \
    .agg(count("ID").alias("Accidents")) \
    .groupBy("State", "Year", "Month") \
    .agg(avg("Accidents").alias("AvgAccidents"))

# Show the results
avg_accidents_state_year.show()
avg_accidents_state_year_month.show()

# Save the results to MongoDB
avg_accidents_state_year.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri + "_state_year") \
    .option("database", "accidents") \
    .option("collection", "avg_accidents_per_year_state") \
    .save()

avg_accidents_state_year_month.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri + "_state_year_month") \
    .option("database", "accidents") \
    .option("collection", "avg_accidents_per_year_month_state") \
    .save()


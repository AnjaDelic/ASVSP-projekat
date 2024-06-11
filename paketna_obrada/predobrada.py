#!/usr/bin/python

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.functions import lit, regexp_replace, round

# Logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("EDA") \
    .master('local')\
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

# Drop rows with any NA values
df_clean = df.dropna()

# Show the count of remaining rows
print("Number of rows after dropping NA values: ", df_clean.count())

#Number of rows after dropping NA values:  3554549


# Define a UDF to convert Fahrenheit to Celsius
def fahrenheit_to_celsius(temp_f):
    return round((temp_f - 32) * 5.0/9.0, 2)

# Register the UDF
spark.udf.register("fahrenheit_to_celsius", fahrenheit_to_celsius)

# Transformations
location_df = df_clean.select(
    regexp_replace("ID", "mi", "").alias("ID"), 
    "City", "County", "State", "Country", "Zipcode", "Street",
    (col("Distance(mi)") * lit(1.60934)).alias("Distance(km)")
)

weather_df = df_clean.select(
    regexp_replace("ID", "mph", "").alias("ID"), 
    "Weather_Timestamp", 
    fahrenheit_to_celsius(col("Temperature(F)")).alias("Temperature(C)"), 
    fahrenheit_to_celsius(col("Wind_Chill(F)")).alias("Wind_Chill(C)"), 
    "Humidity(%)", 
    "Pressure(in)", 
    (col("Visibility(mi)") * lit(1.60934)).alias("Visibility(km)"), 
    "Wind_Direction", 
    (col("Wind_Speed(mph)") * lit(1.60934)).alias("Wind_Speed(kmh)"), 
    "Precipitation(in)", 
    "Weather_Condition"
)

accident_df = df_clean.select(
    regexp_replace("ID", "mi", "").alias("ID"), 
    "Source", "Severity", "Start_Time", "End_Time", "Start_Lat", "Start_Lng", 
    "End_Lat", "End_Lng", 
    (col("Distance(mi)") * lit(1.60934)).alias("Distance(km)"), 
    "Description", "Amenity", "Bump", "Crossing", 
    "Give_Way", "Junction", "No_Exit", "Railway", "Roundabout", "Station", "Stop", 
    "Traffic_Calming", "Traffic_Signal", "Turning_Loop", "Sunrise_Sunset", "Civil_Twilight", 
    "Nautical_Twilight", "Astronomical_Twilight"
)

# Save the cleaned DataFrame as a JSON file
location_df.write.json(HDFS_NAMENODE + "/data/location_df.json", mode="overwrite")
weather_df.write.json(HDFS_NAMENODE + "/data/weather_df.json", mode="overwrite")
accident_df.write.json(HDFS_NAMENODE + "/data/accident_df.json", mode="overwrite")

#!/usr/bin/python

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

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
df = spark.read.csv(HDFS_NAMENODE + "/data/US_Accidents_March23.csv", header=True,schema=accident_schema)

""" # Check for duplicate rows
duplicate_rows = df.groupBy(df.columns).count().filter("count > 1")
num_duplicate_rows = duplicate_rows.count()
print("Number of duplicate rows: ", num_duplicate_rows)

# Check for duplicate columns
duplicate_columns = []
columns = df.columns
for i, col1 in enumerate(columns):
    for col2 in columns[i+1:]:
        if df.select(col1).subtract(df.select(col2)).count() == 0 and df.select(col2).subtract(df.select(col1)).count() == 0:
            duplicate_columns.append((col1, col2))

print("Duplicate columns: ", duplicate_columns) """

print("SKIPPING DUPLICATION CHECK")

""" # Print the unique values for each column
for column in df.columns:
    unique_values = df.select(column).distinct().limit(10).collect()
    unique_values_list = [row[column] for row in unique_values]
    print(f"Column {column} has unique values: {unique_values_list}") """

print("SKIPPING UNIQUE VALUES PRINT")


# Drop rows with any NA values
df_clean = df.dropna()

# Show the count of remaining rows
print("Number of rows after dropping NA values: ", df_clean.count())

# Save the cleaned DataFrame as a JSON file
df_clean.write.json(HDFS_NAMENODE + "/data/US_Accidents_March23_cleaned.json", mode="overwrite")


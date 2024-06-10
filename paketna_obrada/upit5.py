import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import *

# Logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

input_uri = "mongodb://mongodb:27017/accidents.accidents_data"
output_uri = "mongodb://mongodb:27017/accidents.crosswalk_intersection_accidents"

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("Crosswalk and Intersection Accidents") \
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
accident_df = spark.read.json(HDFS_NAMENODE + "/data/accident_df.json")

# Join accident_df with location_df to get city information
accident_with_city_df = accident_df.join(location_df.select("ID", "City"), on="ID")

# Filter accidents that occurred with a distance between 5 and 7 km,
# near a crosswalk and intersection
crosswalk_intersection_accidents = accident_with_city_df.filter(
    (col("Distance(km)") >= 5.0) &
    (col("Distance(km)") <= 7.0) &
    (col("Crossing") == True) &
    (col("Junction") == True)
)

# Order by descending order of Start_Time
crosswalk_intersection_accidents = crosswalk_intersection_accidents.orderBy(desc("Start_Time"))

# Show the results
crosswalk_intersection_accidents.show()

# Save the results to MongoDB
crosswalk_intersection_accidents.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri) \
    .option("database", "accidents") \
    .option("collection", "crosswalk_intersection_accidents") \
    .save()

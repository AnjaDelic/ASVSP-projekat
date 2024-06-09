#Izlistati nezgode koje su se dogodile u blizini saobraćajnih znakova, imale su uticaj stepena 2 na saobraćaj i desile su se tokom noći i slabe vidljivosti.
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
output_uri = "mongodb://mongodb:27017/accidents.near_traffic_sign_accidents"

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("Near Traffic Sign Accidents") \
    .master('local')\
    .config("spark.mongodb.input.uri", input_uri) \
    .config("spark.mongodb.output.uri", output_uri) \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
    .getOrCreate()

quiet_logs(spark)

# Define HDFS namenode
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# Read the CSV file
df = spark.read.json(HDFS_NAMENODE + "/data/US_Accidents_March23_cleaned.json")


# Filter accidents that occurred near traffic signs, had a traffic impact severity level of 2,
# occurred during night and with low visibility
near_traffic_sign_accidents = df.filter(
    (col("Traffic_Signal") == True) & 
    (col("Severity") == 2) &
    (col("Sunrise_Sunset") == "Night") &
    (col("Visibility(mi)") < 1.0)
)

# Use window function to count accidents per city and order by the count
windowSpec = Window.partitionBy("City")
near_traffic_sign_accidents = near_traffic_sign_accidents.withColumn("AccidentsCount", count("ID").over(windowSpec))

# Order the results by the count of accidents in descending order
near_traffic_sign_accidents = near_traffic_sign_accidents.orderBy(desc("AccidentsCount"))

# Show the results
near_traffic_sign_accidents.show()

# Save the results to MongoDB
near_traffic_sign_accidents.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri) \
    .option("database", "accidents") \
    .option("collection", "near_traffic_sign_accidents") \
    .save()

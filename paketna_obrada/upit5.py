#Izlistati nezgode čija je pojava izazvala kolonu od 5 do 7 milja, u blizini ima pešački prelaz i raskrsnicu i sortirati ih prema datumu opadajuće.
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

# Read the CSV file
df = spark.read.json(HDFS_NAMENODE + "/data/US_Accidents_March23_cleaned.json")


# Filter accidents that occurred with a distance between 5 and 7 miles,
# near a crosswalk and intersection
crosswalk_intersection_accidents = df.filter(
    (col("Distance(mi)") >= 5.0) &
    (col("Distance(mi)") <= 7.0) &
    (col("Crossing") == True) &
    (col("Junction") == True)
)

# Use window function to count accidents per city and order by the count
windowSpec = Window.partitionBy("City")
crosswalk_intersection_accidents = crosswalk_intersection_accidents.withColumn("AccidentsCount", count("ID").over(windowSpec))

# Order the results by the count of accidents in descending order
crosswalk_intersection_accidents = crosswalk_intersection_accidents.orderBy(desc("AccidentsCount"))

# Show the results
crosswalk_intersection_accidents.show()

# Save the results to MongoDB
crosswalk_intersection_accidents.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri) \
    .option("database", "accidents") \
    .option("collection", "crosswalk_intersection_accidents") \
    .save()



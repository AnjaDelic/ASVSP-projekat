#Za grad Čikago prikazati prosečan broj nezgoda za svaki mesec.
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, count
from pyspark.sql.window import Window

# Logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

input_uri = "mongodb://mongodb:27017/accidents.accidents_data"
output_uri = "mongodb://mongodb:27017/accidents_per_month_chicago"

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("Average Accidents per Month in Chicago") \
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

# Filter accidents for Chicago
chicago_df = df.filter(col("City") == "Chicago")

# Extract month from Start_Time
chicago_df = chicago_df.withColumn("Month", month(col("Start_Time")))

# Calculate the count of accidents per month in Chicago
accidents_per_month = chicago_df.groupBy("City", "Month").agg(count("*").alias("Accident_Count"))

# Show the results
accidents_per_month.show()

# Save the results to MongoDB
accidents_per_month.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri) \
    .option("database", "accidents") \
    .option("collection", "accidents_per_month_chicago") \
    .save()



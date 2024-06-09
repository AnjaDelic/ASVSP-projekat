import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofweek, hour, count, avg
from pyspark.sql.window import Window

# Logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

input_uri = "mongodb://mongodb:27017/accidents.accidents_data"
output_uri = "mongodb://mongodb:27017/accidents.avg_day_hour_chicago"

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("Average Accidents per Day and Hour for Chicago ") \
    .master('local') \
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
chicago_accidents = df.filter(col("City") == "Chicago")

# Define Window specification
windowSpec = Window.partitionBy( "City",dayofweek("Start_Time"), hour("Start_Time"))

df_with_counts = chicago_accidents.withColumn("AccidentCount", count("ID").over(windowSpec))

avg_accidents_per_hour = df_with_counts \
    .groupBy("City", dayofweek("Start_Time").alias("Day"), hour("Start_Time").alias("Hour")) \
    .agg(avg("AccidentCount").alias("AvgAccidentsPerHour")) \
    .orderBy("City", "Day", "Hour")

avg_accidents_per_day = avg_accidents_per_hour \
    .groupBy("City", "Day") \
    .agg(avg("AvgAccidentsPerHour").alias("AvgAccidentsPerDay"))

final_result = avg_accidents_per_day.join(avg_accidents_per_hour, on=["City", "Day"], how="inner")

final_result.show()

# Save the results to MongoDB
final_result.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri) \
    .option("database", "accidents") \
    .option("collection", "avg_day_hour_chicago") \
    .save()

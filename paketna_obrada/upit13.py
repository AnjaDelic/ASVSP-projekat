#Za grad Čikago izlistati vremenske uslove i doba dana u kojima se najčešće dogadjaju nesreće.
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

input_uri = "mongodb://mongodb:27017/accidents.accidents_data"
output_uri = "mongodb://mongodb:27017/accidents.chicago_weather_conditions"

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


# Read the JSON files
location_df = spark.read.json(HDFS_NAMENODE + "/data/location_df.json")
weather_df = spark.read.json(HDFS_NAMENODE + "/data/weather_df.json")
accident_df = spark.read.json(HDFS_NAMENODE + "/data/accident_df.json")

# Filter accidents for Chicago
location_df2 = location_df.filter(col("City") == "Chicago")
chicago_df = weather_df.join(location_df2.select("ID", "City"), on="ID")
chicago_df_ac=chicago_df.join(accident_df.select("ID","Sunrise_Sunset"),on="ID")

# Define window specifications
windowSpec = Window.partitionBy("Weather_Condition", "Sunrise_Sunset")

# Calculate accident count for each weather condition and time of day
weather_counts = chicago_df_ac.withColumn("AccidentCount", F.count("ID").over(windowSpec))

# Get the top weather conditions and time of day with the highest number of accidents
top_weather_conditions = weather_counts.select("Weather_Condition", "Sunrise_Sunset", "AccidentCount") \
    .distinct() \
    .orderBy(F.col("AccidentCount").desc()) \
    .limit(15)

# Show the results
top_weather_conditions.show()

# Save the results to MongoDB
top_weather_conditions.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri) \
    .option("database", "accidents") \
    .option("collection", "chicago_weather_conditions") \
    .save()
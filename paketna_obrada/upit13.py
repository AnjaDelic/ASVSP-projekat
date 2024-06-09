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

# Read the JSON file
df = spark.read.json(HDFS_NAMENODE + "/data/US_Accidents_March23_cleaned.json")

# Filter accidents for Chicago
chicago_accidents = df.filter(col("City") == "Chicago")

# Define window specifications
windowSpec = Window.partitionBy("Weather_Condition", "Sunrise_Sunset")

# Calculate accident count for each weather condition and time of day
weather_counts = chicago_accidents.withColumn("AccidentCount", F.count("ID").over(windowSpec))

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
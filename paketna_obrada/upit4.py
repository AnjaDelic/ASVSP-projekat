import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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

# Read the JSON files
accident_df = spark.read.json(HDFS_NAMENODE + "/data/accident_df.json")
weather_df = spark.read.json(HDFS_NAMENODE + "/data/weather_df.json")

# Join the accident and weather data on the common field
joined_df = accident_df.join(weather_df, "ID")

# Filter accidents that occurred near traffic signs, had a traffic impact severity level of 2,
# occurred during the night, and with low visibility
near_traffic_sign_accidents = joined_df.filter(
    (col("Traffic_Signal") == True) & 
    (col("Severity") == 2) &
    (col("Sunrise_Sunset") == "Night") &
    (col("Visibility(km)") < 1.0)
)

# Show the results
near_traffic_sign_accidents.show()

# Save the results to MongoDB
near_traffic_sign_accidents.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri) \
    .option("database", "accidents") \
    .option("collection", "near_traffic_sign_accidents") \
    .save()

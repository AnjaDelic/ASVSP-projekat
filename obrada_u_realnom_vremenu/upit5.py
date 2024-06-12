from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import os

# Initialize Spark session
spark = SparkSession \
    .builder \
    .appName("AccidentsComparison") \
    .master('local') \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
    .getOrCreate()

# Function to reduce log verbosity
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

quiet_logs(spark)

# Schema for accidents data
schema = StructType([
    StructField("CRASH_RECORD_ID", StringType(), True),
    StructField("CRASH_DATE_EST_I", StringType(), True),
    StructField("CRASH_DATE", StringType(), True),
    StructField("POSTED_SPEED_LIMIT", IntegerType(), True),
    StructField("TRAFFIC_CONTROL_DEVICE", StringType(), True),
    StructField("DEVICE_CONDITION", StringType(), True),
    StructField("WEATHER_CONDITION", StringType(), True),
    StructField("LIGHTING_CONDITION", StringType(), True),
    StructField("FIRST_CRASH_TYPE", StringType(), True),
    StructField("TRAFFICWAY_TYPE", StringType(), True),
    StructField("LANE_CNT", IntegerType(), True),
    StructField("ALIGNMENT", StringType(), True),
    StructField("ROADWAY_SURFACE_COND", StringType(), True),
    StructField("ROAD_DEFECT", StringType(), True),
    StructField("REPORT_TYPE", StringType(), True),
    StructField("CRASH_TYPE", StringType(), True),
    StructField("INTERSECTION_RELATED_I", StringType(), True),
    StructField("NOT_RIGHT_OF_WAY_I", StringType(), True),
    StructField("HIT_AND_RUN_I", StringType(), True),
    StructField("DAMAGE", StringType(), True),
    StructField("DATE_POLICE_NOTIFIED", StringType(), True),
    StructField("PRIM_CONTRIBUTORY_CAUSE", StringType(), True),
    StructField("SEC_CONTRIBUTORY_CAUSE", StringType(), True),
    StructField("STREET_NO", IntegerType(), True),
    StructField("STREET_DIRECTION", StringType(), True),
    StructField("STREET_NAME", StringType(), True),
    StructField("BEAT_OF_OCCURRENCE", IntegerType(), True),
    StructField("PHOTOS_TAKEN_I", StringType(), True),
    StructField("STATEMENTS_TAKEN_I", StringType(), True),
    StructField("DOORING_I", StringType(), True),
    StructField("WORK_ZONE_I", StringType(), True),
    StructField("WORK_ZONE_TYPE", StringType(), True),
    StructField("WORKERS_PRESENT_I", StringType(), True),
    StructField("NUM_UNITS", IntegerType(), True),
    StructField("MOST_SEVERE_INJURY", StringType(), True),
    StructField("INJURIES_TOTAL", IntegerType(), True),
    StructField("INJURIES_FATAL", IntegerType(), True),
    StructField("INJURIES_INCAPACITATING", IntegerType(), True),
    StructField("INJURIES_NON_INCAPACITATING", IntegerType(), True),
    StructField("INJURIES_REPORTED_NOT_EVIDENT", IntegerType(), True),
    StructField("INJURIES_NO_INDICATION", IntegerType(), True),
    StructField("INJURIES_UNKNOWN", IntegerType(), True),
    StructField("CRASH_HOUR", IntegerType(), True),
    StructField("CRASH_DAY_OF_WEEK", IntegerType(), True),
    StructField("CRASH_MONTH", IntegerType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("LOCATION", StringType(), True)
])

# Read streaming data from Kafka
df_accidents_raw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092") \
    .option("subscribe", "accidents_data") \
    .load()

# Parse JSON data and add timestamp
df_accidents = df_accidents_raw \
    .withColumn("parsed_value", from_json(col("value").cast("string"), schema)) \
    .withColumn("timestamp_received", col("timestamp")) \
    .select("parsed_value.*", "timestamp_received") \
    .withColumn("CRASH_DATE", to_timestamp(col("CRASH_DATE"), "MM/dd/yyyy hh:mm:ss a"))

# Define HDFS Namenode
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# Read the JSON files
location_df = spark.read.json(HDFS_NAMENODE + "/data/location_df.json")
weather_df = spark.read.json(HDFS_NAMENODE + "/data/weather_df.json")
location_df_chicago = location_df.filter(col("City") == "Chicago")

joined_df = location_df_chicago.join(weather_df, "ID")

# Group by weather condition and count occurrences
weather_counts = joined_df.groupBy("Weather_Condition").count()

# Get the most frequent weather condition
most_frequent_weather = weather_counts.orderBy(col("count").desc()).first()["Weather_Condition"]

#print("Most frequent",most_frequent_weather)

# Convert CRASH_DATE to timestamp using the correct format
df_accidents = df_accidents.withColumn("CRASH_DATE", to_timestamp(col("CRASH_DATE"), "MM/dd/yyyy hh:mm:ss a"))

# Calculate the number of accidents in the last 15 minutes for each weather condition
window_duration = "15 minutes"
sliding_interval = "2 minutes"

df_recent_accidents = df_accidents \
    .withWatermark("CRASH_DATE", "3 minutes") \
    .groupBy(window("CRASH_DATE", window_duration, sliding_interval), "WEATHER_CONDITION") \
    .agg(count("*").alias("recent_accidents")) \
    .withColumn("window_start", col("window.start")) \
    .select("window_start","window.end", "recent_accidents","WEATHER_CONDITION", lit(most_frequent_weather).alias("Most common weather historical"))\
# Define a function to write each batch to HDFS and print to console
def write_to_hdfs(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.show(truncate=False)  # Print the batch to the console
        batch_df \
            .write \
            .format("json") \
            .mode("append") \
            .save(HDFS_NAMENODE + f"/data/upit5/batch_{batch_id}")

# Continue from where you left off, ensuring proper usage within streaming context

# Start the streaming query and await termination
query = df_recent_accidents \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_hdfs) \
    .option("checkpointLocation", HDFS_NAMENODE + "/tmp/upit5_checkpoint") \
    .start()

# Await termination of the streams
query.awaitTermination()

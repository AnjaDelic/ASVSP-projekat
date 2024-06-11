from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# Function to suppress logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

# Create SparkSession
spark = SparkSession \
    .builder \
    .appName("AccidentsAnalysis") \
    .master('local') \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
    .getOrCreate()

quiet_logs(spark)

# Define schema for accident data
schema = StructType([
    StructField("CRASH_RECORD_ID", StringType(), True),
    StructField("CRASH_DATE_EST_I", StringType(), True),
    StructField("CRASH_DATE", TimestampType(), True),
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
    .select("parsed_value.*", "timestamp_received")

# Define HDFS Namenode
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# Define window duration and sliding interval
window_duration = "3 seconds"
sliding_interval = "1 second"

# Aggregate data to count fatal and non-fatal accidents in the previous minute
# Aggregate data to count fatal and non-fatal accidents in the previous minute
df_windowed_aggregated = df_accidents \
    .withWatermark("timestamp_received", "3 seconds") \
    .groupBy(window("timestamp_received", window_duration, sliding_interval)) \
    .agg(
        sum(when(col("MOST_SEVERE_INJURY") == "FATAL", 1).otherwise(0)).alias("fatal_accidents"),
        sum(when(col("MOST_SEVERE_INJURY") != "FATAL", 1).otherwise(0)).alias("non_fatal_accidents"),
    )


# Write the aggregated data to console for testing
df_accidents_console = df_windowed_aggregated \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Write the aggregated data to HDFS
df_accidents_hdfs = df_windowed_aggregated \
    .writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", HDFS_NAMENODE + "/data/upit2") \
    .option("checkpointLocation", HDFS_NAMENODE + "/tmp/upit2_checkpoint") \
    .start()

# Await termination of the streams
spark.streams.awaitAnyTermination()

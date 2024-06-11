from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, TimestampType
import os
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaToHDFS") \
    .getOrCreate()

# Define schema for the data
schema = StructType() \
    .add("CRASH_RECORD_ID", StringType()) \
    .add("CRASH_DATE_EST_I", StringType()) \
    .add("CRASH_DATE", TimestampType()) \
    .add("POSTED_SPEED_LIMIT", StringType()) \
    .add("TRAFFIC_CONTROL_DEVICE", StringType()) \
    .add("DEVICE_CONDITION", StringType()) \
    .add("WEATHER_CONDITION", StringType()) \
    .add("LIGHTING_CONDITION", StringType()) \
    .add("FIRST_CRASH_TYPE", StringType()) \
    .add("TRAFFICWAY_TYPE", StringType()) \
    .add("LANE_CNT", StringType()) \
    .add("ALIGNMENT", StringType()) \
    .add("ROADWAY_SURFACE_COND", StringType()) \
    .add("ROAD_DEFECT", StringType()) \
    .add("REPORT_TYPE", StringType()) \
    .add("CRASH_TYPE", StringType()) \
    .add("INTERSECTION_RELATED_I", StringType()) \
    .add("NOT_RIGHT_OF_WAY_I", StringType()) \
    .add("HIT_AND_RUN_I", StringType()) \
    .add("DAMAGE", StringType()) \
    .add("DATE_POLICE_NOTIFIED", StringType()) \
    .add("PRIM_CONTRIBUTORY_CAUSE", StringType()) \
    .add("SEC_CONTRIBUTORY_CAUSE", StringType()) \
    .add("STREET_NO", StringType()) \
    .add("STREET_DIRECTION", StringType()) \
    .add("STREET_NAME", StringType()) \
    .add("BEAT_OF_OCCURRENCE", StringType()) \
    .add("PHOTOS_TAKEN_I", StringType()) \
    .add("STATEMENTS_TAKEN_I", StringType()) \
    .add("DOORING_I", StringType()) \
    .add("WORK_ZONE_I", StringType()) \
    .add("WORK_ZONE_TYPE", StringType()) \
    .add("WORKERS_PRESENT_I", StringType()) \
    .add("NUM_UNITS", StringType()) \
    .add("MOST_SEVERE_INJURY", StringType()) \
    .add("INJURIES_TOTAL", StringType()) \
    .add("INJURIES_FATAL", StringType()) \
    .add("INJURIES_INCAPACITATING", StringType()) \
    .add("INJURIES_NON_INCAPACITATING", StringType()) \
    .add("INJURIES_REPORTED_NOT_EVIDENT", StringType()) \
    .add("INJURIES_NO_INDICATION", StringType()) \
    .add("INJURIES_UNKNOWN", StringType()) \
    .add("CRASH_HOUR", StringType()) \
    .add("CRASH_DAY_OF_WEEK", StringType()) \
    .add("CRASH_MONTH", StringType()) \
    .add("LATITUDE", StringType()) \
    .add("LONGITUDE", StringType()) \
    .add("LOCATION", StringType())

# Read streaming data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092") \
    .option("subscribe", "accidents_data") \
    .load()

# Parse JSON data
df_parsed = df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]


# Write data to HDFS
query = df_parsed \
    .writeStream \
    .format("parquet") \
    .option("path", HDFS_NAMENODE + "/data/all_accidents") \
    .option("checkpointLocation", HDFS_NAMENODE + "/tmp/all_accidents") \
    .option("spark.sql.streaming.fileSink.log.cleanupDelay", "3600") \
    .option("spark.sql.streaming.fileSink.log.compaction.delay", "300") \
    .outputMode("append") \
    .start()


query.awaitTermination()

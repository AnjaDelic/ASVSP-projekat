#Prikazati državu sa najviše nezgoda koje su imale značajan uticaj na okolinu, a desile su se tokom dana i kada je brzina vetra bila veća od prosečne. 

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col,avg
# Logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

input_uri = "mongodb://mongodb:27017/accidents.accidents_data"
output_uri = "mongodb://mongodb:27017/accidents.high_impact_accidents_by_state"

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("High Impact Accidents by State") \
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

# Define a window specification partitioned by State
windowSpec = Window.partitionBy("State")

# Calculate the count of high impact accidents for each state
high_impact_accidents = df.filter(
    (col("Severity") >= 3) &
    (col("Sunrise_Sunset") == "Day") &
    (col("Wind_Speed(mph)") > (df.select(avg("Wind_Speed(mph)")).collect()[0][0]))
).withColumn("HighImpactAccidentCount", count("ID").over(windowSpec))

# Get the top state with the highest number of high impact accidents
top_state_high_impact_accidents = high_impact_accidents.select("State", "HighImpactAccidentCount") \
    .distinct() \
    .orderBy(col("HighImpactAccidentCount").desc()) \
    .limit(1)

# Show the results
top_state_high_impact_accidents.show()

# Save the results to MongoDB
top_state_high_impact_accidents.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri) \
    .option("database", "accidents") \
    .option("collection", "high_impact_accidents_by_state") \
    .save()

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, count, avg
from pyspark.sql.window import Window

# Logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

input_uri = "mongodb://mongodb:27017/accidents.accidents_data"
output_uri = "mongodb://mongodb:27017/average_accidents_per_month_chicago"


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

# Read the JSON files
location_df = spark.read.json(HDFS_NAMENODE + "/data/location_df.json")
weather_df = spark.read.json(HDFS_NAMENODE + "/data/weather_df.json")
accident_df = spark.read.json(HDFS_NAMENODE + "/data/accident_df.json")

# Filter accidents for Chicago
location_df2 = location_df.filter(col("City") == "Chicago")

# Join accident_df with location_df to get city information
chicago_df = accident_df.join(location_df2.select("ID", "City"), on="ID")

# Extract month from Start_Time
chicago_df = chicago_df.withColumn("Month", month(col("Start_Time")))

# Define a window specification partitioned by month
windowSpec = Window.partitionBy("Month")

# Calculate the count of accidents per month in Chicago
accidents_per_month = chicago_df.groupBy("City", "Month").agg(count("*").alias("Accident_Count"))
# Calculate the average accidents per month using window function
average_accidents_per_month = accidents_per_month.withColumn("Average_Accidents_Per_Month", avg("Accident_Count").over(windowSpec))

# Select only the columns you want to show
result = average_accidents_per_month.select("City", "Month", "Average_Accidents_Per_Month")

# Show the results
result.show()

# Save the results to MongoDB
result.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri) \
    .option("database", "accidents") \
    .option("collection", "average_accidents_per_month_chicago") \
    .save()


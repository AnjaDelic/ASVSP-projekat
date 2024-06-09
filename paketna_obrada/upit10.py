#Izlistati 5 gradova u kojima je najvise zabeleženih nezgoda blizu pešačkog prelaza i znaka stop. Za svaki od 5 gradova izlistati 3 najčešćih vremenskih uslova. 
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, row_number, collect_list, struct
from pyspark.sql.window import Window

# Logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

input_uri = "mongodb://mongodb:27017/accidents.accidents_data"
output_uri = "mongodb://mongodb:27017/top_5_cities_weather_conditions"

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("Cities with Most Accidents Near Crosswalk and Stop Sign") \
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

# Filter accidents that occurred near a crosswalk and stop sign
filtered_df = df.filter((col("Crossing") == True) & (col("Stop") == True))

# Group by city to count the number of accidents per city
city_accidents = filtered_df.groupBy("City").agg(count("ID").alias("Accident_Count"))

# Order by the number of accidents in descending order and select the top 5 cities
top_5_cities = city_accidents.orderBy(desc("Accident_Count")).limit(5)

# Join the filtered_df with top_5_cities to get weather conditions for these cities
joined_df = top_5_cities.join(filtered_df, on="City")

# Group by city and weather condition to count the occurrences
city_weather_counts = joined_df.groupBy("City", "Weather_Condition").agg(count("ID").alias("Weather_Count"))

# Define window specification to rank weather conditions within each city
windowSpec = Window.partitionBy("City").orderBy(desc("Weather_Count"))

# Use window function to rank the weather conditions and filter to get the top 3 for each city
ranked_weather_conditions = city_weather_counts.withColumn("Rank", row_number().over(windowSpec)) \
                                                .filter(col("Rank") <= 3)

# Collect the weather conditions into a list for each city
result = ranked_weather_conditions.groupBy("City") \
                                  .agg(collect_list(struct(col("Weather_Condition"), col("Weather_Count"))).alias("Top_3_Weather_Conditions"))

# Show the results
result.show(truncate=False)

# Save the results to MongoDB
result.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", output_uri) \
    .option("database", "accidents") \
    .option("collection", "top_5_cities_weather_conditions") \
    .save()



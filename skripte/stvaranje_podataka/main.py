#!/usr/bin/python3

# OBAVEZNA instalacija kafka-python i pandas
import os
import time
from kafka import KafkaProducer
import kafka.errors
import pandas as pd
from datetime import datetime

KAFKA_BROKER = "localhost:9092"
TOPIC = "accidents_data"

# Load the CSV file into pandas dataframe
df = pd.read_csv("chicago_crashes.csv")

# Initial delay to ensure Kafka is ready
# time.sleep(5)

while True:
    try:
        # Connect to Kafka
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
        for index, row in df.iterrows():
            # Modifying dates to current time for each message
            current_time = datetime.now()
            row['CRASH_DATE'] = current_time.strftime('%m/%d/%Y %I:%M:%S %p')
            row['DATE_POLICE_NOTIFIED'] = current_time.strftime('%m/%d/%Y %I:%M:%S %p')
            row['CRASH_DAY_OF_WEEK'] = current_time.strftime('%w')  # 0 (Sunday) - 6 (Saturday)
            row['CRASH_MONTH'] = current_time.strftime('%m')
            row['CRASH_HOUR'] = current_time.strftime('%H')

            print("Sending message to Kafka")
            # Send the message
            producer.send(TOPIC, value=row.to_json().encode())
            print(row.to_json())
            time.sleep(1)  # Adjust sleep time based on your needs
    except kafka.errors.NoBrokersAvailable as e:
        print("No Kafka brokers available, retrying in 3 seconds...")
        print(e)
        time.sleep(3)
    except Exception as e:
        print("An unexpected error occurred:", e)
        time.sleep(3)

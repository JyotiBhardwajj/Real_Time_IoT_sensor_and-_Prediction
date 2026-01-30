from pyspark.sql import SparkSession
import time
from elasticsearch import Elasticsearch

def log_message(message):
   print(f"{time.strftime('%H:%M:%S')} - {message}")

log_message("Starting connection tests...")

# 1. Create Spark Session
try:
   log_message("Creating Spark Session...")
   spark = SparkSession.builder \
       .appName("Connection Test") \
       .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
       .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
       .getOrCreate()
   log_message("Spark Session created successfully!")
except Exception as e:
   log_message(f"Spark Session creation error: {e}")

# 2. Test Kafka Connection
try:
   log_message("Testing Kafka connection...")
   log_message("Attempting to connect to test-topic...")
   df = spark \
       .readStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", "kafka:9092") \
       .option("subscribe", "test-topic") \
       .load()
   log_message("Kafka connection successful! Topic is accessible.")
   
   # Try starting a simple stream
   query = df.writeStream \
       .format("console") \
       .outputMode("append") \
       .start()
   
   log_message("Kafka stream started successfully! Waiting 5 seconds...")
   time.sleep(5)  # Monitor stream for 5 seconds

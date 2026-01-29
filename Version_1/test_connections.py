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

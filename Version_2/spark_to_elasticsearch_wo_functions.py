import sys
import os
import warnings
import traceback
import logging
import findspark
import time
from datetime import datetime
from colorama import Fore, Back, Style, init

# Initialize Colorama
init()
findspark.init("/opt/spark")

from elasticsearch import Elasticsearch, helpers
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

def print_banner():
    """Displays visual banner at application start"""
    banner = f"""
{Fore.CYAN}
    ╔═══════════════════════════════════════════════════╗
    ║       SPARK STREAMING TO ELASTICSEARCH            ║
    ║                Version 3.3                        ║
    ║ ----------------------------------------         ║
    ║  📥 Kafka → 🔄 Spark → 💾 Elasticsearch → 📊 Kibana ║
    ╚═══════════════════════════════════════════════════╝
{Style.RESET_ALL}"""
    print(banner)

def log_message(message, level="info"):
    """Enhanced logging messages"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    colors = {
        "info": Fore.GREEN,
        "warning": Fore.YELLOW,
        "error": Fore.RED,
        "highlight": Fore.BLUE,
        "success": Fore.CYAN,
        "stats": Fore.MAGENTA
    }
    icons = {
        "info": "ℹ️",
        "warning": "⚠️",
        "error": "❌",
        "highlight": "🔍",
        "success": "✅",
        "stats": "📊"
    }
    print(f"{colors.get(level, Fore.WHITE)}[{timestamp}] {icons.get(level, '')} {message}{Style.RESET_ALL}")

def print_batch_report(batch_id, count, duration):
    """Displays visual batch processing report"""
    report = f"""
{Fore.CYAN}╔════════════════ BATCH PERFORMANCE REPORT ════════════════╗
║ 🆔 Batch ID    : {batch_id:<37} ║
║ 📊 Process Summary:                                    ║
║   ├── 📝 Total Records   : {count:<25} ║
║   └── ⏱️  Process Time    : {duration:.2f} seconds{' ':<16} ║
╚═════════════════════════════════════════════════════════╝{Style.RESET_ALL}
"""
    print(report)
# Program initialization
print_banner()
log_message("Initializing Elasticsearch Stream system...", "highlight")
warnings.filterwarnings('ignore')
checkpointDir = "file:///tmp/streaming/kafka_office_input"

# Initialize Spark Session
try:
    log_message("🚀 Creating Spark Session...")
    spark = (SparkSession.builder
             .appName("Streaming Kafka-Spark")
             .master("local[2]")
             .config("spark.driver.memory", "2g")
             .config("spark.executor.memory", "2g")
             .config("spark.sql.shuffle.partitions", "10")
             .config("spark.default.parallelism", "10")
             .config("spark.network.timeout", "800s")
             .config("spark.executor.heartbeatInterval", "60s")
             .config("spark.storage.blockManagerSlaveTimeoutMs", "800s")
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
             .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
             .getOrCreate())

    spark.sparkContext.setLogLevel("ERROR")
    log_message("✅ Spark Session created successfully!", "success")
except Exception as e:
    log_message(f"❌ Spark Session error: {str(e)}", "error")
    traceback.print_exc()
    sys.exit(1)

# Kafka data read configuration
try:
    log_message("📥 Establishing Kafka connection...")
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "office-input") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()
    log_message("✅ Kafka connection successful!", "success")
except Exception as e:
    log_message(f"❌ Kafka connection error: {str(e)}", "error")
    sys.exit(1)

# Elasticsearch connection and index configuration
try:
    log_message("📡 Establishing Elasticsearch connection...")
    es = Elasticsearch(
        ["http://es:9200"],
        verify_certs=False,
        timeout=30,
        retry_on_timeout=True,
        max_retries=3
    )
    
    # Check and delete old index
    if es.indices.exists(index="office_input"):
        log_message("⚠️ Found old index, deleting...", "warning")
        es.indices.delete(index="office_input")
        time.sleep(2)

    # New index mapping
    index_mapping = {
        "mappings": {
            "properties": {
                "event_ts_min": {"type": "date"},
                "co2": {"type": "float"},
                "humidity": {"type": "float"},
                "light": {"type": "float"},
                "temperature": {"type": "float"},
                "room": {"type": "keyword"},
                "pir": {"type": "float"},
                "if_movement": {"type": "keyword"}
            }
        },
        "settings": {
            "number_of_shards": 2,
            "number_of_replicas": 0,
            "refresh_interval": "1s"
        }
    }

    es.indices.create(index="office_input", body=index_mapping)
    log_message("✅ Elasticsearch index created successfully!", "success")

except Exception as e:
    log_message(f"❌ Elasticsearch configuration error: {str(e)}", "error")
    sys.exit(1)
# Data transformations
log_message("🔄 Starting data processing pipeline...")

# Parse JSON data

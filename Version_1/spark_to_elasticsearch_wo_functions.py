import sys
import os
import warnings
import traceback
import logging
import findspark
import time
from datetime import datetime
from colorama import Fore, Back, Style, init

init()

findspark.init("/opt/spark")
from elasticsearch import Elasticsearch, helpers
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

def print_banner():
   banner = f"""
{Fore.CYAN}
   ╔═══════════════════════════════════════════════════╗
   ║       SPARK STREAMING TO ELASTICSEARCH            ║
   ║                Version 3.1                        ║
   ║ ----------------------------------------         ║
   ║  📥 Kafka → 🔄 Spark → 💾 Elasticsearch → 📊 Kibana ║
   ╚═══════════════════════════════════════════════════╝
{Style.RESET_ALL}"""
   print(banner)

def log_message(message, level="info"):
   timestamp = datetime.now().strftime("%H:%M:%S")
   colors = {
       "info": Fore.GREEN,
       "warning": Fore.YELLOW,
       "error": Fore.RED,
       "highlight": Fore.BLUE,
       "success": Fore.CYAN
   }
   print(f"{colors.get(level, Fore.WHITE)}[{timestamp}] {message}{Style.RESET_ALL}")

def print_batch_report(batch_id, count, duration, rate):
   report = f"""
{Fore.CYAN}╔════════════════ BATCH PERFORMANCE REPORT ════════════════╗
║ 🆔 Batch ID    : {batch_id:<37} ║
║ 📊 Process Summary:                                     ║
║   ├── 📝 Total Records : {count:<28} ║
║   └── ⏱️  Duration     : {duration:.2f} seconds{' ':<20} ║
╚═════════════════════════════════════════════════════════╝{Style.RESET_ALL}
"""
   print(report)

print_banner()
log_message("Starting system...", "highlight")
warnings.filterwarnings('ignore')
checkpointDir = "file:///tmp/streaming/kafka_office_input"

# Initialize Spark Session
try:
   log_message("🚀 Creating Spark Session...")
   spark = (SparkSession.builder
            .appName("Streaming Kafka-Spark")
            .master("local[2]")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")

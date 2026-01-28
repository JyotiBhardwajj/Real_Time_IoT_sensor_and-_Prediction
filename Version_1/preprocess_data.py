import os
import subprocess
from functools import reduce
import findspark
import time
from datetime import datetime
from tqdm import tqdm
from colorama import Fore, Back, Style, init

init()

findspark.init("/opt/spark")
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import *

def print_banner():
   """Print ASCII banner"""
   banner = f"""
{Fore.CYAN}
╔══════════════════════════════════════════════════════════════╗
║             SENSOR DATA PROCESSING SYSTEM                    ║
║                      Version 3.1                            ║
║----------------------------------------------------------  ║
║    🔄 Preprocessing  |  📊 Analysis  |  💾 CSV Export        ║
╚══════════════════════════════════════════════════════════════╝
{Style.RESET_ALL}"""
   print(banner)

def log_message(message, level="info", indent=0):
   """Enhanced colored log messages"""
   timestamp = datetime.now().strftime("%H:%M:%S")
   colors = {
       "info": Fore.GREEN,
       "warning": Fore.YELLOW,
       "error": Fore.RED + Back.WHITE,
       "highlight": Fore.BLUE,
       "success": Fore.CYAN,
       "processing": Fore.MAGENTA
   }
   
   icons = {
       "info": "ℹ️",
       "warning": "⚠️",
       "error": "❌",
       "highlight": "🔍",
       "success": "✅",
       "processing": "🔄"
   }
   
   indent_str = "  " * indent
   print(f"{colors.get(level, Fore.WHITE)}[{timestamp}] {icons.get(level, '')} {indent_str}{message}{Style.RESET_ALL}")

def format_table_output(df, num_rows=5):
   """Print DataFrame output in formatted table"""
   rows = df.limit(num_rows).collect()
   if not rows:
       return "No data found."
   
   # Define column widths
   col_widths = {
       'event_ts_min': 19,
       'ts_min_bignt': 12,
       'room': 6,
       'co2': 12,
       'light': 12,
       'temp': 12,
       'humidity': 12,
       'pir': 12
   }
   
   # Header row
   header = ""
   for col in df.columns:
       header += f"{col:<{col_widths[col]}} "
   
   # Data rows
   rows_str = []
   for row in rows:
       row_str = ""
       for col in df.columns:
           value = row[col]
           if isinstance(value, float):
               row_str += f"{value:>{col_widths[col]}.6f} "
           else:
               row_str += f"{str(value):<{col_widths[col]}} "
       rows_str.append(row_str)
   
   return header + "\n" + "\n".join(rows_str)

# Process start time
start_time = time.time()

# Print banner
print_banner()

log_message("🚀 Starting Spark Session...", "highlight")

# Initialize SparkSession with optimizations
spark_session = SparkSession.builder \
   .appName("Sensor Data Processing") \
   .master("local[3]") \
   .config("spark.driver.memory", "3g") \
   .config("spark.executor.memory", "3g") \
   .config("spark.sql.shuffle.partitions", 75) \
   .config("spark.default.parallelism", 75) \
   .config("spark.network.timeout", "800s") \
   .config("spark.executor.heartbeatInterval", "60s") \
   .config("spark.storage.blockManagerSlaveTimeoutMs", "800s") \
   .config("spark.sql.autoBroadcastJoinThreshold", -1) \
   .getOrCreate()

log_message("✨ Spark Session created successfully!", "success")

# Data structures and variables
room_data = {}
directory_path = '/opt/final_project/KETI'
dataframes_per_room = {}
sensor_columns = ['co2', 'humidity', 'light', 'pir', 'temperature']

# Checkpoint directory
spark_session.sparkContext.setCheckpointDir("/tmp/checkpoint")

# CSV reading schema
schema = StructType([
   StructField("ts_min_bignt", StringType(), True),
   StructField("sensor_value", StringType(), True)
])

log_message("\n📂 Starting data reading process...", "highlight")
log_message(f"└── Source Directory: {directory_path}", "info", indent=1)

# Get total folder count
total_folders = len([f for f in os.listdir(directory_path)])
log_message(f"📊 Total Folders to Process: {total_folders}", "highlight")

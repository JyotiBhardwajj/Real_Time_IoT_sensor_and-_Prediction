import sys
import os
import warnings
import shutil
import findspark
import time
from datetime import datetime
from colorama import Fore, Back, Style, init
from tqdm import tqdm
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.types import *
import pyspark.sql.functions as F

init()
findspark.init("/opt/spark")

def print_banner():
   """Print banner"""
   banner = f"""
{Fore.CYAN}
╔═══════════════════════════════════════════════════════╗
║             SENSOR MOTION ML MODEL                    ║
║                 Training v1.0                         ║
║ --------------------------------------------------- ║
║  📊 Data Prep | 🤖 Model Training | 📈 Performance      ║
╚═══════════════════════════════════════════════════════╝
{Style.RESET_ALL}"""
   print(banner)

def log_message(message, level="info", indent=0):
   """Colored log messages"""
   timestamp = datetime.now().strftime("%H:%M:%S")
   colors = {
       "info": Fore.GREEN,
       "warning": Fore.YELLOW,
       "error": Fore.RED + Back.WHITE,
       "highlight": Fore.BLUE,
       "success": Fore.CYAN
   }
   icons = {
       "info": "ℹ️",
       "warning": "⚠️",
       "error": "❌",
       "highlight": "🔍",
       "success": "✅"
   }
   indent_str = "  " * indent
   print(f"{colors.get(level, Fore.WHITE)}[{timestamp}] {icons.get(level, '')} {indent_str}{message}{Style.RESET_ALL}")

def setup_model_directory(model_path):
   """Prepare model directory"""
   try:
       if os.path.exists(model_path):
           log_message(f"Found existing model directory: {model_path}", "warning")
           shutil.rmtree(model_path)
           log_message("Cleaned old model", "success", indent=1)
       os.makedirs(model_path, exist_ok=True)
       log_message("Model directory prepared", "success")
       return True
   except Exception as e:
       log_message(f"Directory preparation error: {str(e)}", "error")
       return False

def print_performance_report(total, correct, accuracy):
   """Print model performance report"""
   report = f"""
{Fore.CYAN}╔════════════════ MODEL PERFORMANCE REPORT ════════════════╗
║ 📊 Model Evaluation:                                      ║
║   ├── 📝 Total Records : {total:<28} ║
║   ├── ✅ Correct Pred. : {correct:<28} ║
║   └── 🎯 Accuracy     : {accuracy:.2f}%{' ':<26} ║
╚═════════════════════════════════════════════════════════╝{Style.RESET_ALL}
"""
   print(report)

def main():
   print_banner()
   log_message("Starting ML model training...", "highlight")

   model_path = "/opt/spark/ml_model"
   if not setup_model_directory(model_path):
       sys.exit(1)

   try:
       spark = SparkSession.builder \
           .appName("ML Model Training") \
           .master("local[2]") \
           .config("spark.driver.memory", "4g") \
           .config("spark.executor.memory", "4g") \
           .getOrCreate()
       log_message("Spark Session created successfully!", "success")
   except Exception as e:
       log_message(f"Spark Session error: {str(e)}", "error")
       sys.exit(1)

   schema = StructType([
       StructField("event_ts_min", StringType(), True),
       StructField("ts_min_bignt", IntegerType(), True),
       StructField("room", StringType(), True),
       StructField("co2", FloatType(), True),

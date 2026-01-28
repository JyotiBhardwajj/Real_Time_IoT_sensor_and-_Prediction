import sys
import os
import warnings
import traceback
import logging
import findspark
import time
from datetime import datetime
from colorama import Fore, Back, Style, init
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler

init()
findspark.init("/opt/spark")

logging.getLogger("org.apache.spark").setLevel(logging.ERROR)
logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)

def print_banner():
   """Print colored banner"""
   banner = f"""
{Fore.CYAN}
╔═══════════════════════════════════════════════════════╗
║             ML STREAM PROCESSING                      ║
║                 Version 1.0                           ║
║ --------------------------------------------------- ║
║  📥 office-input → 🤖 ML → 📤 activity/no-activity     ║
╚═══════════════════════════════════════════════════════╝
{Style.RESET_ALL}"""
   print(banner)

def print_batch_stats(batch_id, movement_count, no_movement_count, duration):
   """Print batch processing statistics"""
   print(f"\n{Fore.CYAN}{'='*60}")
   stats = f"""
   📊 BATCH {batch_id} SUMMARY
   ├── 🏃 Movement Detected : {movement_count}
   ├── 🚫 No Movement      : {no_movement_count}
   └── ⏱️  Duration        : {duration:.2f} seconds
   """
   print(stats)
   print(f"{'='*60}{Style.RESET_ALL}\n")

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

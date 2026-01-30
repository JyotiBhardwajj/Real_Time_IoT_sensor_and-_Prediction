import os
import sys
import warnings
import logging
import findspark
import time
import pandas as pd
from datetime import datetime
from colorama import Fore, Back, Style, init
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

init()
findspark.init("/opt/spark")

logging.getLogger("org.apache.spark").setLevel(logging.ERROR)
logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)

def print_banner():
   banner = f"""
{Fore.CYAN}
╔══════════════════════════════════════════════════════════════╗
║             SENSOR DATA STREAMING SYSTEM                      ║
║                      Version 3.0                             ║
║----------------------------------------------------------  ║
║    🔄 Automatic Processing  |  📊 Real-time Data             ║
║    💾 Training Data        |  🤖 ML Pipeline                 ║
╚══════════════════════════════════════════════════════════════╝
{Style.RESET_ALL}"""
   print(banner)

def log_message(message, level="info"):
   timestamp = datetime.now().strftime("%H:%M:%S")
   colors = {
       "info": Fore.GREEN, "warning": Fore.YELLOW, "error": Fore.RED,
       "success": Fore.CYAN, "highlight": Fore.BLUE,
       "stats": Fore.MAGENTA, "ml": Fore.MAGENTA + Style.BRIGHT
   }
   icons = {
       "info": "ℹ️", "warning": "⚠️", "error": "❌", "success": "✅",
       "highlight": "🔍", "stats": "📊", "ml": "🤖"
   }
   print(f"{colors.get(level, Fore.WHITE)}[{timestamp}] {icons.get(level, '')} {message}{Style.RESET_ALL}")

def print_progress(room_name, step, details=""):
   colors = {
       "read": Fore.BLUE, "process": Fore.GREEN,
       "write": Fore.CYAN, "ml": Fore.MAGENTA
   }
   step_icons = {
       "read": "📂", "process": "🔄",
       "write": "💾", "ml": "🤖"
   }
   message = f"[Room {room_name}] {step_icons.get(step, '•')} {details}"
   print(f"{colors.get(step, Fore.WHITE)}{message}{Style.RESET_ALL}")

def save_to_csv(df, is_first_room=False):
   try:
       # Convert DataFrame to Pandas
       pandas_df = df.toPandas()
       
       # Save to CSV
       mode = 'w' if is_first_room else 'a'
       header = is_first_room
       pandas_df.to_csv('/opt/data-generator/input/sensors.csv', 
                       mode=mode, header=header, index=False)
       return True
   except Exception as e:
       log_message(f"CSV save error: {str(e)}", "error")
       return False

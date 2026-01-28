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

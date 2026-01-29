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

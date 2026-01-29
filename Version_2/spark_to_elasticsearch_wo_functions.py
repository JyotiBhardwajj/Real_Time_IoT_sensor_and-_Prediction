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

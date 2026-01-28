import pandas as pd
from kafka import KafkaProducer
import time
import argparse
from tqdm import tqdm
from colorama import Fore, Back, Style, init
from datetime import datetime

init()

def print_banner():
    """Kafka Producer Banner"""
    banner = f"""
{Fore.CYAN}
╔════════════════════════════════════════════════════════════╗
║             SENSOR DATA KAFKA PRODUCER                     ║
║                     Version 2.0                            ║
║ -------------------------------------------------------- ║
║    📤 CSV Read  |  🔄 Kafka Streaming  |  📊 Monitoring     ║
╚════════════════════════════════════════════════════════════╝
{Style.RESET_ALL}"""
    print(banner)

def log_message(message, level="info"):
    """Colored log messages"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    colors = {
        "info": Fore.GREEN,
        "warning": Fore.YELLOW,
        "error": Fore.RED,
        "success": Fore.CYAN
    }
    icons = {
        "info": "ℹ️",
        "warning": "⚠️",
        "error": "❌",
        "success": "✅"
    }
    print(f"{colors.get(level, Fore.WHITE)}[{timestamp}] {icons.get(level, '')} {message}{Style.RESET_ALL}")

class DataFrameToKafka:
    def __init__(self, input, sep, kafka_sep, row_sleep_time, source_file_extension, bootstrap_servers,
                 topic, repeat, shuffle, key_index, excluded_cols):
        log_message("Starting Producer...", "info")
        self.input = input
        self.sep = sep
        self.kafka_sep = kafka_sep
        self.row_sleep_time = row_sleep_time
        self.repeat = repeat
        self.shuffle = shuffle

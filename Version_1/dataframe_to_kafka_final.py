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
        self.excluded_cols = excluded_cols
        self.df = self.read_source_file(source_file_extension)
        self.topic = topic
        self.key_index = key_index
        
        # Configuration summary
        log_message("Configuration parameters:", "info")
        print(f"{Fore.CYAN}├── 📁 Input: {self.input}")
        print(f"├── 📋 Topic: {self.topic}")
        print(f"├── ⏱️ Sleep Time: {self.row_sleep_time}")
        print(f"├── 🔄 Repeat: {self.repeat}")
        print(f"└── 🔌 Bootstrap Servers: {bootstrap_servers}{Style.RESET_ALL}")
        
        try:
            self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            log_message("Kafka connection successful!", "success")
        except Exception as e:
            log_message(f"Kafka connection error: {str(e)}", "error")
            raise

    def turn_df_to_str(self, df):
        """Convert DataFrame to string format"""
        x = df.values.astype(str)
        vals = [self.kafka_sep.join(ele) for ele in x]
        return vals

    def read_source_file(self, extension='csv'):
        """Read source file"""
        log_message(f"Reading data file: {self.input}", "info")
        try:
            if extension == 'csv':
                df = pd.read_csv(self.input, sep=self.sep, low_memory=False)
                if self.shuffle:
                    df = df.sample(frac=1)
            else:
                df = pd.read_parquet(self.input, 'auto')
                if self.shuffle:
                    df = df.sample(frac=1)
            
            df = df.dropna()
            columns_to_write = [x for x in df.columns if x not in self.excluded_cols]
            log_message(f"Total columns: {len(columns_to_write)}", "info")
            df = df[columns_to_write]
            df['value'] = self.turn_df_to_str(df)
            return df
            
        except Exception as e:
            log_message(f"File reading error: {str(e)}", "error")
            raise

    def df_to_kafka(self):
        """Send data to Kafka"""
        counter = 0
        df_size = len(self.df) * self.repeat
        total_time = self.row_sleep_time * df_size
        start_time = time.time()
        
        log_message("Starting data stream...", "info")
        print(f"{Fore.CYAN}Total records to send: {df_size:,}{Style.RESET_ALL}")
        
        for _ in range(self.repeat):
            with tqdm(total=len(self.df), desc="📤 Sending Data", 
                     bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as pbar:
                for index, row in self.df.iterrows():
                    try:
                        if self.key_index == 1000:
                            self.producer.send(self.topic, 
                                             key=str(index).encode(), 
                                             value=row[-1].encode())
                        else:

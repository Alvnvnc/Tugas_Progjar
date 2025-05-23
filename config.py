"""
Configuration and utilities for stress testing
"""
import os
import random
import string
import socket
import time
import base64
import json
import logging
import sys
import threading
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import csv
from datetime import datetime
import statistics

# Initial basic configuration
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def setup_logging(log_level=None, log_file=None, verbose=False):
    """
    Setup logging configuration with customizable log level and optional file output
    
    Args:
        log_level: The logging level (default: INFO or DEBUG if verbose=True)
        log_file: Optional file path to write logs to
        verbose: If True, sets log_level to DEBUG
    """
    # Set log level based on verbose flag if log_level not explicitly provided
    if log_level is None:
        log_level = logging.DEBUG if verbose else logging.INFO
    
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear existing handlers to avoid duplicate logs
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create console handler with formatting
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Add file handler if log_file is provided
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    return root_logger

# Server configuration
SERVER_HOST = "172.16.16.101"
SERVER_PORT = 6665

# Test configurations
FILE_SIZES_MB = [10, 50, 100]  # Test file sizes in MB
CLIENT_WORKER_COUNTS = [1, 5, 50]
SERVER_WORKER_COUNTS = [1, 5, 50]  # This is for documentation, actual server config is separate
OPERATIONS = ['upload', 'download', 'list', 'upload_download']  # Separate operations + combined

# Timeout settings
SOCKET_TIMEOUT = 300  # 5 minutes for large files
CONNECT_TIMEOUT = 30
CHUNK_SIZE = 512 * 1024  # 512KB chunks

# Global statistics
stats_lock = threading.Lock()
global_stats = {
    'successful_operations': 0,
    'failed_operations': 0,
    'total_bytes_processed': 0,
    'operation_times': []
}


class StressTestResult:
    """Class for storing stress test results"""
    def __init__(self):
        self.test_number = 0
        self.operation = ""
        self.file_size_mb = 0
        self.client_workers = 0
        self.server_workers = 0
        self.total_time_per_client = 0.0
        self.throughput_per_client = 0.0
        self.successful_clients = 0
        self.failed_clients = 0
        self.successful_servers = 0
        self.failed_servers = 0
        self.detailed_times = []
        self.error_messages = []
        # Add upload-specific and download-specific metrics
        self.upload_time_per_client = 0.0
        self.upload_throughput_per_client = 0.0
        self.download_time_per_client = 0.0
        self.download_throughput_per_client = 0.0


def is_server_running(host=None, port=None):
    """Check if server is running"""
    if host is None:
        host = SERVER_HOST
    if port is None:
        port = SERVER_PORT
        
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((host, port))
        return True
    except Exception as e:
        logger.debug(f"Server check failed: {str(e)}")
        return False


def generate_test_file(file_path, size_mb):
    """Generate a test file of specified size"""
    logger.info(f"Generating {size_mb}MB test file: {file_path}")
    
    bytes_size = size_mb * 1024 * 1024
    
    # Choose chunk size based on file size for efficiency
    if size_mb <= 10:
        chunk_size = 10 * 1024 * 1024  # 10MB chunks 
    elif size_mb <= 50:
        chunk_size = 50 * 1024 * 1024  # 50MB chunks
    else:
        chunk_size = 100 * 1024 * 1024  # 100MB chunks
    
    # Cap chunk size by file size
    chunk_size = min(chunk_size, bytes_size)
    
    with open(file_path, 'wb') as f:
        remaining_bytes = bytes_size
        while remaining_bytes > 0:
            current_chunk = min(chunk_size, remaining_bytes)
            random_data = bytes(random.getrandbits(8) for _ in range(current_chunk))
            f.write(random_data)
            remaining_bytes -= current_chunk
    
    return file_path


def safe_mean(values):
    """Safely calculate mean, return 0 if empty list"""
    return statistics.mean(values) if values else 0.0


def update_stats(success, bytes_processed=0, elapsed_time=None):
    """Update global statistics in a thread-safe manner"""
    with stats_lock:
        if success:
            global_stats['successful_operations'] += 1
            if bytes_processed > 0:
                global_stats['total_bytes_processed'] += bytes_processed
        else:
            global_stats['failed_operations'] += 1
            
        if elapsed_time is not None:
            global_stats['operation_times'].append(elapsed_time)


def reset_stats():
    """Reset global statistics"""
    with stats_lock:
        global_stats['successful_operations'] = 0
        global_stats['failed_operations'] = 0
        global_stats['total_bytes_processed'] = 0
        global_stats['operation_times'] = []

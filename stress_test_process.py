"""
Stress test program using multiprocessing to test the process pool server.
This program will run multiple client processes to test the server's performance.
"""
import os
import sys
import logging
import argparse
import time
from datetime import datetime
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import csv
import json
import base64
import socket
from typing import Dict, List, Tuple

# Add parent directory to path to make imports work when running directly
if __name__ == "__main__":
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Server configuration
SERVER_HOST = "172.16.16.101"
SERVER_PORT = 6665

# Test configurations
FILE_SIZES_MB = [10, 50, 100]  # Test file sizes in MB
CLIENT_WORKER_COUNTS = [1, 5, 50]
SERVER_WORKER_COUNTS = [1, 5, 50]
OPERATIONS = ['upload', 'download']

def setup_logging(log_level=None, log_file=None, verbose=False):
    """Setup logging configuration"""
    if log_level is None:
        log_level = logging.DEBUG if verbose else logging.INFO
    
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Add file handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    return root_logger

def generate_test_file(size_mb: int, filename: str) -> str:
    """Generate a test file of specified size"""
    bytes_size = size_mb * 1024 * 1024
    with open(filename, 'wb') as f:
        f.write(os.urandom(bytes_size))
    return filename

def upload_file(worker_id: int, file_path: str, server_host: str, server_port: int, base_filename: str) -> Dict:
    """Upload a file to the server"""
    start_time = time.time()
    result = {
        'worker_id': worker_id,
        'success': False,
        'bytes_processed': 0,
        'elapsed_time': 0,
        'error': None
    }
    
    try:
        # Read file content
        with open(file_path, 'rb') as f:
            content = f.read()
        
        # Encode content to base64
        content_b64 = base64.b64encode(content).decode('utf-8')
        
        # Create upload command
        command = f"UPLOAD {base_filename}_{worker_id} {content_b64}\n"
        
        # Connect to server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((server_host, server_port))
            s.sendall(command.encode())
            
            # Receive response
            response = b""
            while True:
                chunk = s.recv(4096)
                if not chunk:
                    break
                response += chunk
            
            # Parse response
            try:
                response_json = json.loads(response.decode())
                if response_json['status'] == 'OK':
                    result['success'] = True
                    result['bytes_processed'] = len(content)
            except Exception as e:
                result['error'] = f"Error parsing response: {str(e)}"
    
    except Exception as e:
        result['error'] = str(e)
    
    finally:
        result['elapsed_time'] = time.time() - start_time
    
    return result

def download_file(worker_id: int, server_host: str, server_port: int, base_filename: str) -> Dict:
    """Download a file from the server"""
    start_time = time.time()
    result = {
        'worker_id': worker_id,
        'success': False,
        'bytes_processed': 0,
        'elapsed_time': 0,
        'error': None
    }
    
    try:
        # Create download command
        command = f"GET {base_filename}_{worker_id}\n"
        
        # Connect to server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((server_host, server_port))
            s.sendall(command.encode())
            
            # Receive response
            response = b""
            while True:
                chunk = s.recv(4096)
                if not chunk:
                    break
                response += chunk
            
            # Parse response
            try:
                response_json = json.loads(response.decode())
                if response_json['status'] == 'OK':
                    # Decode base64 content
                    content = base64.b64decode(response_json['data'])
                    result['success'] = True
                    result['bytes_processed'] = len(content)
            except Exception as e:
                result['error'] = f"Error parsing response: {str(e)}"
    
    except Exception as e:
        result['error'] = str(e)
    
    finally:
        result['elapsed_time'] = time.time() - start_time
    
    return result

def run_test_scenario(test_number: int, operation: str, file_size_mb: int, 
                     client_workers: int, server_workers: int) -> Dict:
    """Run a single test scenario"""
    logger = logging.getLogger()
    logger.info(f"Running test {test_number}: {operation} - {file_size_mb}MB - {client_workers} clients")
    
    # Generate test file
    test_file = f"test_file_{file_size_mb}mb.dat"
    base_filename = f"stress_test_{file_size_mb}mb"
    
    if operation == 'upload':
        generate_test_file(file_size_mb, test_file)
    
    start_time = time.time()
    results = []
    
    try:
        with ProcessPoolExecutor(max_workers=client_workers) as executor:
            if operation == 'upload':
                futures = [
                    executor.submit(upload_file, i, test_file, SERVER_HOST, SERVER_PORT, base_filename)
                    for i in range(client_workers)
                ]
            else:  # download
                futures = [
                    executor.submit(download_file, i, SERVER_HOST, SERVER_PORT, base_filename)
                    for i in range(client_workers)
                ]
            
            results = [future.result() for future in futures]
    
    except Exception as e:
        logger.error(f"Error in test scenario: {str(e)}")
    
    finally:
        # Clean up test file
        if operation == 'upload' and os.path.exists(test_file):
            os.remove(test_file)
    
    # Calculate statistics
    successful_workers = sum(1 for r in results if r['success'])
    failed_workers = len(results) - successful_workers
    total_bytes = sum(r['bytes_processed'] for r in results if r['success'])
    total_time = time.time() - start_time
    
    # Server workers are considered successful if all client operations succeeded
    successful_servers = server_workers if successful_workers == client_workers else 0
    failed_servers = server_workers - successful_servers
    
    return {
        'test_number': test_number,
        'operation': operation,
        'file_size_mb': file_size_mb,
        'client_workers': client_workers,
        'server_workers': server_workers,
        'successful_clients': successful_workers,
        'failed_clients': failed_workers,
        'successful_servers': successful_servers,
        'failed_servers': failed_servers,
        'total_bytes': total_bytes,
        'total_time': total_time,
        'throughput': total_bytes / total_time if total_time > 0 else 0,
        'results': results
    }

def save_results_to_csv(results: List[Dict], output_dir: str = "."):
    """Save test results to CSV file"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(output_dir, f"stress_test_results_{timestamp}.csv")
    
    headers = [
        "Test Number",
        "Operation",
        "File Size (MB)",
        "Client Workers",
        "Server Workers",
        "Total Time (s)",
        "Throughput (MB/s)",
        "Successful Clients",
        "Failed Clients",
        "Successful Servers",
        "Failed Servers"
    ]
    
    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        
        for r in results:
            row = [
                r['test_number'],
                r['operation'],
                r['file_size_mb'],
                r['client_workers'],
                r['server_workers'],
                f"{r['total_time']:.2f}",
                f"{r['throughput'] / (1024*1024):.2f}",  # Convert to MB/s
                r['successful_clients'],
                r['failed_clients'],
                r['successful_servers'],
                r['failed_servers']
            ]
            writer.writerow(row)
    
    return csv_path

def main():
    """Main entry point for the stress testing application"""
    parser = argparse.ArgumentParser(description='Process Pool Server Stress Testing Tool')
    
    parser.add_argument('-o', '--operations', nargs='+', 
                        choices=['upload', 'download'],
                        default=['upload', 'download'],
                        help='Operations to test')
    
    parser.add_argument('-f', '--file-sizes', type=int, nargs='+', 
                        default=FILE_SIZES_MB,
                        help='File sizes in MB')
    
    parser.add_argument('-c', '--client-workers', type=int, nargs='+', 
                        default=CLIENT_WORKER_COUNTS,
                        help='Number of client workers')
    
    parser.add_argument('-s', '--server-workers', type=int, nargs='+', 
                        default=SERVER_WORKER_COUNTS,
                        help='Number of server workers')
    
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Enable verbose logging')
    
    parser.add_argument('-l', '--log-file', type=str, default='',
                        help='Log to file instead of console')
    
    parser.add_argument('-r', '--report-dir', type=str, default='.',
                        help='Directory to store report files')
    
    args = parser.parse_args()
    
    # Setup logging
    if args.log_file:
        log_file = args.log_file
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"stress_test_{timestamp}.log"
    
    logger = setup_logging(log_file=log_file, verbose=args.verbose)
    
    # Ensure report directory exists
    if not os.path.exists(args.report_dir):
        os.makedirs(args.report_dir)
    
    # Run all test combinations
    test_number = 1
    all_results = []
    
    for operation in args.operations:
        for file_size in args.file_sizes:
            for client_workers in args.client_workers:
                for server_workers in args.server_workers:
                    result = run_test_scenario(
                        test_number, operation, file_size, 
                        client_workers, server_workers
                    )
                    all_results.append(result)
                    test_number += 1
    
    # Save results to CSV
    csv_path = save_results_to_csv(all_results, args.report_dir)
    logger.info(f"Results saved to: {csv_path}")
    
    # Print summary
    logger.info("\nTest Summary:")
    for result in all_results:
        logger.info(
            f"Test {result['test_number']}: {result['operation']} - "
            f"{result['file_size_mb']}MB - {result['client_workers']} clients/{result['server_workers']} servers - "
            f"Client Success: {result['successful_clients']}/{result['client_workers']} - "
            f"Server Success: {result['successful_servers']}/{result['server_workers']} - "
            f"Throughput: {result['throughput'] / (1024*1024):.2f} MB/s"
        )

if __name__ == "__main__":
    main() 
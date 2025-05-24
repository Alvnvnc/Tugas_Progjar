"""
Test runner module for stress testing client-server file operations.
Contains functions for running individual tests and comprehensive test suites.
"""
import os
import sys
import logging

# Add parent directory to path to make imports work when running directly
if __name__ == "__main__":
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Use absolute imports
from config import StressTestResult, is_server_running, SERVER_HOST, SERVER_PORT, generate_test_file, safe_mean
from file_operations import (
    upload_file_worker, download_file_worker, list_files_worker,
    upload_file_worker_process, download_file_worker_process, list_files_worker_process,
    cleanup_test_files
)
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import threading
import time
import multiprocessing

# Global statistics with lock
stats_lock = threading.Lock()
global_stats = {
    'successful_operations': 0,
    'failed_operations': 0,
    'total_bytes_processed': 0,
    'operation_times': []
}

def run_stress_test_scenario(test_number, operation, file_size_mb, client_workers, server_workers, use_threading=True):
    """Run a single stress test scenario"""
    logger = logging.getLogger()
    logger.info(f"=== Test {test_number}: {operation.upper()} - {file_size_mb}MB - {client_workers} clients ===")
    
    # Reset global stats
    global global_stats
    with stats_lock:
        global_stats = {
            'successful_operations': 0,
            'failed_operations': 0,
            'total_bytes_processed': 0,
            'operation_times': []
        }
    
    result = StressTestResult()
    result.test_number = test_number
    result.operation = operation
    result.file_size_mb = file_size_mb
    result.client_workers = client_workers
    result.server_workers = server_workers
    
    # Check if server is running
    if not is_server_running(SERVER_HOST, SERVER_PORT):
        logger.error(f"Server not running at {SERVER_HOST}:{SERVER_PORT}")
        result.failed_clients = client_workers
        return result
    
    # Generate test file for upload tests
    test_file_path = f"test_file_{file_size_mb}mb.dat"
    base_filename = f"stress_test_{file_size_mb}mb"
    
    # Generate test file for all operations since we're doing upload first
    generate_test_file(test_file_path, file_size_mb)
    
    start_time = time.time()
    
    try:
        # Handle different operations based on type
        if operation == 'upload':
            # ======= UPLOAD ONLY OPERATION =======
            logger.info(f"Starting UPLOAD-ONLY test {test_number}")
            upload_start_time = time.time()
            
            # Reset stats for upload measurement
            with stats_lock:
                global_stats = {
                    'successful_operations': 0,
                    'failed_operations': 0,
                    'total_bytes_processed': 0,
                    'operation_times': []
                }
            
            # Perform uploads
            if use_threading:
                with ThreadPoolExecutor(max_workers=client_workers) as executor:
                    futures = []
                    for i in range(client_workers):
                        futures.append(executor.submit(
                            upload_file_worker, i, test_file_path, SERVER_HOST, SERVER_PORT, base_filename
                        ))
                    upload_worker_results = [future.result() for future in as_completed(futures)]
            else:
                with ProcessPoolExecutor(max_workers=client_workers) as executor:
                    args_list = [(i, test_file_path, SERVER_HOST, SERVER_PORT, base_filename) for i in range(client_workers)]
                    upload_worker_results = list(executor.map(upload_file_worker_process, args_list))
            
            upload_end_time = time.time()
            upload_total_time = upload_end_time - upload_start_time
            
            # Process upload results
            upload_successful_workers = sum(1 for r in upload_worker_results if r['success'])
            upload_failed_workers = len(upload_worker_results) - upload_successful_workers
            
            # Calculate upload statistics
            if upload_worker_results:
                upload_times = [r['elapsed_time'] for r in upload_worker_results if r['success']]
                upload_avg_time = safe_mean(upload_times) if upload_times else 0.0
                upload_bytes = sum(r['bytes_processed'] for r in upload_worker_results if r['success'])
                upload_throughput = upload_bytes / upload_avg_time if upload_avg_time > 0 else 0.0
            else:
                upload_avg_time = 0.0
                upload_bytes = 0.0
                upload_throughput = 0.0
            
            # Set results for final report
            result.upload_time_per_client = upload_avg_time
            result.upload_throughput_per_client = upload_throughput
            result.total_time_per_client = upload_avg_time
            result.throughput_per_client = upload_throughput
            result.successful_clients = upload_successful_workers
            result.failed_clients = upload_failed_workers
            # Set server statistics based on client success
            result.successful_servers = upload_successful_workers
            result.failed_servers = upload_failed_workers
            
            for r in upload_worker_results:
                if r.get('error'):
                    result.error_messages.append(f"Upload worker {r['worker_id']}: {r['error']}")
        
        elif operation == 'download':
            # ======= DOWNLOAD ONLY OPERATION =======
            logger.info(f"Starting DOWNLOAD-ONLY test {test_number}")
            
            # First, need to upload files for each client worker to download later
            logger.info(f"Preparing files on server for download test")
            upload_start_time = time.time()
            
            # Upload files first
            with ThreadPoolExecutor(max_workers=min(10, client_workers)) as executor:  # Limit upload concurrency for preparation
                futures = []
                for i in range(client_workers):
                    futures.append(executor.submit(
                        upload_file_worker, i, test_file_path, SERVER_HOST, SERVER_PORT, base_filename
                    ))
                upload_results = [future.result() for future in as_completed(futures)]
            
            upload_successful_workers = sum(1 for r in upload_results if r['success'])
            logger.info(f"Preparation phase: {upload_successful_workers}/{client_workers} files uploaded successfully")
            
            # Reset stats for download measurement
            with stats_lock:
                global_stats = {
                    'successful_operations': 0,
                    'failed_operations': 0,
                    'total_bytes_processed': 0,
                    'operation_times': []
                }
            
            # Start download test
            download_start_time = time.time()
            
            # Perform downloads
            if use_threading:
                with ThreadPoolExecutor(max_workers=client_workers) as executor:
                    futures = []
                    for i in range(client_workers):
                        futures.append(executor.submit(
                            download_file_worker, i, SERVER_HOST, SERVER_PORT, base_filename
                        ))
                    download_worker_results = [future.result() for future in as_completed(futures)]
            else:
                with ProcessPoolExecutor(max_workers=client_workers) as executor:
                    args_list = [(i, SERVER_HOST, SERVER_PORT, base_filename) for i in range(client_workers)]
                    download_worker_results = list(executor.map(download_file_worker_process, args_list))
            
            download_end_time = time.time()
            download_total_time = download_end_time - download_start_time
            
            # Process download results
            download_successful_workers = sum(1 for r in download_worker_results if r['success'])
            download_failed_workers = client_workers - download_successful_workers
            
            # Calculate download statistics
            if download_worker_results:
                download_times = [r['elapsed_time'] for r in download_worker_results if r['success']]
                download_avg_time = safe_mean(download_times) if download_times else 0.0
                download_bytes = sum(r['bytes_processed'] for r in download_worker_results if r['success'])
                download_throughput = download_bytes / download_avg_time if download_avg_time > 0 else 0.0
            else:
                download_avg_time = 0.0
                download_bytes = 0.0
                download_throughput = 0.0
            
            # Set results
            result.download_time_per_client = download_avg_time
            result.download_throughput_per_client = download_throughput
            result.total_time_per_client = download_avg_time
            result.throughput_per_client = download_throughput
            result.successful_clients = download_successful_workers
            result.failed_clients = download_failed_workers
            # Set server statistics based on client success
            result.successful_servers = download_successful_workers
            result.failed_servers = download_failed_workers
            
            for r in download_worker_results:
                if r.get('error'):
                    result.error_messages.append(f"Download worker {r['worker_id']}: {r['error']}")
            
            # Clean up the uploaded files
            logger.info("Cleaning up files uploaded for download test")
            worker_ids = [r['worker_id'] for r in upload_results if r['success']]
            cleanup_test_files(SERVER_HOST, SERVER_PORT, base_filename, worker_ids)
            
        elif operation == 'list':
            # ======= LIST ONLY OPERATION =======
            logger.info(f"Starting LIST-ONLY test {test_number}")
            
            # Prepare by uploading a few test files first
            logger.info(f"Preparing some files on server for list test")
            num_prep_files = min(5, client_workers)  # Just need a few files for listing
            
            with ThreadPoolExecutor(max_workers=num_prep_files) as executor:
                futures = []
                for i in range(num_prep_files):
                    futures.append(executor.submit(
                        upload_file_worker, i, test_file_path, SERVER_HOST, SERVER_PORT, base_filename
                    ))
                upload_results = [future.result() for future in as_completed(futures)]
            
            upload_successful = sum(1 for r in upload_results if r['success'])
            logger.info(f"Preparation: {upload_successful}/{num_prep_files} files uploaded")
            
            # Reset stats for list measurement
            with stats_lock:
                global_stats = {
                    'successful_operations': 0,
                    'failed_operations': 0,
                    'total_bytes_processed': 0,
                    'operation_times': []
                }
            
            # Start list test
            list_start_time = time.time()
            
            # Perform list operations
            if use_threading:
                with ThreadPoolExecutor(max_workers=client_workers) as executor:
                    futures = []
                    for i in range(client_workers):
                        futures.append(executor.submit(
                            list_files_worker, i, SERVER_HOST, SERVER_PORT
                        ))
                    list_worker_results = [future.result() for future in as_completed(futures)]
            else:
                with ProcessPoolExecutor(max_workers=client_workers) as executor:
                    args_list = [(i, SERVER_HOST, SERVER_PORT) for i in range(client_workers)]
                    list_worker_results = list(executor.map(list_files_worker_process, args_list))
            
            list_end_time = time.time()
            list_total_time = list_end_time - list_start_time
            
            # Process list results
            list_successful_workers = sum(1 for r in list_worker_results if r['success'])
            list_failed_workers = client_workers - list_successful_workers
            
            # Calculate list statistics
            list_times = [r['elapsed_time'] for r in list_worker_results if r['success']]
            list_avg_time = safe_mean(list_times) if list_times else 0.0
            
            # Set results
            result.total_time_per_client = list_avg_time
            # No throughput for listing as it's not a data transfer operation
            result.successful_clients = list_successful_workers
            result.failed_clients = list_failed_workers
            # Set server statistics based on client success
            result.successful_servers = list_successful_workers
            result.failed_servers = list_failed_workers
            
            for r in list_worker_results:
                if r.get('error'):
                    result.error_messages.append(f"List worker {r['worker_id']}: {r['error']}")
            
            # Clean up the uploaded files
            logger.info("Cleaning up files uploaded for list test")
            worker_ids = [r['worker_id'] for r in upload_results if r['success']]
            cleanup_test_files(SERVER_HOST, SERVER_PORT, base_filename, worker_ids)
            
        elif operation == 'upload_download':
            # ======= COMBINED UPLOAD + DOWNLOAD OPERATION =======
            logger.info(f"Starting UPLOAD+DOWNLOAD test {test_number}")
            
            # First phase: Upload
            logger.info(f"Phase 1: Upload test {test_number}")
            upload_start_time = time.time()
            
            # Reset stats for upload measurement
            with stats_lock:
                global_stats = {
                    'successful_operations': 0,
                    'failed_operations': 0,
                    'total_bytes_processed': 0,
                    'operation_times': []
                }
            
            # Perform uploads
            if use_threading:
                with ThreadPoolExecutor(max_workers=client_workers) as executor:
                    futures = []
                    for i in range(client_workers):
                        futures.append(executor.submit(
                            upload_file_worker, i, test_file_path, SERVER_HOST, SERVER_PORT, base_filename
                        ))
                    upload_worker_results = [future.result() for future in as_completed(futures)]
            else:
                with ProcessPoolExecutor(max_workers=client_workers) as executor:
                    args_list = [(i, test_file_path, SERVER_HOST, SERVER_PORT, base_filename) for i in range(client_workers)]
                    upload_worker_results = list(executor.map(upload_file_worker_process, args_list))
            
            upload_end_time = time.time()
            upload_total_time = upload_end_time - upload_start_time
            
            # Process upload results
            upload_successful_workers = sum(1 for r in upload_worker_results if r['success'])
            upload_failed_workers = client_workers - upload_successful_workers
            
            # Second phase: Download
            logger.info(f"Phase 2: Download test {test_number}")
            
            # Reset stats for download measurement
            with stats_lock:
                global_stats = {
                    'successful_operations': 0,
                    'failed_operations': 0,
                    'total_bytes_processed': 0,
                    'operation_times': []
                }
            
            # Only download files that were successfully uploaded
            successful_worker_ids = [r['worker_id'] for r in upload_worker_results if r['success']]
            download_start_time = time.time()
            download_worker_results = []
            
            if successful_worker_ids:
                # Perform downloads only for successfully uploaded files
                if use_threading:
                    with ThreadPoolExecutor(max_workers=len(successful_worker_ids)) as executor:
                        futures = []
                        for worker_id in successful_worker_ids:
                            futures.append(executor.submit(
                                download_file_worker, worker_id, SERVER_HOST, SERVER_PORT, base_filename
                            ))
                        download_worker_results = [future.result() for future in as_completed(futures)]
                else:
                    with ProcessPoolExecutor(max_workers=len(successful_worker_ids)) as executor:
                        args_list = [(worker_id, SERVER_HOST, SERVER_PORT, base_filename) for worker_id in successful_worker_ids]
                        download_worker_results = list(executor.map(download_file_worker_process, args_list))
            
            download_end_time = time.time()
            download_total_time = download_end_time - download_start_time
            
            # Process download results
            download_successful_workers = sum(1 for r in download_worker_results if r['success'])
            download_failed_workers = len(successful_worker_ids) - download_successful_workers
            
            # Calculate combined statistics
            # Upload statistics
            if upload_worker_results:
                upload_times = [r['elapsed_time'] for r in upload_worker_results if r['success']]
                upload_avg_time = safe_mean(upload_times) if upload_times else 0.0
                upload_bytes = sum(r['bytes_processed'] for r in upload_worker_results if r['success'])
                upload_throughput = upload_bytes / upload_avg_time if upload_avg_time > 0 else 0.0
            else:
                upload_avg_time = 0.0
                upload_throughput = 0.0
            
            # Download statistics
            if download_worker_results:
                download_times = [r['elapsed_time'] for r in download_worker_results if r['success']]
                download_avg_time = safe_mean(download_times) if download_times else 0.0
                download_bytes = sum(r['bytes_processed'] for r in download_worker_results if r['success'])
                download_throughput = download_bytes / download_avg_time if download_avg_time > 0 else 0.0
            else:
                download_avg_time = 0.0
                download_throughput = 0.0
            
            # Combined statistics
            total_avg_time = upload_avg_time + download_avg_time
            total_bytes = upload_bytes + download_bytes if 'upload_bytes' in locals() and 'download_bytes' in locals() else 0
            total_throughput = total_bytes / total_avg_time if total_avg_time > 0 else 0.0
            
            # Set results
            result.upload_time_per_client = upload_avg_time
            result.upload_throughput_per_client = upload_throughput
            result.download_time_per_client = download_avg_time
            result.download_throughput_per_client = download_throughput
            result.total_time_per_client = total_avg_time
            result.throughput_per_client = total_throughput
            result.successful_clients = min(upload_successful_workers, download_successful_workers)
            result.failed_clients = client_workers - result.successful_clients
            
            # Collect error messages
            for r in upload_worker_results:
                if r.get('error'):
                    result.error_messages.append(f"Upload worker {r['worker_id']}: {r['error']}")
            for r in download_worker_results:
                if r.get('error'):
                    result.error_messages.append(f"Download worker {r['worker_id']}: {r['error']}")
        
        end_time = time.time()
        total_test_time = end_time - start_time
        
        logger.info(f"Test {test_number} completed in {total_test_time:.2f}s: {result.successful_clients}/{client_workers} successful")
        
        # Only perform cleanup if we've completed both upload and download phases
        if operation == 'upload_download':
            # Clean up the uploaded files
            logger.info("Cleaning up files after testing")
            worker_ids = [r['worker_id'] for r in upload_worker_results if r['success']]
            cleanup_test_files(SERVER_HOST, SERVER_PORT, base_filename, worker_ids)
        
    except Exception as e:
        logger.error(f"Test {test_number} failed: {str(e)}")
        result.failed_clients = client_workers
        result.error_messages = [str(e)]
    
    finally:
        # Move file cleanup here, but keep the rest of cleanup code
        if os.path.exists(test_file_path):
            try:
                os.remove(test_file_path)
                logger.info(f"Removed local test file: {test_file_path}")
            except Exception as e:
                logger.warning(f"Could not remove test file {test_file_path}: {e}")
    
    return result

def run_comprehensive_stress_test(operations, file_sizes_mb, client_worker_counts, server_worker_counts):
    """Run all combinations of stress tests"""
    logger = logging.getLogger()
    logger.info("Starting comprehensive stress test...")
    
    # Calculate total number of tests
    total_tests = len(operations) * len(file_sizes_mb) * len(client_worker_counts) * len(server_worker_counts)
    logger.info(f"Total test combinations: {total_tests}")
    
    results = []
    test_number = 1
    
    # Run all combinations
    for operation in operations:
        for file_size_mb in file_sizes_mb:
            for client_workers in client_worker_counts:
                for server_workers in server_worker_counts:
                    logger.info(f"Running test {test_number}/{total_tests}")
                    result = run_stress_test_scenario(
                        test_number, operation, file_size_mb, client_workers, server_workers
                    )
                    results.append(result)
                    test_number += 1
    
    return results

def run_stress_test_scenario_process(test_number, operation, file_size_mb, client_workers, server_workers, 
                                   max_workers=None, chunk_size_kb=64, timeout=300, server_port=8889,
                                   connect_timeout=30, retry_delay=5, max_retries=3):
    """Run a single stress test scenario using multiprocessing"""
    logger = logging.getLogger()
    logger.info(f"=== Process Test {test_number}: {operation.upper()} - {file_size_mb}MB - {client_workers} clients ===")
    
    # Set reasonable defaults for multiprocessing
    if max_workers is None:
        max_workers = min(multiprocessing.cpu_count(), client_workers)
    
    # Update SERVER_PORT for this test
    global SERVER_PORT
    original_port = SERVER_PORT
    SERVER_PORT = server_port
    
    try:
        result = StressTestResult()
        result.test_number = test_number
        result.operation = operation
        result.file_size_mb = file_size_mb
        result.client_workers = client_workers
        result.server_workers = server_workers
        
        # Check if server is running
        if not is_server_running(SERVER_HOST, SERVER_PORT):
            logger.error(f"Process pool server not running at {SERVER_HOST}:{SERVER_PORT}")
            result.failed_clients = client_workers
            return result
        
        # Generate test file for upload tests
        test_file_path = f"test_file_{file_size_mb}mb_process.dat"
        base_filename = f"stress_test_process_{file_size_mb}mb"
        
        # Generate test file for all operations
        generate_test_file(test_file_path, file_size_mb)
        
        start_time = time.time()
        
        # Handle different operations based on type
        if operation == 'upload':
            # ======= UPLOAD ONLY OPERATION (MULTIPROCESSING) =======
            logger.info(f"Starting UPLOAD-ONLY process test {test_number}")
            upload_start_time = time.time()
            
            # Perform uploads using ProcessPoolExecutor
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                args_list = [(i, test_file_path, SERVER_HOST, SERVER_PORT, base_filename, chunk_size_kb, timeout) for i in range(client_workers)]
                upload_worker_results = list(executor.map(upload_file_worker_process, args_list))
            
            upload_end_time = time.time()
            upload_total_time = upload_end_time - upload_start_time
            
            # Process upload results
            upload_successful_workers = sum(1 for r in upload_worker_results if r and r.get('success', False))
            upload_failed_workers = len(upload_worker_results) - upload_successful_workers
            
            # Calculate upload statistics
            if upload_worker_results:
                upload_times = [r['elapsed_time'] for r in upload_worker_results if r and r.get('success', False)]
                upload_avg_time = safe_mean(upload_times) if upload_times else 0.0
                upload_bytes = sum(r.get('bytes_processed', 0) for r in upload_worker_results if r and r.get('success', False))
                upload_throughput = upload_bytes / upload_avg_time if upload_avg_time > 0 else 0.0
            else:
                upload_avg_time = 0.0
                upload_bytes = 0.0
                upload_throughput = 0.0
            
            # Set results for final report
            result.upload_time_per_client = upload_avg_time
            result.upload_throughput_per_client = upload_throughput
            result.total_time_per_client = upload_avg_time
            result.throughput_per_client = upload_throughput
            result.successful_clients = upload_successful_workers
            result.failed_clients = upload_failed_workers
            result.successful_servers = upload_successful_workers
            result.failed_servers = upload_failed_workers
            
            for i, r in enumerate(upload_worker_results):
                if r and r.get('error'):
                    result.error_messages.append(f"Upload process {i}: {r['error']}")
        
        elif operation == 'download':
            # ======= DOWNLOAD ONLY OPERATION (MULTIPROCESSING) =======
            logger.info(f"Starting DOWNLOAD-ONLY process test {test_number}")
            
            # First, need to upload files for each client worker to download later
            logger.info(f"Preparing files on server for download test")
            
            # Upload files first using limited concurrency
            prep_workers = min(10, client_workers)
            with ProcessPoolExecutor(max_workers=prep_workers) as executor:
                args_list = [(i, test_file_path, SERVER_HOST, SERVER_PORT, base_filename, chunk_size_kb, timeout) for i in range(client_workers)]
                upload_results = list(executor.map(upload_file_worker_process, args_list))
            
            upload_successful_workers = sum(1 for r in upload_results if r and r.get('success', False))
            logger.info(f"Preparation phase: {upload_successful_workers}/{client_workers} files uploaded successfully")
            
            # Start download test
            download_start_time = time.time()
            
            # Perform downloads using ProcessPoolExecutor
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                args_list = [(i, SERVER_HOST, SERVER_PORT, base_filename, chunk_size_kb, timeout) for i in range(client_workers)]
                download_worker_results = list(executor.map(download_file_worker_process, args_list))
            
            download_end_time = time.time()
            download_total_time = download_end_time - download_start_time
            
            # Process download results
            download_successful_workers = sum(1 for r in download_worker_results if r and r.get('success', False))
            download_failed_workers = client_workers - download_successful_workers
            
            # Calculate download statistics
            if download_worker_results:
                download_times = [r['elapsed_time'] for r in download_worker_results if r and r.get('success', False)]
                download_avg_time = safe_mean(download_times) if download_times else 0.0
                download_bytes = sum(r.get('bytes_processed', 0) for r in download_worker_results if r and r.get('success', False))
                download_throughput = download_bytes / download_avg_time if download_avg_time > 0 else 0.0
            else:
                download_avg_time = 0.0
                download_bytes = 0.0
                download_throughput = 0.0
            
            # Set results
            result.download_time_per_client = download_avg_time
            result.download_throughput_per_client = download_throughput
            result.total_time_per_client = download_avg_time
            result.throughput_per_client = download_throughput
            result.successful_clients = download_successful_workers
            result.failed_clients = download_failed_workers
            result.successful_servers = download_successful_workers
            result.failed_servers = download_failed_workers
            
            for i, r in enumerate(download_worker_results):
                if r and r.get('error'):
                    result.error_messages.append(f"Download process {i}: {r['error']}")
        
        elif operation == 'list':
            # ======= LIST ONLY OPERATION (MULTIPROCESSING) =======
            logger.info(f"Starting LIST-ONLY process test {test_number}")
            list_start_time = time.time()
            
            # Perform list operations using ProcessPoolExecutor
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                args_list = [(i, SERVER_HOST, SERVER_PORT, timeout) for i in range(client_workers)]
                list_worker_results = list(executor.map(list_files_worker_process, args_list))
            
            list_end_time = time.time()
            list_total_time = list_end_time - list_start_time
            
            # Process list results
            list_successful_workers = sum(1 for r in list_worker_results if r and r.get('success', False))
            list_failed_workers = client_workers - list_successful_workers
            
            # Calculate list statistics
            if list_worker_results:
                list_times = [r['elapsed_time'] for r in list_worker_results if r and r.get('success', False)]
                list_avg_time = safe_mean(list_times) if list_times else 0.0
            else:
                list_avg_time = 0.0
            
            # Set results
            result.total_time_per_client = list_avg_time
            result.throughput_per_client = 1.0 / list_avg_time if list_avg_time > 0 else 0.0  # Operations per second
            result.successful_clients = list_successful_workers
            result.failed_clients = list_failed_workers
            result.successful_servers = list_successful_workers
            result.failed_servers = list_failed_workers
            
            for i, r in enumerate(list_worker_results):
                if r and r.get('error'):
                    result.error_messages.append(f"List process {i}: {r['error']}")
        
        elif operation == 'upload_download':
            # ======= COMBINED UPLOAD-DOWNLOAD OPERATION (MULTIPROCESSING) =======
            logger.info(f"Starting UPLOAD-DOWNLOAD process test {test_number}")
            
            # Upload phase
            upload_start_time = time.time()
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                args_list = [(i, test_file_path, SERVER_HOST, SERVER_PORT, base_filename, chunk_size_kb, timeout) for i in range(client_workers)]
                upload_worker_results = list(executor.map(upload_file_worker_process, args_list))
            upload_end_time = time.time()
            
            # Download phase
            download_start_time = time.time()
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                args_list = [(i, SERVER_HOST, SERVER_PORT, base_filename, chunk_size_kb, timeout) for i in range(client_workers)]
                download_worker_results = list(executor.map(download_file_worker_process, args_list))
            download_end_time = time.time()
            
            # Process combined results
            upload_successful = sum(1 for r in upload_worker_results if r and r.get('success', False))
            download_successful = sum(1 for r in download_worker_results if r and r.get('success', False))
            
            combined_successful = min(upload_successful, download_successful)
            combined_failed = client_workers - combined_successful
            
            upload_times = [r['elapsed_time'] for r in upload_worker_results if r and r.get('success', False)]
            download_times = [r['elapsed_time'] for r in download_worker_results if r and r.get('success', False)]
            
            upload_avg_time = safe_mean(upload_times) if upload_times else 0.0
            download_avg_time = safe_mean(download_times) if download_times else 0.0
            combined_avg_time = upload_avg_time + download_avg_time
            
            upload_bytes = sum(r.get('bytes_processed', 0) for r in upload_worker_results if r and r.get('success', False))
            download_bytes = sum(r.get('bytes_processed', 0) for r in download_worker_results if r and r.get('success', False))
            total_bytes = upload_bytes + download_bytes
            
            combined_throughput = total_bytes / combined_avg_time if combined_avg_time > 0 else 0.0
            
            # Set results
            result.upload_time_per_client = upload_avg_time
            result.download_time_per_client = download_avg_time
            result.total_time_per_client = combined_avg_time
            result.throughput_per_client = combined_throughput
            result.successful_clients = combined_successful
            result.failed_clients = combined_failed
            
            # Collect error messages
            for i, r in enumerate(upload_worker_results):
                if r and r.get('error'):
                    result.error_messages.append(f"Upload process {i}: {r['error']}")
            for i, r in enumerate(download_worker_results):
                if r and r.get('error'):
                    result.error_messages.append(f"Download process {i}: {r['error']}")
        
        end_time = time.time()
        total_time = end_time - start_time
        
        logger.info(f"Process test {test_number} completed in {total_time:.2f}s")
        logger.info(f"Success rate: {result.successful_clients}/{result.client_workers} clients")
        
        # Cleanup
        try:
            worker_ids = list(range(client_workers))
            cleanup_test_files(SERVER_HOST, SERVER_PORT, base_filename, worker_ids)
        except Exception as e:
            logger.warning(f"Cleanup warning: {str(e)}")
        
        return result
        
    finally:
        # Restore original port
        SERVER_PORT = original_port

def run_comprehensive_stress_test_process(operations, file_sizes_mb, client_worker_counts, server_worker_counts,
                                        max_workers=None, chunk_size_kb=64, timeout=300, server_port=8889,
                                        connect_timeout=30, retry_delay=5, max_retries=3):
    """Run comprehensive stress tests using multiprocessing for all combinations"""
    logger = logging.getLogger()
    
    if max_workers is None:
        max_workers = min(multiprocessing.cpu_count(), 8)
    
    logger.info(f"Starting comprehensive multiprocessing stress test")
    logger.info(f"Max workers per test: {max_workers}")
    logger.info(f"Chunk size: {chunk_size_kb}KB")
    logger.info(f"Timeout: {timeout}s")
    logger.info(f"Connect timeout: {connect_timeout}s")
    logger.info(f"Retry delay: {retry_delay}s")
    logger.info(f"Max retries: {max_retries}")
    
    results = []
    test_number = 1
    
    total_combinations = len(operations) * len(file_sizes_mb) * len(client_worker_counts) * len(server_worker_counts)
    logger.info(f"Total test combinations: {total_combinations}")
    
    for operation in operations:
        for file_size_mb in file_sizes_mb:
            for client_workers in client_worker_counts:
                for server_workers in server_worker_counts:
                    logger.info(f"Running test {test_number}/{total_combinations}: {operation} - {file_size_mb}MB - {client_workers}C/{server_workers}S")
                    
                    result = run_stress_test_scenario_process(
                        test_number, operation, file_size_mb, client_workers, server_workers,
                        max_workers=min(max_workers, client_workers), chunk_size_kb=chunk_size_kb,
                        timeout=timeout, server_port=server_port,
                        connect_timeout=connect_timeout,
                        retry_delay=retry_delay,
                        max_retries=max_retries
                    )
                    
                    results.append(result)
                    test_number += 1
                    
                    # Brief pause between tests to let server stabilize
                    time.sleep(2)
    
    logger.info(f"Comprehensive multiprocessing stress test completed: {len(results)} tests")
    return results

def upload_worker_process(worker_id, filename, file_data, server_host, server_port, 
                         chunk_size_kb, timeout, connect_timeout, retry_delay, max_retries,
                         results_queue):
    """
    Worker process for uploading files using base64 encoding and UPLOAD command
    """
    import socket
    import time
    import base64
    import logging
    
    # Setup logging for this worker process
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
    logger = logging.getLogger()
    
    start_time = time.time()
    success = False
    error_message = ""
    bytes_sent = 0
    
    # Convert file data to base64
    try:
        if isinstance(file_data, str):
            file_data = file_data.encode('utf-8')
        base64_data = base64.b64encode(file_data).decode('ascii')
        logger.info(f"Upload worker {worker_id}: Encoded {len(file_data)} bytes to {len(base64_data)} base64 chars")
    except Exception as e:
        error_message = f"Base64 encoding failed: {e}"
        logger.error(f"Upload worker {worker_id}: {error_message}")
        results_queue.put((worker_id, success, 0, time.time() - start_time, error_message))
        return
    
    logger.info(f"Upload worker {worker_id}: Sending command for file: {filename}")
    
    for attempt in range(max_retries):
        sock = None
        try:
            logger.info(f"Upload worker {worker_id}: Attempt {attempt + 1}/{max_retries} - Connecting to {server_host}:{server_port}")
            
            # Create and configure socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(connect_timeout)
            
            # Connect to server
            sock.connect((server_host, server_port))
            logger.info(f"Upload worker {worker_id}: Connected successfully")
            
            # Send UPLOAD command with filename and size
            command = f"UPLOAD {filename} {len(file_data)}\n"
            sock.send(command.encode('utf-8'))
            logger.debug(f"Upload worker {worker_id}: Sent command: {command.strip()}")
            
            # Wait for server acknowledgment (optional, some servers might not send one)
            sock.settimeout(5)  # Short timeout for ack
            try:
                ack = sock.recv(1024)
                logger.debug(f"Upload worker {worker_id}: Server ack: {ack}")
            except socket.timeout:
                logger.debug(f"Upload worker {worker_id}: No server ack (continuing)")
            except Exception as e:
                logger.debug(f"Upload worker {worker_id}: Ack error: {e} (continuing)")
            
            # Set timeout for data transfer
            sock.settimeout(timeout)
            
            # Send base64 data in chunks
            chunk_size = chunk_size_kb * 1024  # Convert to bytes for base64 data
            total_base64_size = len(base64_data)
            bytes_sent_this_attempt = 0
            
            logger.info(f"Upload worker {worker_id}: Starting to send {total_base64_size} base64 chars in chunks of {chunk_size}")
            
            # Send data in chunks
            for i in range(0, total_base64_size, chunk_size):
                chunk = base64_data[i:i + chunk_size]
                
                try:
                    sent = sock.send(chunk.encode('utf-8'))
                    bytes_sent_this_attempt += sent
                    logger.debug(f"Upload worker {worker_id}: Sent {bytes_sent_this_attempt}/{total_base64_size} base64 chars")
                    
                    # Small delay between chunks to prevent overwhelming the server
                    if i + chunk_size < total_base64_size:
                        time.sleep(0.01)  # 10ms delay
                        
                except Exception as e:
                    logger.error(f"Upload worker {worker_id}: Error sending chunk: {e}")
                    raise
            
            # Send end marker
            end_marker = b'\r\n\r\n'
            sock.send(end_marker)
            logger.debug(f"Upload worker {worker_id}: Sent end marker")
            
            # Wait for server response
            sock.settimeout(30)  # 30 seconds for server to process
            try:
                response = sock.recv(8192)
                logger.debug(f"Upload worker {worker_id}: Server response: {response}")
                
                # Check if response indicates success
                if b'"status": "OK"' in response or b'success' in response.lower():
                    success = True
                    bytes_sent = len(file_data)  # Original file size
                    logger.info(f"Upload worker {worker_id}: Upload successful!")
                else:
                    error_message = f"Server rejected upload: {response.decode('utf-8', errors='ignore')}"
                    logger.warning(f"Upload worker {worker_id}: {error_message}")
                    
            except socket.timeout:
                error_message = "Timeout waiting for server response"
                logger.error(f"Upload worker {worker_id}: {error_message}")
            except Exception as e:
                error_message = f"Error receiving server response: {e}"
                logger.error(f"Upload worker {worker_id}: {error_message}")
            
            # If we got here without exception, break out of retry loop
            if success:
                break
                
        except socket.timeout:
            error_message = f"Connection timeout (attempt {attempt + 1}/{max_retries})"
            logger.warning(f"Upload worker {worker_id}: {error_message}")
        except ConnectionRefusedError:
            error_message = f"Connection refused (attempt {attempt + 1}/{max_retries})"
            logger.warning(f"Upload worker {worker_id}: {error_message}")
        except (BrokenPipeError, ConnectionResetError) as e:
            error_message = f"Connection error (attempt {attempt + 1}/{max_retries}): {e}"
            logger.warning(f"Upload worker {worker_id}: {error_message}")
        except Exception as e:
            error_message = f"Unexpected error (attempt {attempt + 1}/{max_retries}): {e}"
            logger.error(f"Upload worker {worker_id}: {error_message}")
        finally:
            if sock:
                try:
                    sock.close()
                    logger.debug(f"Upload worker {worker_id}: Socket closed")
                except:
                    pass
        
        # Wait before retry (except for last attempt)
        if attempt < max_retries - 1:
            retry_wait = retry_delay * (2 ** attempt)  # Exponential backoff
            logger.info(f"Upload worker {worker_id}: Waiting {retry_wait} seconds before retry")
            time.sleep(retry_wait)
    
    total_time = time.time() - start_time
    
    if not success:
        logger.error(f"Upload worker {worker_id}: All {max_retries} attempts failed. Final error: {error_message}")
    
    # Put result in queue
    results_queue.put((worker_id, success, bytes_sent, total_time, error_message))

def download_worker_process(worker_id, filename, expected_size, server_host, server_port,
                           timeout, connect_timeout, retry_delay, max_retries, results_queue):
    """
    Worker process for downloading files and decoding base64 response
    """
    import socket
    import time
    import base64
    import json
    import logging
    
    # Setup logging for this worker process
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
    logger = logging.getLogger()
    
    start_time = time.time()
    success = False
    error_message = ""
    bytes_received = 0
    
    logger.info(f"Download worker {worker_id}: Downloading file: {filename}")
    
    for attempt in range(max_retries):
        sock = None
        try:
            logger.info(f"Download worker {worker_id}: Attempt {attempt + 1}/{max_retries} - Connecting to {server_host}:{server_port}")
            
            # Create and configure socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(connect_timeout)
            
            # Connect to server
            sock.connect((server_host, server_port))
            logger.info(f"Download worker {worker_id}: Connected successfully")
            
            # Send GET command
            command = f"GET {filename}\n"
            sock.send(command.encode('utf-8'))
            logger.debug(f"Download worker {worker_id}: Sent command: {command.strip()}")
            
            # Set timeout for data transfer
            sock.settimeout(timeout)
            
            # Receive response
            response_data = b""
            while True:
                chunk = sock.recv(8192)
                if not chunk:
                    break
                response_data += chunk
                if b'\r\n\r\n' in response_data:
                    break
            
            logger.debug(f"Download worker {worker_id}: Received {len(response_data)} bytes")
            
            # Parse response
            try:
                response_text = response_data.decode('utf-8')
                
                # Try to parse JSON response
                if response_text.strip().startswith('{'):
                    response_json = json.loads(response_text.strip().split('\r\n\r\n')[0])
                    
                    if response_json.get('status') == 'OK' and 'data' in response_json:
                        # Decode base64 data
                        base64_data = response_json['data']
                        decoded_data = base64.b64decode(base64_data)
                        bytes_received = len(decoded_data)
                        
                        # Verify size if expected
                        if expected_size > 0 and bytes_received != expected_size:
                            error_message = f"Size mismatch: expected {expected_size}, got {bytes_received}"
                            logger.warning(f"Download worker {worker_id}: {error_message}")
                        else:
                            success = True
                            logger.info(f"Download worker {worker_id}: Download successful! Received {bytes_received} bytes")
                    else:
                        error_message = f"Server error: {response_json.get('data', 'Unknown error')}"
                        logger.error(f"Download worker {worker_id}: {error_message}")
                else:
                    error_message = f"Invalid server response: {response_text[:200]}"
                    logger.error(f"Download worker {worker_id}: {error_message}")
                    
            except json.JSONDecodeError as e:
                error_message = f"JSON decode error: {e}"
                logger.error(f"Download worker {worker_id}: {error_message}")
            except Exception as e:
                error_message = f"Response parsing error: {e}"
                logger.error(f"Download worker {worker_id}: {error_message}")
            
            # If we got here without exception, break out of retry loop
            if success:
                break
                
        except socket.timeout:
            error_message = f"Connection timeout (attempt {attempt + 1}/{max_retries})"
            logger.warning(f"Download worker {worker_id}: {error_message}")
        except ConnectionRefusedError:
            error_message = f"Connection refused (attempt {attempt + 1}/{max_retries})"
            logger.warning(f"Download worker {worker_id}: {error_message}")
        except (BrokenPipeError, ConnectionResetError) as e:
            error_message = f"Connection error (attempt {attempt + 1}/{max_retries}): {e}"
            logger.warning(f"Download worker {worker_id}: {error_message}")
        except Exception as e:
            error_message = f"Unexpected error (attempt {attempt + 1}/{max_retries}): {e}"
            logger.error(f"Download worker {worker_id}: {error_message}")
        finally:
            if sock:
                try:
                    sock.close()
                    logger.debug(f"Download worker {worker_id}: Socket closed")
                except:
                    pass
        
        # Wait before retry (except for last attempt)
        if attempt < max_retries - 1:
            retry_wait = retry_delay * (2 ** attempt)  # Exponential backoff
            logger.info(f"Download worker {worker_id}: Waiting {retry_wait} seconds before retry")
            time.sleep(retry_wait)
    
    total_time = time.time() - start_time
    
    if not success:
        logger.error(f"Download worker {worker_id}: All {max_retries} attempts failed. Final error: {error_message}")
    
    # Put result in queue
    results_queue.put((worker_id, success, bytes_received, total_time, error_message))

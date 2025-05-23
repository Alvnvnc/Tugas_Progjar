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
from stress_test.config import StressTestResult, is_server_running, SERVER_HOST, SERVER_PORT, generate_test_file, safe_mean
from stress_test.file_operations import (
    upload_file_worker, download_file_worker, list_files_worker,
    upload_file_worker_process, download_file_worker_process, list_files_worker_process,
    cleanup_test_files
)
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import threading
import time

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

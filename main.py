"""
Main entry point for the stress testing application.
This module coordinates the execution of stress tests and reporting of results.
"""
import os
import sys
import logging
import argparse
import time
from datetime import datetime

# Add parent directory to path to make imports work when running directly
if __name__ == "__main__":
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Use absolute imports
from stress_test.config import setup_logging, is_server_running, SERVER_HOST, SERVER_PORT
from stress_test.test_runner import run_stress_test_scenario, run_comprehensive_stress_test
from stress_test.reporting import prepare_test_report, save_results_to_csv

def main():
    """Main entry point for the stress testing application"""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='File Transfer Protocol Stress Testing Tool')
    
    parser.add_argument('-o', '--operations', nargs='+', 
                        choices=['upload', 'download', 'list', 'upload_download'],
                        default=['upload', 'download', 'list', 'upload_download'],
                        help='Operations to test')
    
    parser.add_argument('-f', '--file-sizes', type=int, nargs='+', default=[10, 50, 100],
                        help='File sizes in MB')
    
    parser.add_argument('-c', '--client-workers', type=int, nargs='+', default=[1, 5, 50],
                        help='Number of client workers')
    
    parser.add_argument('-s', '--server-workers', type=int, nargs='+', default=[1, 5, 50],
                        help='Number of server workers')
    
    parser.add_argument('-t', '--single-test', action='store_true',
                        help='Run a single test instead of all combinations')
    
    parser.add_argument('-p', '--processes', action='store_true',
                        help='Use process pools instead of thread pools')
    
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
    
    setup_logging(log_file=log_file, verbose=args.verbose)
    logger = logging.getLogger()
    
    # Ensure report directory exists
    if not os.path.exists(args.report_dir):
        try:
            os.makedirs(args.report_dir)
            logger.info(f"Created report directory: {args.report_dir}")
        except Exception as e:
            logger.error(f"Could not create report directory: {str(e)}")
            return 1
    
    # Check if server is running
    logger.info("Checking if server is running...")
    if not is_server_running(SERVER_HOST, SERVER_PORT):
        logger.error(f"Server not running at {SERVER_HOST}:{SERVER_PORT}")
        return 1
    
    # Start timestamp
    start_time = time.time()
    logger.info(f"Starting stress test at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Run tests
        if args.single_test:
            # Run a single test with the first parameter from each list
            operation = args.operations[0]
            file_size_mb = args.file_sizes[0]
            client_workers = args.client_workers[0]
            server_workers = args.server_workers[0]
            
            logger.info(f"Running single test: {operation} - {file_size_mb}MB - {client_workers} clients")
            result = run_stress_test_scenario(
                1, operation, file_size_mb, client_workers, server_workers, use_threading=not args.processes
            )
            results = [result]
        else:
            # Run comprehensive test with all combinations
            logger.info("Running comprehensive test with all combinations")
            results = run_comprehensive_stress_test(
                args.operations, args.file_sizes, args.client_workers, args.server_workers
            )
        
        # Save results to CSV first
        csv_path = save_results_to_csv(results, args.report_dir)
        logger.info(f"Results saved to CSV: {csv_path}")
        
        # Then prepare detailed report
        prepare_test_report(results, args.report_dir)
        
        # Print summary
        logger.info("\nTest Summary:")
        for result in results:
            logger.info(
                f"Test {result.test_number}: {result.operation} - "
                f"{result.file_size_mb}MB - {result.client_workers} clients/{result.server_workers} servers - "
                f"Client Success: {result.successful_clients}/{result.client_workers} - "
                f"Server Success: {result.successful_servers}/{result.server_workers} - "
                f"Throughput: {result.throughput_per_client / (1024*1024):.2f} MB/s"
            )
        
    except KeyboardInterrupt:
        logger.info("Test interrupted by user.")
        return 130
    except Exception as e:
        logger.error(f"Error during testing: {str(e)}", exc_info=True)
        return 1
    finally:
        end_time = time.time()
        total_time = end_time - start_time
        logger.info(f"Stress test completed in {total_time:.2f} seconds")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

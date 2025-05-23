"""
Reporting module for stress testing client-server file operations.
Contains functions for presenting and analyzing test results.
"""
import os
import sys
import logging
import matplotlib.pyplot as plt
import numpy as np
from tabulate import tabulate
from typing import List, Dict, Union
import csv
from datetime import datetime

# Add parent directory to path to make imports work when running directly
if __name__ == "__main__":
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Use direct import if running as a module, otherwise use absolute import
try:
    from .config import StressTestResult
except ImportError:
    from stress_test.config import StressTestResult

def format_bytes(byte_count):
    """Format byte count as human-readable string"""
    if byte_count is None or byte_count == 0:
        return "0 B"
    
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB']
    i = 0
    while byte_count >= 1024 and i < len(suffixes) - 1:
        byte_count /= 1024.0
        i += 1
    return f"{byte_count:.2f} {suffixes[i]}"

def format_throughput(throughput_bytes_per_second):
    """Format throughput as human-readable string"""
    if throughput_bytes_per_second is None or throughput_bytes_per_second == 0:
        return "0 B/s"
    
    return f"{format_bytes(throughput_bytes_per_second)}/s"

def print_test_results_summary(results: List[StressTestResult]):
    """Print a summary table of all test results"""
    logger = logging.getLogger()
    
    # Prepare data for tabulate
    headers = ["Test", "Operation", "File Size", "Workers", "Success", "Failed", 
               "Upload time", "Download time", "Total time", "Throughput"]
    
    table_data = []
    for r in results:
        row = [
            r.test_number,
            r.operation,
            f"{r.file_size_mb} MB",
            r.client_workers,
            r.successful_clients,
            r.failed_clients,
            f"{r.upload_time_per_client:.2f}s" if r.upload_time_per_client else "-",
            f"{r.download_time_per_client:.2f}s" if r.download_time_per_client else "-",
            f"{r.total_time_per_client:.2f}s",
            format_throughput(r.throughput_per_client)
        ]
        table_data.append(row)
    
    # Print the table
    logger.info("\n==== TEST RESULTS SUMMARY ====\n")
    logger.info("\n" + tabulate(table_data, headers=headers, tablefmt="grid"))
    logger.info("\n")

def print_detailed_test_results(results: List[StressTestResult]):
    """Print detailed results for each test"""
    logger = logging.getLogger()
    
    logger.info("\n==== DETAILED TEST RESULTS ====\n")
    
    for result in results:
        logger.info(f"Test {result.test_number}: {result.operation.upper()} - "
                    f"{result.file_size_mb}MB - {result.client_workers} clients")
        
        logger.info(f"  Success/Fail: {result.successful_clients}/{result.failed_clients}")
        
        if result.upload_time_per_client:
            logger.info(f"  Upload time per client: {result.upload_time_per_client:.2f}s")
            logger.info(f"  Upload throughput per client: {format_throughput(result.upload_throughput_per_client)}")
        
        if result.download_time_per_client:
            logger.info(f"  Download time per client: {result.download_time_per_client:.2f}s")
            logger.info(f"  Download throughput per client: {format_throughput(result.download_throughput_per_client)}")
        
        logger.info(f"  Total time per client: {result.total_time_per_client:.2f}s")
        logger.info(f"  Throughput per client: {format_throughput(result.throughput_per_client)}")
        
        if result.error_messages:
            logger.info("  Errors:")
            for error in result.error_messages[:5]:  # Show first 5 errors at most
                logger.info(f"    - {error}")
            if len(result.error_messages) > 5:
                logger.info(f"    ... and {len(result.error_messages) - 5} more errors")
        
        logger.info("")  # Empty line between tests

def generate_charts(results: List[StressTestResult], output_dir: str = "."):
    """Generate charts visualizing the test results"""
    logger = logging.getLogger()
    logger.info("Generating charts...")
    
    try:
        # Group results by operation type
        operation_groups = {}
        for result in results:
            if result.operation not in operation_groups:
                operation_groups[result.operation] = []
            operation_groups[result.operation].append(result)
        
        # Charts for each operation type
        for operation, op_results in operation_groups.items():
            # Sort by file size and then by client count
            op_results.sort(key=lambda r: (r.file_size_mb, r.client_workers))
            
            # Group by file size
            file_size_groups = {}
            for r in op_results:
                if r.file_size_mb not in file_size_groups:
                    file_size_groups[r.file_size_mb] = []
                file_size_groups[r.file_size_mb].append(r)
            
            # Plot throughput by client count for each file size
            plt.figure(figsize=(10, 6))
            
            for file_size_mb, fs_results in file_size_groups.items():
                client_counts = [r.client_workers for r in fs_results]
                throughputs = [r.throughput_per_client / (1024*1024) for r in fs_results]  # Convert to MB/s
                
                plt.plot(client_counts, throughputs, marker='o', label=f"{file_size_mb} MB")
            
            plt.title(f"{operation.upper()} - Throughput by Client Count")
            plt.xlabel("Number of Clients")
            plt.ylabel("Throughput (MB/s)")
            plt.grid(True)
            plt.legend()
            
            # Save the chart
            chart_path = f"{output_dir}/{operation}_throughput_chart.png"
            plt.savefig(chart_path)
            logger.info(f"Saved chart: {chart_path}")
            
            # Plot time by client count for each file size
            plt.figure(figsize=(10, 6))
            
            for file_size_mb, fs_results in file_size_groups.items():
                client_counts = [r.client_workers for r in fs_results]
                times = [r.total_time_per_client for r in fs_results]
                
                plt.plot(client_counts, times, marker='o', label=f"{file_size_mb} MB")
            
            plt.title(f"{operation.upper()} - Time by Client Count")
            plt.xlabel("Number of Clients")
            plt.ylabel("Time (seconds)")
            plt.grid(True)
            plt.legend()
            
            # Save the chart
            chart_path = f"{output_dir}/{operation}_time_chart.png"
            plt.savefig(chart_path)
            logger.info(f"Saved chart: {chart_path}")
        
        # Compare operations
        if len(operation_groups) > 1:
            # Get average throughput for each operation
            op_throughputs = {}
            for op, op_results in operation_groups.items():
                op_throughputs[op] = np.mean([r.throughput_per_client / (1024*1024) for r in op_results])  # MB/s
            
            # Bar chart comparing operations
            plt.figure(figsize=(10, 6))
            operations = list(op_throughputs.keys())
            throughputs = list(op_throughputs.values())
            
            plt.bar(operations, throughputs)
            plt.title("Average Throughput by Operation")
            plt.xlabel("Operation")
            plt.ylabel("Throughput (MB/s)")
            plt.grid(True, axis='y')
            
            # Save the chart
            chart_path = f"{output_dir}/operation_comparison_chart.png"
            plt.savefig(chart_path)
            logger.info(f"Saved chart: {chart_path}")
        
    except Exception as e:
        logger.error(f"Error generating charts: {str(e)}")
    
    finally:
        plt.close('all')

def save_results_to_csv(results: List[Union[StressTestResult, Dict]], output_dir: str = ".") -> str:
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
            # Handle both StressTestResult objects and dictionaries
            if isinstance(r, StressTestResult):
                row = [
                    r.test_number,
                    r.operation,
                    r.file_size_mb,
                    r.client_workers,
                    r.server_workers,
                    f"{r.total_time_per_client:.2f}",
                    f"{r.throughput_per_client / (1024*1024):.2f}",  # Convert to MB/s
                    r.successful_clients,
                    r.failed_clients,
                    r.successful_servers,
                    r.failed_servers
                ]
            else:  # Dictionary
                row = [
                    r['test_number'],
                    r['operation'],
                    r['file_size_mb'],
                    r['client_workers'],
                    r['server_workers'],
                    f"{r['total_time']:.2f}",
                    f"{r['throughput'] / (1024*1024):.2f}",  # Convert to MB/s
                    r.get('successful_clients', 0),
                    r.get('failed_clients', 0),
                    r.get('successful_servers', 0),
                    r.get('failed_servers', 0)
                ]
            writer.writerow(row)
    
    return csv_path

def prepare_test_report(results: List[StressTestResult], output_dir: str = "."):
    """Prepare a comprehensive test report including text summary and charts"""
    logger = logging.getLogger()
    logger.info("Preparing test report...")
    
    # Filter only upload and download operations for reporting
    filtered_results = [r for r in results if r.operation in ['upload', 'download']]
    
    # Print text summaries
    print_test_results_summary(filtered_results)
    print_detailed_test_results(filtered_results)
    
    # Save results to CSV
    save_results_to_csv(filtered_results, output_dir)
    
    # Generate charts
    generate_charts(filtered_results, output_dir)
    
    logger.info(f"Report generation completed. Results saved to {output_dir}")

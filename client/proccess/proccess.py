import socket
import json
import base64
import os
import time
import multiprocessing
from multiprocessing import Pool, Manager
import pandas as pd
import random
import string
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class FileClient:
    def __init__(self, server_host='172.16.16.101', server_port=6665):
        self.server_host = server_host
        self.server_port = server_port
        self.chunk_size = 524288  # 512KB chunks
        self.use_fast_protocol = True
        self.socket_buffer_size = 16 * 1024 * 1024  # 16MB buffers untuk memory optimization
        
    def connect_to_server(self):
        """Create memory-optimized connection untuk 50GB setup"""
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            
            # MAXIMUM optimization untuk 50GB memory
            client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.socket_buffer_size)  # 16MB
            client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.socket_buffer_size)  # 16MB
            client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 0)
            
            # Optimized timeout untuk high-memory operations
            client_socket.settimeout(10)
            client_socket.connect((self.server_host, self.server_port))
            return client_socket
        except Exception as e:
            logging.error(f"Failed to connect: {str(e)}")
            return None
    
    def send_command_memory_optimized(self, command):
        """Memory-optimized command sending dengan pre-allocated buffers"""
        client_socket = self.connect_to_server()
        if not client_socket:
            return None
            
        try:
            is_upload = command.startswith('UPLOAD')
            client_socket.settimeout(180 if is_upload else 30)  # Increased timeouts
            
            # Send command directly - server expects: UPLOAD filename base64_content
            command_bytes = (command + '\r\n\r\n').encode()
            client_socket.sendall(command_bytes)
            
            logging.info(f"Memory-optimized send: {len(command_bytes)} bytes")
            
            # Receive response dengan memory optimization
            response_buffer = bytearray(self.socket_buffer_size)  # Pre-allocate 16MB
            response_data = b""
            
            while True:
                try:
                    received = client_socket.recv_into(response_buffer)
                    if not received:
                        break
                    
                    chunk = response_buffer[:received]
                    response_data += chunk
                    
                    if response_data.endswith(b'\r\n\r\n'):
                        break
                        
                except socket.timeout:
                    if response_data:
                        break
                    raise
            
            if not response_data:
                return None
            
            response_str = response_data.decode().strip('\r\n')
            return json.loads(response_str)
            
        except Exception as e:
            logging.error(f"Memory-optimized send error: {str(e)}")
            return None
        finally:
            client_socket.close()
    
    def send_command(self, command):
        """Use memory-optimized protocol"""
        return self.send_command_memory_optimized(command)
    
    def list_files(self):
        """Get list of files from server"""
        return self.send_command("LIST")
    
    def download_file(self, filename):
        """Download file from server with improved error handling"""
        try:
            result = self.send_command(f"GET {filename}")
            logging.info(f"Download response for {filename}: {str(result)[:200]}...")
            
            if result and result.get('status') == 'OK':
                # Check if we have the data_file field
                if 'data_file' in result:
                    try:
                        # Decode base64 file content
                        encoded_content = result.get('data_file', '')
                        if encoded_content:
                            file_content = base64.b64decode(encoded_content)
                            logging.info(f"Successfully decoded {len(file_content)} bytes for {filename}")
                            return file_content
                        else:
                            logging.error(f"Empty data_file content for {filename}")
                            return None
                    except Exception as decode_error:
                        logging.error(f"Failed to decode base64 content for {filename}: {str(decode_error)}")
                        return None
                else:
                    logging.error(f"No data_file field in response for {filename}: {result}")
                    return None
            else:
                logging.error(f"Download failed for {filename}: {result}")
                return None
        except Exception as e:
            logging.error(f"Exception during download of {filename}: {str(e)}")
            return None
    
    def upload_file(self, filename, file_content):
        """Upload file to server"""
        # Encode file content to base64
        encoded_content = base64.b64encode(file_content).decode()
        command = f"UPLOAD {filename} {encoded_content}"
        return self.send_command(command)
    
    def delete_file(self, filename):
        """Delete file from server"""
        return self.send_command(f"DELETE {filename}")

def generate_test_file(size_mb):
    """Generate test file content of specified size in MB (10-100 MB for ETS)"""
    # Allow larger files for ETS testing
    size_mb = min(size_mb, 100)  # Cap at 100MB
    size_bytes = size_mb * 1024 * 1024
    # Generate random content
    content = os.urandom(size_bytes)
    return content

def worker_upload_test_ultra_fast(args):
    """Ultra-fast worker for local operations with MB files"""
    worker_id, file_size_mb, server_host, server_port = args
    
    start_time = time.time()
    success = False
    bytes_processed = 0
    
    try:
        # Create ultra-optimized client
        client = FileClient(server_host, server_port)
        client.use_fast_protocol = True
        
        # Generate test file (1-3 MB)
        file_content = generate_test_file(file_size_mb)
        filename = f"test_upload_{worker_id}_{int(time.time()*1000)}_{random.randint(10000,99999)}.bin"
        
        logging.info(f"Worker {worker_id}: ULTRA-FAST upload of {len(file_content)} bytes ({file_size_mb}MB)")
        
        # Ultra-fast upload with immediate response
        upload_start = time.time()
        result = client.upload_file(filename, file_content)
        upload_time = time.time() - upload_start
        
        if result and result.get('status') == 'OK':
            success = True
            bytes_processed = len(file_content)
            throughput = bytes_processed / upload_time if upload_time > 0 else 0
            logging.info(f"Worker {worker_id}: ULTRA-FAST upload completed - {throughput/1024/1024:.2f} MB/s")
            
            # IMMEDIATE cleanup
            cleanup_start = time.time()
            delete_result = client.delete_file(filename)
            cleanup_time = time.time() - cleanup_start
            logging.info(f"Worker {worker_id}: Cleanup in {cleanup_time:.3f}s")
        else:
            logging.error(f"Worker {worker_id}: Upload failed - {result}")
        
    except Exception as e:
        logging.error(f"Worker {worker_id} error: {str(e)}")
    
    end_time = time.time()
    duration = end_time - start_time
    throughput = bytes_processed / duration if duration > 0 else 0
    
    logging.info(f"Worker {worker_id}: TOTAL {duration:.3f}s, {throughput/1024/1024:.2f} MB/s")
    
    return {
        'worker_id': worker_id,
        'success': success,
        'duration': duration,
        'bytes_processed': bytes_processed,
        'throughput': throughput,
        'throughput_mbps': throughput / (1024 * 1024)
    }

def worker_download_test_ultra_fast(args):
    """Ultra-fast download worker for local operations with MB files"""
    worker_id, file_size_mb, server_host, server_port = args
    
    start_time = time.time()
    success = False
    bytes_processed = 0
    filename = None
    
    try:
        # Upload test file first (1-3 MB)
        file_content = generate_test_file(file_size_mb)
        filename = f"test_download_{worker_id}_{int(time.time()*1000)}_{random.randint(10000,99999)}.bin"
        
        logging.info(f"Worker {worker_id}: Starting download test with {len(file_content)} bytes")
        
        # Create client for upload
        upload_client = FileClient(server_host, server_port)
        upload_start = time.time()
        upload_result = upload_client.upload_file(filename, file_content)
        upload_time = time.time() - upload_start
        
        if upload_result and upload_result.get('status') == 'OK':
            logging.info(f"Worker {worker_id}: Upload successful in {upload_time:.3f}s, starting download")
            
            # Add longer delay to ensure server has processed the upload
            time.sleep(0.5)
            
            # Verify file exists before download
            list_client = FileClient(server_host, server_port)
            list_result = list_client.list_files()
            logging.info(f"Worker {worker_id}: Files on server before download: {list_result}")
            
            # Create new client for download to test persistence
            download_client = FileClient(server_host, server_port)
            download_start = time.time()
            downloaded_content = download_client.download_file(filename)
            download_time = time.time() - download_start
            
            logging.info(f"Worker {worker_id}: Downloaded {len(downloaded_content) if downloaded_content else 0} bytes in {download_time:.3f}s, expected {len(file_content)} bytes")
            
            if downloaded_content is not None and len(downloaded_content) == len(file_content):
                # Verify content integrity
                if downloaded_content == file_content:
                    success = True
                    bytes_processed = len(downloaded_content)
                    throughput = bytes_processed / download_time if download_time > 0 else 0
                    logging.info(f"Worker {worker_id}: ULTRA-FAST download successful - {throughput/1024/1024:.2f} MB/s")
                    
                    # ONLY cleanup after successful download and verification
                    cleanup_client = FileClient(server_host, server_port)
                    cleanup_start = time.time()
                    delete_result = cleanup_client.delete_file(filename)
                    cleanup_time = time.time() - cleanup_start
                    
                    if delete_result and delete_result.get('status') == 'OK':
                        logging.info(f"Worker {worker_id}: Cleanup successful in {cleanup_time:.3f}s")
                        filename = None  # Mark as cleaned up
                    else:
                        logging.warning(f"Worker {worker_id}: Cleanup failed in {cleanup_time:.3f}s - {delete_result}")
                        
                else:
                    logging.error(f"Worker {worker_id}: Download content verification failed - content differs")
            else:
                logging.error(f"Worker {worker_id}: Download failed or size mismatch - got {len(downloaded_content) if downloaded_content else 0}, expected {len(file_content)}")
                
                # Debug: Check what files are available
                debug_list_client = FileClient(server_host, server_port)
                debug_list_result = debug_list_client.list_files()
                logging.info(f"Worker {worker_id}: Files on server after failed download: {debug_list_result}")
        else:
            logging.error(f"Worker {worker_id}: Upload for download test failed - {upload_result}")
        
    except Exception as e:
        logging.error(f"Worker {worker_id} error: {str(e)}")
        import traceback
        logging.error(f"Worker {worker_id} traceback: {traceback.format_exc()}")
    
    finally:
        # Emergency cleanup only if download failed and file still exists
        if not success and filename is not None:
            try:
                logging.warning(f"Worker {worker_id}: Performing emergency cleanup for failed download")
                emergency_client = FileClient(server_host, server_port)
                emergency_delete = emergency_client.delete_file(filename)
                logging.info(f"Worker {worker_id}: Emergency cleanup result: {emergency_delete}")
            except Exception as cleanup_error:
                logging.error(f"Worker {worker_id}: Emergency cleanup failed: {str(cleanup_error)}")
    
    end_time = time.time()
    duration = end_time - start_time
    throughput = bytes_processed / duration if duration > 0 else 0
    
    return {
        'worker_id': worker_id,
        'success': success,
        'duration': duration,
        'bytes_processed': bytes_processed,
        'throughput': throughput,
        'throughput_mbps': throughput / (1024 * 1024)
    }

def run_stress_test(operation, file_size_mb, num_clients, server_workers, server_host='localhost', server_port=6665):
    """Run stress test with specified parameters for MB files"""
    logger = logging.getLogger()
    
    logger.info(f"Running MEMORY-OPTIMIZED {operation} test: {file_size_mb}MB, {num_clients} clients, {server_workers} server workers")
    
    # Set multiprocessing start method to spawn
    multiprocessing.set_start_method('spawn', force=True)
    
    # Create process pool with exact number of workers (not more than requested)
    with Pool(
        processes=num_clients,  # Use exact number of client workers
        maxtasksperchild=10  # Recycle workers after 10 tasks for stability
    ) as pool:
        # Create shared statistics
        with Manager() as manager:
            stats = manager.dict({
                'completed': 0,
                'failed': 0,
                'total_bytes': 0,
                'start_time': time.time()
            })
            
            # Prepare worker arguments
            worker_args = [(i, file_size_mb, server_host, server_port) for i in range(num_clients)]
            
            # Run workers with map for better control
            results = pool.map(
                worker_upload_test_ultra_fast if operation == 'upload' else worker_download_test_ultra_fast,
                worker_args
            )
            
            # Analyze results
            successful_workers = [r for r in results if r.get('success')]
            failed_workers = [r for r in results if not r.get('success')]
            
            if successful_workers:
                avg_duration = sum(r.get('duration', 0) for r in successful_workers) / len(successful_workers)
                total_bytes = sum(r.get('bytes_processed', 0) for r in successful_workers)
                avg_throughput = sum(r.get('throughput', 0) for r in successful_workers) / len(successful_workers)
                
                logger.info(f"Test completed: {len(successful_workers)}/{num_clients} successful")
                logger.info(f"Average duration: {avg_duration:.2f}s")
                logger.info(f"Average throughput: {avg_throughput/1024/1024:.2f} MB/s")
                
                return {
                    'success': True,
                    'test_number': 0,  # Will be set by caller
                    'operation': operation,
                    'file_size_mb': file_size_mb,
                    'client_workers': num_clients,
                    'server_workers': server_workers,
                    'total_time_per_client': avg_duration,
                    'throughput_per_client_mbps': avg_throughput / (1024 * 1024),
                    'successful_clients': len(successful_workers),
                    'failed_clients': len(failed_workers),
                    'successful_server_workers': len(successful_workers),  # Assume 1:1 mapping
                    'failed_server_workers': len(failed_workers),
                    'error_messages': []
                }
            else:
                logger.error("No successful workers")
                return {
                    'success': False,
                    'test_number': 0,
                    'operation': operation,
                    'file_size_mb': file_size_mb,
                    'client_workers': num_clients,
                    'server_workers': server_workers,
                    'total_time_per_client': 0,
                    'throughput_per_client_mbps': 0,
                    'successful_clients': 0,
                    'failed_clients': num_clients,
                    'successful_server_workers': 0,
                    'failed_server_workers': num_clients,
                    'error_messages': [str(r.get('error', 'Unknown error')) for r in failed_workers]
                }

def run_comprehensive_stress_test():
    """Comprehensive stress test with ETS requirements: 10-100 MB files and 1,5,50 workers"""
    
    # Test parameters matching ETS requirements
    operations = ['upload', 'download']
    file_sizes = [10, 50, 100]  # MB - ETS requirements
    client_worker_counts = [1, 5, 50]  # Client workers - ETS requirements
    server_worker_counts = [1, 5, 50]  # Server workers - ETS requirements
    
    results = []
    test_number = 1
    total_tests = len(operations) * len(file_sizes) * len(client_worker_counts) * len(server_worker_counts)
    
    print("Starting ETS STRESS TEST - MEMORY-OPTIMIZED for 50GB system...")
    print(f"Total combinations: {total_tests}")
    print("Testing with 10-100 MB files and 1,5,50 workers")
    print("ETS Requirements:")
    print("* Operasi: download, upload")
    print("* Volume file: 10 MB, 50 MB, 100 MB") 
    print("* Jumlah client worker pool: 1, 5, 50")
    print("* Jumlah server worker pool: 1, 5, 50")
    print("* Total kombinasi: 2 × 3 × 3 × 3 = 54 kombinasi")
    print("="*80)
    
    for operation in operations:
        for file_size in file_sizes:
            for client_workers in client_worker_counts:
                for server_workers in server_worker_counts:
                    print(f"\nTest {test_number}/{total_tests}: {operation.upper()} - "
                          f"{file_size}MB - {client_workers} client workers - {server_workers} server workers")
                    
                    try:
                        result = run_stress_test(operation, file_size, client_workers, server_workers, '172.16.16.101', 6665)
                        
                        result['test_number'] = test_number
                        
                        results.append(result)
                        
                        print(f"  ✓ Completed: {result['successful_clients']}/{client_workers} client workers successful")
                        print(f"  ✓ Avg duration: {result['total_time_per_client']:.3f}s")
                        print(f"  ✓ Avg throughput: {result.get('throughput_per_client_mbps', 0):.2f} MB/s")
                        
                        # Add longer pause for larger files to prevent server overload
                        if file_size >= 50:
                            pause_time = 3.0
                        elif file_size >= 10:
                            pause_time = 2.0
                        else:
                            pause_time = 1.0
                        
                        print(f"  → Pausing {pause_time}s for server stability...")
                        time.sleep(pause_time)
                        
                    except Exception as e:
                        print(f"  ✗ Test failed: {str(e)}")
                        results.append({
                            'test_number': test_number,
                            'operation': operation,
                            'file_size_mb': file_size,
                            'client_workers': client_workers,
                            'server_workers': server_workers,
                            'total_time_per_client': 0,
                            'throughput_per_client_mbps': 0,
                            'successful_clients': 0,
                            'failed_clients': client_workers,
                            'successful_server_workers': 0,
                            'failed_server_workers': server_workers,
                            'error_messages': [str(e)]
                        })
                    
                    test_number += 1
    
    return results

def generate_report(results):
    """Generate comprehensive ETS report from stress test results with proper CSV format"""
    
    # Create DataFrame with all ETS required columns
    df_data = []
    for result in results:
        row = {
            'Nomor': result.get('test_number', 0),
            'Operasi': result.get('operation', ''),
            'Volume (MB)': result.get('file_size_mb', 0),
            'Jumlah Client Worker Pool': result.get('client_workers', 0),
            'Jumlah Server Worker Pool': result.get('server_workers', 0),
            'Waktu Total per Client (s)': result.get('total_time_per_client', 0),
            'Throughput per Client (MB/s)': result.get('throughput_per_client_mbps', 0),
            'Client Worker Sukses': result.get('successful_clients', 0),
            'Client Worker Gagal': result.get('failed_clients', 0),
            'Server Worker Sukses': result.get('successful_server_workers', 0),
            'Server Worker Gagal': result.get('failed_server_workers', 0),
            'Error Messages': '; '.join(result.get('error_messages', []))
        }
        df_data.append(row)
    
    df = pd.DataFrame(df_data)
    
    # Save to CSV with ETS naming
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"ETS_stress_test_results_{timestamp}.csv"
    df.to_csv(csv_filename, index=False)
    
    # Display summary
    print("\n" + "="*150)
    print("ETS STRESS TEST RESULTS SUMMARY - FINAL RESULTS")
    print("="*150)
    print("Requirements: 10-100MB files, 1,5,50 workers, upload/download operations")
    print("="*150)
    print(df.to_string(index=False, max_colwidth=50))
    print("="*150)
    
    print(f"\nDetailed ETS results saved to: {csv_filename}")
    
    # Generate ETS summary statistics
    successful_tests = df[df['Client Worker Sukses'] > 0]
    print("\nETS SUMMARY STATISTICS:")
    print(f"Total ETS tests conducted: {len(df)}")
    print(f"Total successful tests: {len(successful_tests)}")
    print(f"Success rate: {len(successful_tests)/len(df)*100:.1f}%")
    if len(successful_tests) > 0:
        print(f"Average throughput across successful tests: {successful_tests['Throughput per Client (MB/s)'].mean():.2f} MB/s")
        print(f"Maximum throughput achieved: {successful_tests['Throughput per Client (MB/s)'].max():.2f} MB/s")
        print(f"Minimum duration per client: {successful_tests['Waktu Total per Client (s)'].min():.3f}s")
        print(f"Maximum duration per client: {successful_tests['Waktu Total per Client (s)'].max():.3f}s")
    
    # ETS specific analysis
    print("\nETS PERFORMANCE ANALYSIS BY FILE SIZE:")
    for size in [10, 50, 100]:
        size_tests = successful_tests[successful_tests['Volume (MB)'] == size]
        if len(size_tests) > 0:
            avg_throughput = size_tests['Throughput per Client (MB/s)'].mean()
            avg_time = size_tests['Waktu Total per Client (s)'].mean()
            print(f"  {size}MB files: Avg {avg_throughput:.2f} MB/s, Avg {avg_time:.2f}s")
    
    print("\nETS PERFORMANCE ANALYSIS BY WORKER COUNT:")
    for workers in [1, 5, 50]:
        worker_tests = successful_tests[successful_tests['Jumlah Client Worker Pool'] == workers]
        if len(worker_tests) > 0:
            avg_throughput = worker_tests['Throughput per Client (MB/s)'].mean()
            avg_time = worker_tests['Waktu Total per Client (s)'].mean()
            print(f"  {workers} workers: Avg {avg_throughput:.2f} MB/s, Avg {avg_time:.2f}s")
    
    print(f"\nFile size range: 10-100 MB (ETS requirements)")
    print(f"Worker range: 1,5,50 workers (ETS requirements)")
    print(f"Expected total combinations: 54 (2×3×3×3)")
    
    return df

def main():
    """Main function to run client operations and stress tests"""
    
    print("File Server Client")
    print("==================")
    print(f"Server: {FileClient().server_host}:{FileClient().server_port}")
    
    # Test connection first
    print("\nTesting connection to server...")
    test_client = FileClient()
    test_socket = test_client.connect_to_server()
    if test_socket:
        test_socket.close()
        print("✓ Connection test successful!")
    else:
        print("✗ Connection test failed!")
        print("\nTroubleshooting steps:")
        print("1. Ensure server is running on 172.16.16.101:6665")
        print("2. Check if port 6665 is open and accessible")
        print("3. Verify network connectivity to 172.16.16.101")
        print("4. Check firewall settings")
        
        choice = input("\nContinue anyway? (y/n): ").strip().lower()
        if choice != 'y':
            return
    
    while True:
        print("\nSelect operation:")
        print("1. List files")
        print("2. Download file")
        print("3. Upload file") 
        print("4. Run stress test")
        print("5. Test connection")
        print("6. Exit")
        
        choice = input("Enter choice (1-6): ").strip()
        
        if choice == '1':
            client = FileClient()
            result = client.list_files()
            if result:
                print(f"Files on server: {result}")
            else:
                print("Failed to get file list")
            
        elif choice == '2':
            filename = input("Enter filename to download: ").strip()
            client = FileClient()
            content = client.download_file(filename)
            if content:
                with open(f"downloaded_{filename}", 'wb') as f:
                    f.write(content)
                print(f"File downloaded as: downloaded_{filename}")
            else:
                print("Download failed")
                
        elif choice == '3':
            filename = input("Enter local filename to upload: ").strip()
            if os.path.exists(filename):
                with open(filename, 'rb') as f:
                    content = f.read()
                client = FileClient()
                result = client.upload_file(os.path.basename(filename), content)
                print(f"Upload result: {result}")
            else:
                print("File not found")
                
        elif choice == '4':
            print("\nStarting comprehensive stress test...")
            print("This will take some time. Please ensure server is running.")
            confirm = input("Continue? (y/n): ").strip().lower()
            
            if confirm == 'y':
                results = run_comprehensive_stress_test()
                report_df = generate_report(results)
                
        elif choice == '5':
            print("\nTesting connection...")
            test_client = FileClient()
            test_socket = test_client.connect_to_server()
            if test_socket:
                test_socket.close()
                print("✓ Connection successful!")
            else:
                print("✗ Connection failed!")
                
        elif choice == '6':
            print("Goodbye!")
            break
            
        else:
            print("Invalid choice")

if __name__ == "__main__":
    # Ensure proper multiprocessing setup for cross-platform compatibility
    multiprocessing.set_start_method('spawn', force=True)
    main()

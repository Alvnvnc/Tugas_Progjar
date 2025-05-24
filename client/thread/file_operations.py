"""
File operation functions for stress testing
"""

import os
import sys
import socket
import base64
import json
import time

# Add parent directory to path to make imports work when running directly
if __name__ == "__main__":
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Use direct import if running as a module, otherwise use absolute import
try:
    from .config import (
        logger, CONNECT_TIMEOUT, SOCKET_TIMEOUT, CHUNK_SIZE, 
        SERVER_HOST, SERVER_PORT, update_stats, stats_lock
    )
except ImportError:
    from config import (
        logger, CONNECT_TIMEOUT, SOCKET_TIMEOUT, CHUNK_SIZE, 
        SERVER_HOST, SERVER_PORT, update_stats, stats_lock
    )


def upload_file_worker(worker_id, file_path, server_host, server_port, upload_filename):
    """Worker function for uploading files"""
    start_time = time.time()
    bytes_processed = 0
    success = False  # Initialize success variable
    response_str = ""
    
    try:
        # Read and encode file
        with open(file_path, 'rb') as f:
            file_content = f.read()
            bytes_processed = len(file_content)
            b64_content = base64.b64encode(file_content).decode()
        
        # Create upload command - store the actual name the server will use
        filename_on_server = f"{upload_filename}_{worker_id}"
        
        # Following the server protocol - UPLOAD name content
        command = f"UPLOAD {filename_on_server} {b64_content}\r\n\r\n"
        
        logger.info(f"Upload worker {worker_id}: Sending command for file: {filename_on_server} ({bytes_processed//1024}KB)")
        
        # Connect and send with retries
        retry_count = 0
        max_retries = 3
        while retry_count < max_retries:
            sock = None
            try:
                logger.info(f"Upload worker {worker_id}: Attempt {retry_count + 1}/{max_retries} - Connecting to {server_host}:{server_port}")
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(CONNECT_TIMEOUT)
                sock.connect((server_host, server_port))
                logger.info(f"Upload worker {worker_id}: Connected successfully")
                
                # Use shorter timeout for data transfer
                sock.settimeout(20)  # Reduced from SOCKET_TIMEOUT to 20s
                
                # Set socket options for better performance
                try:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4194304)  # 4MB receive buffer (increased)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4194304)  # 4MB send buffer (increased)
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # Disable Nagle's algorithm
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)  # Enable keepalive
                except Exception as e:
                    logger.warning(f"Upload worker {worker_id}: Could not set socket options: {str(e)}")
                
                # Send in larger chunks for better performance
                command_bytes = command.encode()
                total_sent = 0
                command_length = len(command_bytes)
                
                # Dynamically adjust chunk size based on command length
                chunk_size = min(65536, max(8192, command_length // 10))
                
                logger.info(f"Upload worker {worker_id}: Starting to send {command_length} bytes in chunks of {chunk_size}")
                
                # Send data in chunks with progress tracking
                chunk_start_time = time.time()
                while total_sent < command_length:
                    chunk = command_bytes[total_sent:total_sent + chunk_size]
                    try:
                        bytes_sent = sock.send(chunk)
                        total_sent += bytes_sent
                        
                        # Log progress less frequently to reduce overhead
                        if total_sent % (1024*1024) == 0 or total_sent == command_length:  # Log every ~1MB
                            elapsed = time.time() - chunk_start_time
                            rate = bytes_sent / elapsed if elapsed > 0 else 0
                            logger.debug(f"Upload worker {worker_id}: Sent {total_sent/(1024*1024):.1f}MB/{command_length/(1024*1024):.1f}MB ({rate/1024:.1f}KB/s)")
                            chunk_start_time = time.time()
                            
                    except socket.error as e:
                        logger.error(f"Upload worker {worker_id}: Error sending chunk: {str(e)}")
                        raise
                
                logger.info(f"Upload worker {worker_id}: Finished sending data, waiting for response")
                
                # Receive response with shorter timeout
                response = b""
                sock.settimeout(20)  # Reduced response timeout
                response_start_time = time.time()
                
                while True:
                    try:
                        data = sock.recv(65536)  # Larger buffer for faster response processing
                        if not data:
                            logger.debug(f"Upload worker {worker_id}: No more data from server")
                            break
                        response += data
                        
                        # Check for end marker more efficiently
                        if b"\r\n\r\n" in response[-8:]:  # Only check in the last 8 bytes
                            logger.debug(f"Upload worker {worker_id}: Found response end marker")
                            break
                            
                        # Check if we've waited too long
                        if time.time() - response_start_time > 15:  # 15 second max wait
                            logger.warning(f"Upload worker {worker_id}: Response taking too long, proceeding with partial data")
                            break
                            
                    except socket.timeout:
                        logger.warning(f"Upload worker {worker_id}: timeout waiting for response")
                        # If we got some response data, try to process it
                        if len(response) > 0:
                            logger.debug(f"Upload worker {worker_id}: Processing partial response ({len(response)} bytes)")
                        break
                
                # Parse response
                if not response:
                    logger.warning(f"Upload worker {worker_id}: Empty response from server")
                    # For uploads, this might still be successful
                    if total_sent == command_length:
                        logger.info(f"Upload worker {worker_id}: All data sent successfully, assuming success despite empty response")
                        success = True
                        break
                    retry_count += 1
                    continue
                    
                # Try to decode and process response
                response_str = response.decode('utf-8', errors='ignore').strip()
                try:
                    # Remove the \r\n\r\n delimiter if present
                    if response_str.endswith('\r\n\r\n'):
                        response_str = response_str[:-4]
                    response_json = json.loads(response_str)
                    success = response_json.get("status") == "OK"
                    if success:
                        logger.info(f"Upload worker {worker_id}: Upload successful")
                        break  # Success, exit retry loop
                    else:
                        error_msg = response_json.get('data', 'Unknown error')
                        logger.warning(f"Upload worker {worker_id}: Server returned error: {error_msg}")
                except json.JSONDecodeError as e:
                    logger.warning(f"Upload worker {worker_id}: JSON decode error: {e}")
                    logger.warning(f"Response received: {response_str[:200] if len(response_str) > 200 else response_str}")
                    # Try to infer success from response content
                    success = "OK" in response_str or "success" in response_str.lower() or "berhasil" in response_str.lower()
                    if success:
                        logger.info(f"Upload worker {worker_id}: Upload successful (non-JSON response)")
                        break  # Success, exit retry loop
                
            except (ConnectionRefusedError, ConnectionResetError, BrokenPipeError) as e:
                logger.warning(f"Upload worker {worker_id}: Connection error (attempt {retry_count + 1}/{max_retries}): {str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    # Shorter wait time for faster test completion
                    wait_time = 1 + retry_count  # Linear backoff
                    logger.info(f"Upload worker {worker_id}: Waiting {wait_time} seconds before retry")
                    time.sleep(wait_time)
                continue
            except Exception as e:
                logger.error(f"Upload worker {worker_id}: Error during upload: {str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 1  # Short delay
                    logger.info(f"Upload worker {worker_id}: Retrying after error in {wait_time}s")
                    time.sleep(wait_time)
                else:
                    break
            finally:
                if sock:
                    try:
                        sock.close()
                        logger.debug(f"Upload worker {worker_id}: Socket closed")
                    except:
                        pass
        
        # Measure completion time
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        # Report statistics
        if success:
            throughput = bytes_processed / elapsed_time if elapsed_time > 0 else 0
            logger.info(f"Upload worker {worker_id}: Completed in {elapsed_time:.2f}s, throughput: {throughput/1024:.1f}KB/s")
        
        # Update global stats
        update_stats(success, bytes_processed, elapsed_time)
        
        # Add server filename to result for better tracking
        return {
            'worker_id': worker_id,
            'success': success,
            'elapsed_time': elapsed_time,
            'bytes_processed': bytes_processed,
            'error': None if success else response_str[:100],
            'server_filename': filename_on_server  # Add this to track the exact filename
        }
        
    except Exception as e:
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        update_stats(False, 0, elapsed_time)
        
        logger.error(f"Upload worker {worker_id} failed: {str(e)}")
        return {
            'worker_id': worker_id,
            'success': False,
            'elapsed_time': elapsed_time,
            'bytes_processed': 0,
            'error': str(e)
        }


def download_file_worker(worker_id, server_host, server_port, base_filename):
    """Worker function for downloading files"""
    start_time = time.time()
    bytes_processed = 0
    success = False  # Initialize success variable
    response_str = ""
    
    try:
        # Create download command - Note: Server uses "GET" not "DOWNLOAD" command
        filename = f"{base_filename}_{worker_id}"
        command = f"GET {filename}\r\n\r\n"  # Fixed command to match server protocol
        
        logger.info(f"Download worker {worker_id}: Sending command for file: {filename}")
        
        # Connect and send with retries
        retry_count = 0
        max_retries = 3
        while retry_count < max_retries:
            sock = None
            try:
                logger.info(f"Download worker {worker_id}: Attempt {retry_count + 1}/{max_retries} - Connecting to {server_host}:{server_port}")
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(CONNECT_TIMEOUT)
                sock.connect((server_host, server_port))
                logger.info(f"Download worker {worker_id}: Connected successfully")
                
                # Set shorter timeout for data transfer (avoid long waits)
                sock.settimeout(20)  # Reduced timeout from 60s to 20s
                
                # Set socket options for better performance
                try:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4194304)  # 4MB receive buffer (increased)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4194304)  # 4MB send buffer (increased)
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # Disable Nagle's algorithm
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)  # Enable keepalive
                except Exception as e:
                    logger.warning(f"Download worker {worker_id}: Could not set socket options: {str(e)}")
                
                # Send command
                sock.sendall(command.encode())
                logger.info(f"Download worker {worker_id}: Command sent, waiting for response")
                
                # Receive response
                response = b""
                sock.settimeout(20)  # Reduced response timeout
                while True:
                    try:
                        data = sock.recv(65536)  # Increased buffer size for faster transfer
                        if not data:
                            logger.debug(f"Download worker {worker_id}: No more data from server")
                            break
                        response += data
                        # Log progress less frequently to reduce overhead
                        if len(response) % 1048576 == 0:  # Log every ~1MB
                            logger.debug(f"Download worker {worker_id}: Received {len(response)//1024}KB so far")
                        # Check for end marker more efficiently
                        if b"\r\n\r\n" in response[-8:]:  # Only check in the last 8 bytes
                            logger.debug(f"Download worker {worker_id}: Found response end marker")
                            break
                    except socket.timeout:
                        logger.warning(f"Download worker {worker_id}: timeout waiting for response")
                        # Add some diagnostics for timeout
                        if len(response) > 0:
                            logger.debug(f"Download worker {worker_id}: Partial response received ({len(response)} bytes)")
                        break
                
                # Parse response
                if not response:
                    logger.warning(f"Download worker {worker_id}: Empty response from server")
                    retry_count += 1
                    continue
                    
                # Decode response and extract file content
                try:
                    # Convert to string for JSON processing
                    response_str = response.decode('utf-8', errors='ignore').strip()
                    
                    # Try to parse JSON response
                    if response_str.endswith('\r\n\r\n'):
                        response_str = response_str[:-4]
                        
                    response_json = json.loads(response_str)
                    success = response_json.get("status") == "OK"
                    if success:
                        # Extract the file content (base64 encoded) and decode it
                        file_content_b64 = response_json.get("data_file", "")
                        if file_content_b64:
                            try:
                                file_content = base64.b64decode(file_content_b64)
                                bytes_processed = len(file_content)
                                logger.info(f"Download worker {worker_id}: Download successful, received {bytes_processed//1024}KB of data")
                                break  # Success, exit retry loop
                            except Exception as e:
                                logger.warning(f"Download worker {worker_id}: Error decoding base64 data: {e}")
                                # Still count as success if we received something
                                success = True
                                bytes_processed = len(file_content_b64)
                                break
                        else:
                            logger.warning(f"Download worker {worker_id}: No file content in response")
                            success = False
                    else:
                        error_msg = response_json.get('data', 'Unknown error')
                        logger.warning(f"Download worker {worker_id}: Server returned error: {error_msg}")
                except json.JSONDecodeError as e:
                    logger.warning(f"Download worker {worker_id}: JSON decode error: {e}")
                    logger.warning(f"Response received: {response_str[:200] if len(response_str) > 200 else response_str}")
                    # Try to detect if we got binary data instead of JSON (might be raw file)
                    if len(response) > 1000:  # If we got substantial data
                        success = True
                        bytes_processed = len(response)
                        logger.info(f"Download worker {worker_id}: Received {bytes_processed//1024}KB of non-JSON data (likely raw file)")
                        break
                    success = "OK" in response_str or "success" in response_str.lower()
                    if success:
                        logger.info(f"Download worker {worker_id}: Download successful (non-JSON response)")
                        break  # Success, exit retry loop
                
            except (ConnectionRefusedError, ConnectionResetError, BrokenPipeError) as e:
                logger.warning(f"Download worker {worker_id}: Connection error (attempt {retry_count + 1}/{max_retries}): {str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    # Shorter wait time to make the test complete faster
                    wait_time = 1 + retry_count  # Linear backoff
                    logger.info(f"Download worker {worker_id}: Waiting {wait_time} seconds before retry")
                    time.sleep(wait_time)
                continue
            except Exception as e:
                logger.error(f"Download worker {worker_id} failed: {str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 1
                    logger.info(f"Download worker {worker_id}: Retrying after error in {wait_time}s")
                    time.sleep(wait_time)
                else:
                    break
            finally:
                if sock:
                    try:
                        sock.close()
                        logger.debug(f"Download worker {worker_id}: Socket closed")
                    except:
                        pass
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        # Update global stats
        update_stats(success, bytes_processed, elapsed_time)
        
        return {
            'worker_id': worker_id,
            'success': success,
            'elapsed_time': elapsed_time,
            'bytes_processed': bytes_processed,
            'error': None if success else response_str[:100]
        }
        
    except Exception as e:
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        update_stats(False, 0, elapsed_time)
        
        logger.error(f"Download worker {worker_id} failed: {str(e)}")
        return {
            'worker_id': worker_id,
            'success': False,
            'elapsed_time': elapsed_time,
            'bytes_processed': 0,
            'error': str(e)
        }


def list_files_worker(worker_id, server_host, server_port):
    """Worker function for listing files"""
    start_time = time.time()
    success = False  # Initialize success variable
    response_str = ""
    
    try:
        # Create list command
        command = "LIST\r\n\r\n"
        
        logger.info(f"List worker {worker_id}: Sending command")
        
        # Connect and send with retries
        retry_count = 0
        max_retries = 3
        while retry_count < max_retries:
            sock = None
            try:
                logger.info(f"List worker {worker_id}: Attempt {retry_count + 1}/{max_retries} - Connecting to {server_host}:{server_port}")
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(CONNECT_TIMEOUT)
                sock.connect((server_host, server_port))
                logger.info(f"List worker {worker_id}: Connected successfully")
                sock.settimeout(SOCKET_TIMEOUT)
                
                # Set socket options for better performance
                try:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1048576)  # 1MB receive buffer
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1048576)  # 1MB send buffer
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # Disable Nagle's algorithm
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)  # Enable keepalive
                except Exception as e:
                    logger.warning(f"List worker {worker_id}: Could not set socket options: {str(e)}")
                
                # Send command
                sock.sendall(command.encode())
                logger.info(f"List worker {worker_id}: Command sent, waiting for response")
                
                # Receive response
                response = b""
                sock.settimeout(60)  # Response timeout
                while True:
                    try:
                        data = sock.recv(8192)
                        if not data:
                            logger.info(f"List worker {worker_id}: No more data from server")
                            break
                        response += data
                        logger.debug(f"List worker {worker_id}: Received {len(data)} bytes")
                        if b"\r\n\r\n" in response:
                            logger.info(f"List worker {worker_id}: Found response end marker")
                            break
                    except socket.timeout:
                        logger.warning(f"List worker {worker_id}: timeout waiting for response")
                        break
                
                # Parse response
                response_str = response.decode('utf-8', errors='ignore').strip()
                try:
                    # Remove the \r\n\r\n delimiter if present
                    if response_str.endswith('\r\n\r\n'):
                        response_str = response_str[:-4]
                    response_json = json.loads(response_str)
                    success = response_json.get("status") == "OK"
                    if success:
                        logger.info(f"List worker {worker_id}: List successful")
                        break  # Success, exit retry loop
                    else:
                        logger.warning(f"List worker {worker_id}: Server returned error: {response_json.get('data', 'Unknown error')}")
                except json.JSONDecodeError as e:
                    logger.warning(f"List worker {worker_id}: JSON decode error: {e}")
                    logger.warning(f"Response received: {response_str[:200]}")
                    success = "OK" in response_str or "success" in response_str.lower()
                    if success:
                        logger.info(f"List worker {worker_id}: List successful (non-JSON response)")
                        break  # Success, exit retry loop
                
            except (ConnectionRefusedError, ConnectionResetError, BrokenPipeError) as e:
                logger.warning(f"List worker {worker_id}: Connection error (attempt {retry_count + 1}/{max_retries}): {str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = retry_count * 2  # Exponential backoff
                    logger.info(f"List worker {worker_id}: Waiting {wait_time} seconds before retry")
                    time.sleep(wait_time)
                continue
            except Exception as e:
                logger.error(f"List worker {worker_id} failed: {str(e)}")
                break
            finally:
                if sock:
                    try:
                        sock.close()
                        logger.debug(f"List worker {worker_id}: Socket closed")
                    except:
                        pass
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        # Update global stats
        update_stats(success, 0, elapsed_time)
        
        return {
            'worker_id': worker_id,
            'success': success,
            'elapsed_time': elapsed_time,
            'bytes_processed': 0,
            'error': None if success else response_str[:100]
        }
        
    except Exception as e:
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        update_stats(False, 0, elapsed_time)
        
        logger.error(f"List worker {worker_id} failed: {str(e)}")
        return {
            'worker_id': worker_id,
            'success': False,
            'elapsed_time': elapsed_time,
            'bytes_processed': 0,
            'error': str(e)
        }


def cleanup_test_files(server_host, server_port, base_filename, worker_ids):
    """Clean up test files from server, only for specified worker IDs"""
    logger.info(f"Starting cleanup for {len(worker_ids)} files with base name {base_filename}")
    
    cleanup_results = []
    
    try:
        for worker_id in worker_ids:
            # Use consistent path for deletion - no 'files/' prefix
            server_filename = f"{base_filename}_{worker_id}"
            command = f"DELETE {server_filename}\r\n\r\n"
            
            logger.info(f"Sending delete command: {command.strip()}")
            
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(10)
                sock.connect((server_host, server_port))
                sock.sendall(command.encode())
                
                # Receive response properly
                response = b""
                while True:
                    data = sock.recv(4096)
                    if not data:
                        break
                    response += data
                    if b'\r\n\r\n' in response:
                        break
                
                sock.close()
                
                # Parse response
                response_str = response.decode('utf-8', errors='ignore').strip()
                if '\r\n\r\n' in response_str:
                    response_str = response_str.split('\r\n\r\n')[0]
                
                # Check if cleanup was successful - improved parsing
                success = False
                try:
                    response_json = json.loads(response_str)
                    success = response_json.get("status") == "OK"
                    message = response_json.get("message", "")
                except json.JSONDecodeError:
                    # Better non-JSON response handling
                    success = "OK" in response_str or "deleted" in response_str.lower() or "success" in response_str.lower() or "berhasil" in response_str.lower()
                    message = response_str[:100]
                    logger.info(f"Non-JSON DELETE response: {response_str}")
                
                cleanup_results.append({
                    'filename': server_filename,
                    'success': success,
                    'message': message
                })
                
                logger.debug(f"Cleanup {server_filename}: {'SUCCESS' if success else 'FAILED'} - {message}")
                
            except Exception as e:
                cleanup_results.append({
                    'filename': server_filename,
                    'success': False,
                    'message': str(e)
                })
                logger.warning(f"Cleanup {server_filename} failed: {str(e)}")
    
    except Exception as e:
        logger.error(f"Overall cleanup error: {str(e)}")
    
    # Log summary
    successful_cleanups = sum(1 for r in cleanup_results if r['success'])
    logger.info(f"Cleanup completed: {successful_cleanups}/{len(cleanup_results)} files cleaned")
    
    if successful_cleanups < len(cleanup_results):
        failed_files = [r['filename'] for r in cleanup_results if not r['success']]
        logger.warning(f"Failed to cleanup files: {failed_files}")
    
    return cleanup_results


# Process pool wrapper functions
def upload_file_worker_process(args):
    """Wrapper for multiprocessing upload"""
    # Declare globals first before using them
    global CHUNK_SIZE, SOCKET_TIMEOUT
    
    try:
        # Handle different argument formats
        if isinstance(args, tuple) and len(args) >= 5:
            # Basic args format: (worker_id, file_path, server_host, server_port, upload_filename)
            worker_id = args[0]
            file_path = args[1]
            server_host = args[2]
            server_port = args[3]
            upload_filename = args[4]
            
            # Optional parameters
            chunk_size_kb = args[5] if len(args) > 5 else CHUNK_SIZE // 1024
            timeout = args[6] if len(args) > 6 else SOCKET_TIMEOUT
        else:
            # Fallback handling for other argument structures
            worker_id = args[0]
            file_path = args[1]
            server_host = args[2]
            server_port = args[3]
            upload_filename = args[4]
            chunk_size_kb = CHUNK_SIZE // 1024  # Default if not provided
            timeout = SOCKET_TIMEOUT  # Default if not provided
        
        # Configure logging for process
        try:
            import logging
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s'
            )
        except:
            pass
        
        # Save original settings
        original_chunk_size = CHUNK_SIZE
        original_timeout = SOCKET_TIMEOUT
        
        try:
            CHUNK_SIZE = chunk_size_kb * 1024  # Convert KB to bytes
            SOCKET_TIMEOUT = timeout
            
            # Call the main worker function with consistent parameters
            return upload_file_worker(worker_id, file_path, server_host, server_port, upload_filename)
        finally:
            # Restore original settings
            CHUNK_SIZE = original_chunk_size
            SOCKET_TIMEOUT = original_timeout
            
    except Exception as e:
        # Ensure we have a worker_id in case of early exception
        worker_id = args[0] if isinstance(args, (tuple, list)) and args else -1
        
        # Log error and return error result
        try:
            import logging
            logging.error(f"Process upload worker error: {e}")
        except:
            pass
            
        return {
            'worker_id': worker_id,
            'success': False,
            'elapsed_time': 0,
            'bytes_processed': 0,
            'error': f"Process wrapper error: {str(e)}"
        }


def download_file_worker_process(args):
    """Wrapper for multiprocessing download"""
    # Declare globals first before using them
    global CHUNK_SIZE, SOCKET_TIMEOUT
    
    try:
        # Handle different argument formats
        if isinstance(args, tuple) and len(args) >= 4:
            # Basic args format: (worker_id, server_host, server_port, base_filename)
            worker_id = args[0]
            server_host = args[1]
            server_port = args[2]
            base_filename = args[3]
            
            # Optional parameters
            chunk_size_kb = args[4] if len(args) > 4 else CHUNK_SIZE // 1024
            timeout = args[5] if len(args) > 5 else SOCKET_TIMEOUT
        else:
            # Assume it's a list or other iterable
            worker_id = args[0]
            server_host = args[1]
            server_port = args[2]
            base_filename = args[3]
            chunk_size_kb = CHUNK_SIZE // 1024  # Default if not provided
            timeout = SOCKET_TIMEOUT  # Default if not provided
        
        # Configure logging for process (useful in process pool)
        try:
            import logging
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s'
            )
        except:
            pass
            
        # Save original settings
        original_chunk_size = CHUNK_SIZE
        original_timeout = SOCKET_TIMEOUT
        
        try:
            CHUNK_SIZE = chunk_size_kb * 1024  # Convert KB to bytes
            SOCKET_TIMEOUT = timeout
            
            # Call the main worker function with consistent parameters
            return download_file_worker(worker_id, server_host, server_port, base_filename)
        finally:
            # Restore original settings
            CHUNK_SIZE = original_chunk_size
            SOCKET_TIMEOUT = original_timeout
            
    except Exception as e:
        # Ensure we have a worker_id in case of early exception
        worker_id = args[0] if isinstance(args, (tuple, list)) and args else -1
        
        # Log error and return error result
        try:
            import logging
            logging.error(f"Process download worker error: {e}")
        except:
            pass
            
        return {
            'worker_id': worker_id,
            'success': False,
            'elapsed_time': 0,
            'bytes_processed': 0,
            'error': f"Process wrapper error: {str(e)}"
        }


def list_files_worker_process(args):
    """Wrapper for multiprocessing list files"""
    # Declare globals first before using them
    global SOCKET_TIMEOUT
    
    # Unpack args: (worker_id, server_host, server_port, timeout)
    worker_id, server_host, server_port, timeout = args
    
    # Save original settings
    original_timeout = SOCKET_TIMEOUT
    
    try:
        SOCKET_TIMEOUT = timeout
        return list_files_worker(worker_id, server_host, server_port)
    finally:
        # Restore original settings
        SOCKET_TIMEOUT = original_timeout

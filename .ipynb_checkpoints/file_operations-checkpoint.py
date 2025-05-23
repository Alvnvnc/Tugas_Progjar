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
    from stress_test.config import (
        logger, CONNECT_TIMEOUT, SOCKET_TIMEOUT, CHUNK_SIZE, 
        SERVER_HOST, SERVER_PORT, update_stats, stats_lock
    )


def upload_file_worker(worker_id, file_path, server_host, server_port, upload_filename):
    """Worker function for uploading files"""
    start_time = time.time()
    bytes_processed = 0
    
    try:
        # Read and encode file
        with open(file_path, 'rb') as f:
            file_content = f.read()
            bytes_processed = len(file_content)
            b64_content = base64.b64encode(file_content).decode()
        
        # Create upload command - store the actual name the server will use
        filename_on_server = f"{upload_filename}_{worker_id}"
        command = f"UPLOAD {filename_on_server} {b64_content}\r\n\r\n"
        
        logger.info(f"Upload worker {worker_id}: Sending command for file: {filename_on_server}")
        
        # Connect and send
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(CONNECT_TIMEOUT)
        sock.connect((server_host, server_port))
        sock.settimeout(SOCKET_TIMEOUT)
        
        # Send in chunks to avoid buffer issues
        command_bytes = command.encode()
        total_sent = 0
        
        while total_sent < len(command_bytes):
            chunk = command_bytes[total_sent:total_sent + CHUNK_SIZE]
            sock.sendall(chunk)
            total_sent += len(chunk)
        
        # Receive response
        response = b""
        sock.settimeout(60)  # Response timeout
        while True:
            try:
                data = sock.recv(8192)
                if not data:
                    break
                response += data
                if b"\r\n\r\n" in response:
                    break
            except socket.timeout:
                logger.warning(f"Upload worker {worker_id}: timeout waiting for response")
                break
        
        sock.close()
        
        # Parse response
        response_str = response.decode('utf-8', errors='ignore').strip()
        try:
            # Remove the \r\n\r\n delimiter if present
            if response_str.endswith('\r\n\r\n'):
                response_str = response_str[:-4]
            response_json = json.loads(response_str)
            success = response_json.get("status") == "OK"
        except json.JSONDecodeError as e:
            logger.warning(f"Upload worker {worker_id}: JSON decode error: {e}")
            logger.warning(f"Response received: {response_str[:200]}")
            success = "OK" in response_str or "success" in response_str.lower()
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
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


def download_file_worker(worker_id, server_host, server_port, filename):
    """Worker function for downloading files"""
    start_time = time.time()
    bytes_processed = 0
    
    # Define command formats to try
    commands_to_try = [
        f"GET {filename}_{worker_id}\r\n\r\n",             # Format 1: tanpa prefix 'files/'
        f"GET files/{filename}_{worker_id}\r\n\r\n",       # Format 2: dengan prefix 'files/'
        f"get {filename}_{worker_id}\r\n\r\n",             # Format 3: lowercase
    ]
    
    success = False
    response_str = ""
    
    for attempt, command in enumerate(commands_to_try, 1):
        if success:
            break
            
        logger.info(f"Download worker {worker_id}: Attempt {attempt} - Sending command: {command.strip()}")
        
        try:
            # Connect and send
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(CONNECT_TIMEOUT)
            sock.connect((server_host, server_port))
            logger.info(f"Download worker {worker_id}: Connected to server {server_host}:{server_port}")
            sock.settimeout(SOCKET_TIMEOUT)
            
            # Send command
            sock.sendall(command.encode())
            
            # Receive response
            response = b""
            buffer_size = 8192
            
            while True:
                try:
                    data = sock.recv(buffer_size)
                    if not data:
                        logger.info(f"Download worker {worker_id}: No more data, connection closed by server")
                        break
                    response += data
                    logger.info(f"Download worker {worker_id}: Received chunk of {len(data)} bytes")
                    
                    # For large responses, increase buffer size
                    if len(response) > 1024 * 1024:
                        buffer_size = CHUNK_SIZE
                    
                    # Check for end markers
                    if b'\r\n\r\n' in response:
                        logger.info(f"Download worker {worker_id}: Found end marker")
                        break
                        
                except socket.timeout:
                    logger.warning(f"Download worker {worker_id}: timeout waiting for data")
                    break
            
            sock.close()
            logger.info(f"Download worker {worker_id}: Connection closed after attempt {attempt}")
            
            # Parse response
            response_str = response.decode('utf-8', errors='ignore')
            logger.info(f"Download worker {worker_id} raw response (first 500 chars): {response_str[:500]}")
            
            # Remove delimiter
            if '\r\n\r\n' in response_str:
                response_str = response_str.split('\r\n\r\n')[0]
            
            response_str = response_str.strip()
            
            # Try JSON parsing first
            try:
                response_json = json.loads(response_str)
                logger.info(f"Download worker {worker_id}: Successfully parsed response as JSON with keys: {list(response_json.keys())}")
                
                if response_json.get("status") == "OK":
                    # Try data_file field first (based on server implementation)
                    if "data_file" in response_json:
                        file_data = response_json["data_file"]
                        logger.info(f"Download worker {worker_id}: Found data in data_file field")
                    # Then try data field
                    elif "data" in response_json:
                        file_data = response_json["data"]
                        logger.info(f"Download worker {worker_id}: Found data in data field")
                    else:
                        logger.warning(f"Download worker {worker_id}: JSON response OK but no data field found")
                        continue
                    
                    try:
                        decoded_data = base64.b64decode(file_data)
                        bytes_processed = len(decoded_data)
                        logger.info(f"Download worker {worker_id}: Successfully decoded {bytes_processed} bytes")
                        success = True
                        break  # Found successful command format
                    except Exception as decode_error:
                        logger.error(f"Download worker {worker_id}: Base64 decode error: {str(decode_error)}")
                else:
                    error_msg = response_json.get("data", "Unknown error")
                    logger.warning(f"Download worker {worker_id}: Server error: {error_msg}")
                    if "not found" in error_msg.lower() or "no such file" in error_msg.lower():
                        logger.warning(f"Download worker {worker_id}: File not found, trying next format")
                        continue
            except json.JSONDecodeError:
                logger.warning(f"Download worker {worker_id}: Could not parse as JSON: {response_str[:200]}")
                # Try simple keyword detection as fallback
                if "OK" in response_str or "Success" in response_str or "success" in response_str.lower():
                    logger.info(f"Download worker {worker_id}: Non-JSON response but seems successful")
                    bytes_processed = len(response_str)
                    success = True
                    break
    
        except Exception as e:
            logger.error(f"Download worker {worker_id}: Error in attempt {attempt}: {str(e)}")
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    # Update global stats
    update_stats(success, bytes_processed, elapsed_time)
    
    return {
        'worker_id': worker_id,
        'success': success,
        'elapsed_time': elapsed_time,
        'bytes_processed': bytes_processed,
        'error': None if success else f"All download attempts failed: {response_str[:200]}"
    }


def list_files_worker(worker_id, server_host, server_port):
    """Worker function for listing files on the server"""
    start_time = time.time()
    
    try:
        # Connect to server
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(CONNECT_TIMEOUT)
        sock.connect((server_host, server_port))
        logger.info(f"List worker {worker_id}: Connected to server {server_host}:{server_port}")
        
        # Send list command
        command = "LIST\r\n\r\n"
        sock.sendall(command.encode())
        
        # Receive response
        response = b""
        sock.settimeout(30)  # List should be quick
        
        while True:
            try:
                data = sock.recv(8192)
                if not data:
                    break
                response += data
                if b"\r\n\r\n" in response:
                    break
            except socket.timeout:
                logger.warning(f"List worker {worker_id}: timeout waiting for response")
                break
        
        sock.close()
        
        # Parse response
        response_str = response.decode('utf-8', errors='ignore').strip()
        try:
            if response_str.endswith('\r\n\r\n'):
                response_str = response_str[:-4]
            
            response_json = json.loads(response_str)
            success = response_json.get("status") == "OK"
            
            if success and "data" in response_json:
                files = response_json["data"]
                if isinstance(files, list):
                    logger.info(f"List worker {worker_id}: Found {len(files)} files")
                    logger.debug(f"Files: {files[:10]}{'...' if len(files) > 10 else ''}")
                else:
                    logger.warning(f"List worker {worker_id}: data field is not a list")
                    success = False
            else:
                logger.warning(f"List worker {worker_id}: Invalid response format or status not OK")
                success = False
                
        except json.JSONDecodeError as e:
            logger.warning(f"List worker {worker_id}: JSON parse error: {e}")
            logger.warning(f"Response received: {response_str[:200]}")
            success = False
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        # Update global stats
        update_stats(success, 0, elapsed_time)  # No bytes processed for listing
        
        return {
            'worker_id': worker_id,
            'success': success,
            'elapsed_time': elapsed_time,
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
    return upload_file_worker(*args)


def download_file_worker_process(args):
    """Wrapper for multiprocessing download"""
    return download_file_worker(*args)


def list_files_worker_process(args):
    """Wrapper for multiprocessing list"""
    return list_files_worker(*args)

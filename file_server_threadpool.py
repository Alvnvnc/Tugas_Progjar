from socket import *
import socket
import threading
import logging
import time
import sys
import signal
import gc
import os
from concurrent.futures import ThreadPoolExecutor
import base64
from io import BytesIO

from file_protocol import FileProtocol
fp = FileProtocol()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Maximum number of workers in the thread pool
# Can be overridden via command line
DEFAULT_THREAD_WORKERS = 50
MAX_THREAD_WORKERS = DEFAULT_THREAD_WORKERS

# Timeout settings - optimized for Docker environment
STANDARD_TIMEOUT = 300   # 5 minutes for standard operations
UPLOAD_TIMEOUT = 1200    # 20 minutes for file uploads
SOCKET_BUFFER_SIZE = 32768  # 32KB - reduced for better memory management

# Thread-safe statistics
stats_lock = threading.Lock()
stats = {
    'total_requests': 0,
    'active_connections': 0,
    'request_times': {
        'LIST': [],
        'GET': [],
        'UPLOAD': [],
        'DELETE': []
    }
}

# Set up a signal handler for graceful shutdown
def signal_handler(sig, frame):
    logging.warning('Received signal to terminate. Shutting down server...')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def update_stats(stat_name, value=1):
    """Thread-safe stats update"""
    with stats_lock:
        if stat_name in stats:
            stats[stat_name] += value

def add_request_time(command_type, duration):
    """Thread-safe request time tracking"""
    with stats_lock:
        if command_type in stats['request_times']:
            stats['request_times'][command_type].append(duration)
            # Limit history to last 50 requests for memory efficiency
            if len(stats['request_times'][command_type]) > 50:
                stats['request_times'][command_type].pop(0)

def handle_streaming_upload(connection, address, command_parts):
    """Handle file uploads with streaming approach to prevent memory issues"""
    temp_filepath = None
    try:
        if len(command_parts) < 2:
            logging.error(f"Invalid UPLOAD command from {address}")
            return '{"status": "ERROR", "data": "Invalid upload command format"}'
            
        # Extract the filename from the command
        filename = command_parts[1].strip()
        if not filename:
            return '{"status": "ERROR", "data": "Filename required for upload"}'
            
        # Create files directory if it doesn't exist
        os.makedirs('files', exist_ok=True)
            
        filepath = os.path.join('files', filename)
        temp_filepath = filepath + ".tmp"
        
        # Parse beginning of content if available
        content_start = ''
        if len(command_parts) > 2:
            content_start = command_parts[2]
        
        logging.info(f"Starting streaming upload for {filename} from {address}")
        
        # Initialize variables for tracking
        bytes_received = len(content_start.encode()) if content_start else 0
        last_report_time = time.time()
        base64_buffer = content_start  # Start with any initial content
        
        # Create temporary file for writing
        with open(temp_filepath, 'wb') as temp_file:
            
            # Process any initial content
            if content_start:
                try:
                    # Process complete base64 chunks immediately
                    complete_chunks = (len(base64_buffer) // 4) * 4
                    if complete_chunks >= 4:
                        complete_b64 = base64_buffer[:complete_chunks]
                        decoded_data = base64.b64decode(complete_b64)
                        temp_file.write(decoded_data)
                        base64_buffer = base64_buffer[complete_chunks:]  # Keep remainder
                except Exception as e:
                    logging.error(f"Error processing initial upload data: {str(e)}")
            
            # Continue receiving data in chunks
            end_marker_found = False
            while not end_marker_found:
                try:
                    # Receive data chunk
                    chunk = connection.recv(SOCKET_BUFFER_SIZE)
                    if not chunk:
                        logging.warning(f"No more data received from {address}")
                        break
                    
                    # Decode chunk and add to buffer
                    try:
                        chunk_str = chunk.decode('utf-8', errors='ignore')
                    except UnicodeDecodeError:
                        chunk_str = chunk.decode('latin-1', errors='ignore')
                    
                    bytes_received += len(chunk)
                    
                    # Check for end markers
                    if '\r\n\r\n' in chunk_str or '\n\n' in chunk_str:
                        # Found end marker, extract the content before it
                        if '\r\n\r\n' in chunk_str:
                            content_part = chunk_str.split('\r\n\r\n')[0]
                        else:
                            content_part = chunk_str.split('\n\n')[0]
                        
                        base64_buffer += content_part
                        end_marker_found = True
                    else:
                        base64_buffer += chunk_str
                    
                    # Process buffer when it gets large enough or at the end
                    if len(base64_buffer) >= 65536 or end_marker_found:  # Process every 64KB or at end
                        try:
                            # Calculate complete base64 chunks (multiple of 4 characters)
                            complete_chunks = (len(base64_buffer) // 4) * 4
                            
                            if complete_chunks >= 4:
                                # Process complete chunks
                                complete_b64 = base64_buffer[:complete_chunks]
                                decoded_data = base64.b64decode(complete_b64)
                                temp_file.write(decoded_data)
                                
                                # Keep remainder for next iteration (unless we're at the end)
                                if not end_marker_found:
                                    base64_buffer = base64_buffer[complete_chunks:]
                                else:
                                    # At end, try to process any remaining data
                                    remainder = base64_buffer[complete_chunks:]
                                    if remainder and len(remainder) > 0:
                                        # Pad if necessary for base64 decoding
                                        while len(remainder) % 4 != 0:
                                            remainder += '='
                                        try:
                                            final_decoded = base64.b64decode(remainder)
                                            temp_file.write(final_decoded)
                                        except Exception as e:
                                            logging.warning(f"Could not decode final chunk: {str(e)}")
                                    base64_buffer = ""
                                    
                                # Force garbage collection periodically
                                if bytes_received % (1024 * 1024) == 0:  # Every MB
                                    gc.collect()
                            
                        except Exception as e:
                            logging.error(f"Error decoding base64 data: {str(e)}")
                            return f'{{"status": "ERROR", "data": "Base64 decode error: {str(e)}"}}'
                    
                    # Log progress periodically
                    current_time = time.time()
                    if current_time - last_report_time > 10:  # Every 10 seconds
                        mb_received = bytes_received / 1024 / 1024
                        logging.info(f"Upload progress for {filename}: {mb_received:.2f} MB received")
                        last_report_time = current_time
                        
                except socket.timeout:
                    logging.error(f"Upload timeout for {filename} from {address}")
                    return '{"status": "ERROR", "data": "Upload timeout"}'
                except Exception as e:
                    logging.error(f"Error during upload: {str(e)}")
                    return f'{{"status": "ERROR", "data": "Upload failed: {str(e)}"}}'
        
        # Rename temporary file to final filename
        if os.path.exists(temp_filepath):
            os.rename(temp_filepath, filepath)
            file_size = os.path.getsize(filepath) / 1024 / 1024  # Size in MB
            logging.info(f"Upload completed successfully: {filename} ({file_size:.2f} MB)")
            
            # Force garbage collection after upload
            gc.collect()
            
            return f'{{"status": "OK", "data": "File uploaded successfully: {filename} ({file_size:.2f} MB)"}}'
        else:
            return '{"status": "ERROR", "data": "Upload failed - temporary file not created"}'
        
    except Exception as e:
        logging.error(f"Critical error in upload handler: {str(e)}")
        # Clean up temp file if it exists
        if temp_filepath and os.path.exists(temp_filepath):
            try:
                os.remove(temp_filepath)
                logging.info(f"Cleaned up temporary file: {temp_filepath}")
            except Exception as cleanup_error:
                logging.error(f"Could not clean up temp file: {cleanup_error}")
        return f'{{"status": "ERROR", "data": "Upload failed: {str(e)}"}}'

def handle_client(connection, address):
    update_stats('active_connections', 1)
    start_time = time.time()
    thread_id = threading.current_thread().name
    logging.info(f"[{thread_id}] Handling client from {address} (Active connections: {stats['active_connections']})")
    
    try:
        # Set initial timeout
        connection.settimeout(STANDARD_TIMEOUT)
        
        # Set socket options for better performance
        connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # Disable Nagle's algorithm
        
        while True:
            try:
                # Receive initial command data
                data = connection.recv(8192)  # Increased buffer for better performance
                if not data:
                    logging.info(f"[{thread_id}] No data received from {address}, closing connection")
                    break
                    
                # Decode the command
                try:
                    command_string = data.decode('utf-8').strip()
                except UnicodeDecodeError:
                    try:
                        command_string = data.decode('latin-1').strip()
                    except UnicodeDecodeError:
                        logging.error(f"[{thread_id}] Cannot decode data from {address}")
                        error_response = '{"status": "ERROR", "data": "Invalid character encoding"}\r\n\r\n'
                        connection.sendall(error_response.encode())
                        continue
                
                if not command_string:
                    continue
                
                # Parse command
                command_parts = command_string.split(' ', 2)
                command_type = command_parts[0].upper()
                
                # Track request start time
                cmd_start_time = time.time()
                update_stats('total_requests', 1)
                
                logging.info(f"[{thread_id}] Processing {command_type} command from {address}")
                
                # Handle different command types
                if command_type == 'UPLOAD':
                    # Set longer timeout for uploads
                    connection.settimeout(UPLOAD_TIMEOUT)
                    logging.info(f"[{thread_id}] Upload request, extended timeout to {UPLOAD_TIMEOUT}s")
                    
                    # Handle upload with streaming approach
                    result = handle_streaming_upload(connection, address, command_parts)
                    
                elif command_type == 'DELETE':
                    # Validate delete command
                    if len(command_parts) < 2 or not command_parts[1].strip():
                        result = '{"status": "ERROR", "data": "Filename required for DELETE command"}'
                    else:
                        # Process delete command
                        result = fp.proses_string(command_string)
                        
                elif command_type in ['LIST', 'GET']:
                    # Process standard commands
                    result = fp.proses_string(command_string)
                    
                else:
                    # Unknown command
                    result = f'{{"status": "ERROR", "data": "Unknown command: {command_type}"}}'
                
                # Ensure result is a string
                if not isinstance(result, str):
                    result = str(result)
                
                # Add response terminators if not present
                if not result.endswith('\r\n\r\n'):
                    result += '\r\n\r\n'
                
                # Send response
                try:
                    connection.sendall(result.encode('utf-8'))
                except socket.error as e:
                    logging.error(f"[{thread_id}] Error sending response to {address}: {str(e)}")
                    break
                
                # Track command performance
                cmd_end_time = time.time()
                cmd_duration = cmd_end_time - cmd_start_time
                add_request_time(command_type, cmd_duration)
                
                logging.info(f"[{thread_id}] Completed {command_type} in {cmd_duration:.4f} seconds")
                
                # Reset timeout for next command
                connection.settimeout(STANDARD_TIMEOUT)
                
                # Periodic garbage collection
                if stats['total_requests'] % 10 == 0:
                    gc.collect()
                
            except socket.timeout:
                logging.warning(f"[{thread_id}] Socket timeout for {address}")
                break
            except ConnectionAbortedError:
                logging.info(f"[{thread_id}] Connection aborted by {address}")
                break
            except Exception as e:
                logging.error(f"[{thread_id}] Error processing request from {address}: {str(e)}")
                try:
                    error_response = f'{{"status": "ERROR", "data": "Server error: {str(e)}"}}\r\n\r\n'
                    connection.sendall(error_response.encode())
                except:
                    pass  # Connection might be closed
                break
    
    except Exception as e:
        logging.error(f"[{thread_id}] Critical error handling client {address}: {str(e)}")
    finally:
        update_stats('active_connections', -1)
        total_time = time.time() - start_time
        try:
            connection.close()
        except:
            pass
        logging.info(f"[{thread_id}] Connection from {address} closed after {total_time:.2f} seconds (Active: {stats['active_connections']})")
        
        # Force garbage collection after client disconnects
        gc.collect()

class Server(threading.Thread):
    def __init__(self, ipaddress='0.0.0.0', port=8889):
        self.ipinfo = (ipaddress, port)
        self.running = True
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Optimize socket buffer sizes for Docker environment
        try:
            self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1048576)  # 1MB receive buffer
            self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1048576)  # 1MB send buffer
            logging.info("Socket buffers set to 1MB each")
        except Exception as e:
            logging.warning(f"Could not set socket buffer sizes: {str(e)}")
        
        # Configure TCP settings
        try:
            self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            logging.info("Socket keep-alive enabled")
        except Exception as e:
            logging.warning(f"Could not enable keep-alive: {str(e)}")
        
        # Create thread pool with better configuration
        self.thread_pool = ThreadPoolExecutor(
            max_workers=MAX_THREAD_WORKERS, 
            thread_name_prefix="FileServer"
        )
        threading.Thread.__init__(self)

    def run(self):
        logging.warning(f"Server starting on {self.ipinfo}")
        logging.warning(f"Thread Pool Server - using {MAX_THREAD_WORKERS} worker threads")
        logging.warning(f"Timeouts: Standard={STANDARD_TIMEOUT}s, Upload={UPLOAD_TIMEOUT}s")
        
        try:
            self.my_socket.bind(self.ipinfo)
            self.my_socket.listen(20)  # Increased backlog for Docker environment
            self.my_socket.settimeout(2)  # Check for shutdown every 2 seconds
            
            # Start monitoring thread
            monitor = threading.Thread(target=self.monitor_server, name="ServerMonitor")
            monitor.daemon = True
            monitor.start()
            
            logging.info("Server ready to accept connections")
            
            while self.running:
                try:
                    connection, client_address = self.my_socket.accept()
                    logging.info(f"New connection from {client_address}")
                    
                    # Submit to thread pool
                    future = self.thread_pool.submit(handle_client, connection, client_address)
                    
                    # Optional: You can add error handling for the future if needed
                    # future.add_done_callback(lambda f: logging.error(f"Task error: {f.exception()}") if f.exception() else None)
                    
                except socket.timeout:
                    continue  # Check if server should keep running
                except Exception as e:
                    if self.running:
                        logging.error(f"Error accepting connection: {str(e)}")
                        time.sleep(0.1)  # Brief pause before retrying
        except Exception as e:
            logging.error(f"Fatal error starting server: {str(e)}")
        finally:
            self.shutdown()
    
    def monitor_server(self):
        """Monitor server performance and log statistics"""
        while self.running:
            time.sleep(30)  # Log stats every 30 seconds
            
            with stats_lock:
                active_connections = stats['active_connections']
                total_requests = stats['total_requests']
                
                # Calculate average request times
                avg_times = {}
                for cmd, times in stats['request_times'].items():
                    if times:
                        avg_times[cmd] = sum(times) / len(times)
            
            logging.info(f"Server Monitor - Active: {active_connections}, Total requests: {total_requests}")
            if any(t > 0 for t in avg_times.values()):
                avg_str = ', '.join([f'{cmd}: {t:.3f}s' for cmd, t in avg_times.items() if t > 0])
                logging.info(f"Average response times: {avg_str}")
    
    def shutdown(self):
        """Gracefully shut down the server"""
        logging.warning("Initiating server shutdown...")
        self.running = False
        
        # Shutdown thread pool
        logging.info("Shutting down thread pool...")
        self.thread_pool.shutdown(wait=True, timeout=30)
        
        # Close server socket
        try:
            self.my_socket.close()
        except:
            pass
            
        logging.warning("Server shutdown complete")


def main():
    # Parse command line arguments for worker count
    import argparse
    parser = argparse.ArgumentParser(description='File Server with Thread Pool')
    parser.add_argument('--workers', type=int, default=DEFAULT_THREAD_WORKERS,
                       help=f'Number of worker threads (default: {DEFAULT_THREAD_WORKERS})')
    parser.add_argument('--port', type=int, default=6665,
                       help='Port to listen on (default: 6665)')
    args = parser.parse_args()
    
    # Set worker count
    global MAX_THREAD_WORKERS
    MAX_THREAD_WORKERS = args.workers
    
    # Ensure the files directory exists
    os.makedirs('files', exist_ok=True)
    logging.info("Files directory ready")
    
    # Log system info
    logging.info(f"Python GC thresholds: {gc.get_threshold()}")
    logging.info(f"Thread pool size: {MAX_THREAD_WORKERS}")
    
    try:
        svr = Server(ipaddress='0.0.0.0', port=args.port)
        svr.daemon = True
        svr.start()
        
        logging.info("Server started successfully. Press Ctrl+C to stop.")
        
        # Keep main thread running with periodic status checks
        try:
            while True:
                time.sleep(5)
                # Periodic garbage collection in main thread
                if stats['total_requests'] % 100 == 0 and stats['total_requests'] > 0:
                    collected = gc.collect()
                    if collected > 0:
                        logging.info(f"Garbage collected {collected} objects")
        except KeyboardInterrupt:
            logging.warning("Received keyboard interrupt")
        finally:
            svr.shutdown()
            
    except Exception as e:
        logging.error(f"Failed to start server: {str(e)}")

if __name__ == "__main__":
    main()
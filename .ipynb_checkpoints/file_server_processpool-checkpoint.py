from socket import *
import socket
import threading
import logging
import time
import sys
import signal
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import os
import pickle

from file_protocol import FileProtocol

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Maximum number of workers in the process pool
MAX_PROCESS_WORKERS = 3  # Reduced from 5 for better resource management

# Shared statistics using multiprocessing Manager
class SharedStats:
    def __init__(self):
        self.manager = multiprocessing.Manager()
        self.total_requests = self.manager.Value('i', 0)
        self.active_connections = self.manager.Value('i', 0)
        self.lock = self.manager.Lock()
    
    def increment_requests(self):
        with self.lock:
            self.total_requests.value += 1
    
    def increment_connections(self):
        with self.lock:
            self.active_connections.value += 1
    
    def decrement_connections(self):
        with self.lock:
            self.active_connections.value -= 1
    
    def get_stats(self):
        with self.lock:
            return self.total_requests.value, self.active_connections.value

# Global stats object
shared_stats = None

# Set up a signal handler for graceful shutdown
def signal_handler(sig, frame):
    logging.warning('Received signal to terminate. Shutting down server...')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def handle_client_process(client_socket_info):
    """Handle client in a separate process"""
    try:
        # Recreate socket from socket info
        client_socket = socket.fromfd(client_socket_info['fd'], 
                                    client_socket_info['family'], 
                                    client_socket_info['type'])
        address = client_socket_info['address']
        
        # Initialize FileProtocol in this process
        fp = FileProtocol()
        
        # Update connection count (if shared_stats is available)
        start_time = time.time()
        
        try:
            client_socket.settimeout(60)  # Increased timeout to 60 seconds
            logging.info(f"Process {os.getpid()} handling client from {address}")
            
            # Buffer for accumulating data
            buffer = b""
            
            while True:
                try:
                    # Receive data in chunks
                    chunk = client_socket.recv(4096)  # Reduced chunk size for better memory management
                    if not chunk:
                        break
                    
                    buffer += chunk
                    
                    # Convert to string for processing
                    try:
                        data_str = buffer.decode('utf-8')
                    except UnicodeDecodeError:
                        # If we can't decode, continue receiving
                        continue
                    
                    # Check if we have a complete command
                    if '\n' in data_str or '\r' in data_str:
                        # Process the command
                        command = data_str.strip()
                        
                        # Performance tracking
                        cmd_start_time = time.time()
                        command_type = command.split(' ', 1)[0].upper() if ' ' in command else command.upper()
                        
                        # Handle special cases for upload and delete
                        if command.lower().startswith('upload'):
                            # For upload, we might need more data
                            command_parts = command.split(' ', 2)
                            if len(command_parts) >= 3:
                                # We have command, filename, and content
                                pass
                            elif len(command_parts) == 2:
                                # We need to wait for content
                                while '\n' not in data_str and '\r' not in data_str:
                                    more_chunk = client_socket.recv(4096)
                                    if not more_chunk:
                                        break
                                    buffer += more_chunk
                                    try:
                                        data_str = buffer.decode('utf-8')
                                        command = data_str.strip()
                                    except UnicodeDecodeError:
                                        continue
                        
                        elif command.lower().startswith('delete'):
                            command_parts = command.split(' ', 1)
                            if len(command_parts) < 2 or not command_parts[1].strip():
                                # Send error response
                                error_response = '{"status": "ERROR", "data": "Nama file diperlukan untuk delete"}\r\n\r\n'
                                client_socket.sendall(error_response.encode())
                                buffer = b""  # Clear buffer
                                continue
                        
                        # Process the command
                        try:
                            hasil = fp.proses_string(command)
                            response = hasil + "\r\n\r\n"
                            client_socket.sendall(response.encode())
                            
                            # Log performance
                            cmd_end_time = time.time()
                            cmd_duration = cmd_end_time - cmd_start_time
                            logging.info(f"Process {os.getpid()} handled {command_type} in {cmd_duration:.4f} seconds")
                            
                        except Exception as e:
                            error_response = f'{{"status": "ERROR", "data": "Server error: {str(e)}"}}\r\n\r\n'
                            client_socket.sendall(error_response.encode())
                            logging.error(f"Error processing command in process {os.getpid()}: {str(e)}")
                        
                        # Clear buffer after processing
                        buffer = b""
                    
                    # Prevent buffer from growing too large
                    if len(buffer) > 1024 * 1024:  # 1MB limit
                        logging.warning(f"Buffer size exceeded limit, clearing buffer")
                        buffer = b""
                        error_response = '{"status": "ERROR", "data": "Request too large"}\r\n\r\n'
                        client_socket.sendall(error_response.encode())
                        
                except socket.timeout:
                    logging.warning(f"Connection from {address} timed out in process {os.getpid()}")
                    break
                except Exception as e:
                    logging.error(f"Error receiving data from {address} in process {os.getpid()}: {str(e)}")
                    break
                    
        except Exception as e:
            logging.error(f"Error handling client {address} in process {os.getpid()}: {str(e)}")
        finally:
            total_time = time.time() - start_time
            client_socket.close()
            logging.info(f"Connection from {address} closed after {total_time:.2f} seconds in process {os.getpid()}")
    except Exception as e:
        logging.error(f"Critical error in handle_client_process: {str(e)}")

class Server(threading.Thread):
    def __init__(self, ipaddress='0.0.0.0', port=8889):
        self.ipinfo = (ipaddress, port)
        self.running = True
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        threading.Thread.__init__(self)
        self.process_pool = None

    def run(self):
        global shared_stats
        
        logging.warning(f"Server starting on {self.ipinfo}")
        logging.warning(f"Process Pool Server - using up to {MAX_PROCESS_WORKERS} worker processes")
        
        try:
            self.my_socket.bind(self.ipinfo)
            self.my_socket.listen(10)  # Increased backlog
            self.my_socket.settimeout(1)  # Allow interrupting the accept call
            
            # Initialize shared stats
            shared_stats = SharedStats()
            
            # Start a monitoring thread
            monitor = threading.Thread(target=self.monitor_server)
            monitor.daemon = True
            monitor.start()
            
            # Create the process pool
            with ProcessPoolExecutor(max_workers=MAX_PROCESS_WORKERS) as process_pool:
                self.process_pool = process_pool
                logging.info(f"Process pool created with {MAX_PROCESS_WORKERS} workers")
                
                while self.running:
                    try:
                        connection, client_address = self.my_socket.accept()
                        logging.info(f"New connection from {client_address}")
                        
                        # Create socket info that can be pickled
                        socket_info = {
                            'fd': connection.fileno(),
                            'family': connection.family,
                            'type': connection.type,
                            'address': client_address
                        }
                        
                        # Submit to process pool
                        future = process_pool.submit(handle_client_process, socket_info)
                        
                        # Update stats
                        if shared_stats:
                            shared_stats.increment_connections()
                            shared_stats.increment_requests()
                        
                        # Don't close the connection here - let the worker process handle it
                        
                    except socket.timeout:
                        continue
                    except Exception as e:
                        if self.running:
                            logging.error(f"Error accepting connection: {str(e)}")
        except Exception as e:
            logging.error(f"Error starting server: {str(e)}")
        finally:
            self.shutdown()
    
    def monitor_server(self):
        """Monitor server performance and log statistics"""
        while self.running:
            time.sleep(30)  # Log stats every 30 seconds
            if shared_stats:
                total_req, active_conn = shared_stats.get_stats()
                logging.info(f"Server stats - PID: {os.getpid()}, Total requests: {total_req}, Active connections: {active_conn}")
            else:
                logging.info(f"Server running. Process ID: {os.getpid()}")
    
    def shutdown(self):
        """Gracefully shut down the server"""
        self.running = False
        logging.warning("Shutting down server...")
        if self.my_socket:
            self.my_socket.close()
        logging.warning("Server shutdown complete")


def main():
    # Ensure the files directory exists
    os.makedirs('files', exist_ok=True)
    logging.info("Ensured files directory exists")
    
    try:
        # Set multiprocessing start method
        if hasattr(multiprocessing, 'set_start_method'):
            try:
                multiprocessing.set_start_method('fork')  # Use fork method for better performance
                logging.info("Using 'fork' multiprocessing method")
            except RuntimeError:
                logging.info("Multiprocessing start method already set")
        
        svr = Server(ipaddress='0.0.0.0', port=6665)
        svr.daemon = True
        svr.start()
        
        logging.info("Server started successfully. Press Ctrl+C to stop.")
        
        # Keep the main thread running
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.warning("Received keyboard interrupt, shutting down...")
            svr.shutdown()
    except Exception as e:
        logging.error(f"Failed to start server: {str(e)}")

if __name__ == "__main__":
    main()
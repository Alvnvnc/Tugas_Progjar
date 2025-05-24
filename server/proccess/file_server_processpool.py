import socket
import multiprocessing
import logging
import time
import sys
import os
import base64
from multiprocessing import Pool, Manager, shared_memory
import signal
from collections import defaultdict
import threading

from file_protocol import FileProtocol

# Configure logging untuk memori besar
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

# Global memory optimization settings for ETS requirements (larger files)
CHUNK_SIZE = 1048576  # 1MB chunks for larger files
MAX_MEMORY_USAGE = 50 * 1024 * 1024 * 1024  # 50GB
SOCKET_BUFFER_SIZE = 32 * 1024 * 1024  # 32MB socket buffers for larger files
PROCESS_BUFFER_SIZE = 256 * 1024 * 1024  # 256MB per process buffer for 100MB files
MAX_CONCURRENT_CONNECTIONS = 1000  # Support hingga 1000 koneksi bersamaan

class MemoryOptimizedCache:
    """Cache yang dioptimalkan untuk memori besar dengan chunk 1MB for ETS"""
    
    def __init__(self, max_size_gb=35):  # Use 35GB from 50GB for cache (reserve more for larger files)
        self.max_size = max_size_gb * 1024 * 1024 * 1024
        self.cache = {}
        self.access_times = {}
        self.current_size = 0
        self.lock = threading.RLock()
        self.chunk_size = CHUNK_SIZE
        logging.warning(f"ETS MemoryOptimizedCache initialized with {max_size_gb}GB capacity for large files")
    
    def _evict_lru(self, needed_size):
        """Evict least recently used items untuk membuat ruang"""
        if not self.cache:
            return
        
        # Sort by access time
        sorted_items = sorted(self.access_times.items(), key=lambda x: x[1])
        freed_size = 0
        
        for key, _ in sorted_items:
            if freed_size >= needed_size:
                break
            
            if key in self.cache:
                item_size = len(self.cache[key])
                del self.cache[key]
                del self.access_times[key]
                self.current_size -= item_size
                freed_size += item_size
                logging.info(f"Evicted {key} ({item_size} bytes)")
    
    def store(self, key, data):
        """Store data dengan optimasi memori besar"""
        with self.lock:
            data_size = len(data)
            
            # Check if we need to evict
            if self.current_size + data_size > self.max_size:
                self._evict_lru(data_size)
            
            # Store in cache
            if key in self.cache:
                self.current_size -= len(self.cache[key])
            
            self.cache[key] = data
            self.access_times[key] = time.time()
            self.current_size += data_size
            
            logging.info(f"Cached {key} ({data_size} bytes). Total cache: {self.current_size // (1024*1024)}MB")
    
    def retrieve(self, key):
        """Retrieve data dari cache"""
        with self.lock:
            if key in self.cache:
                self.access_times[key] = time.time()
                return self.cache[key]
            return None
    
    def delete(self, key):
        """Delete dari cache"""
        with self.lock:
            if key in self.cache:
                self.current_size -= len(self.cache[key])
                del self.cache[key]
                del self.access_times[key]
                return True
            return False

def receive_complete_message_memory_optimized(client_socket):
    """Memory-optimized receive dengan buffer besar untuk ETS files (up to 100MB)"""
    try:
        # Set optimized timeouts untuk memori besar dan file besar
        client_socket.settimeout(300.0)  # Increased timeout to 5 minutes for large files
        
        # Pre-allocate buffer untuk performa maksimal dengan file besar
        buffer = bytearray(SOCKET_BUFFER_SIZE)  # 32MB buffer for large files
        message_parts = []
        total_received = 0
        
        # Receive data until we get the end marker
        while True:
            try:
                received = client_socket.recv_into(buffer)
                if not received:
                    break
                    
                # Use bytes() conversion
                chunk = bytes(buffer[:received]).decode()
                message_parts.append(chunk)
                total_received += received
                
                # Check for end marker
                complete_data = ''.join(message_parts)
                if complete_data.endswith('\r\n\r\n'):
                    break
                    
                # Progress logging for large files
                if total_received > 10 * 1024 * 1024:  # Log every 10MB
                    logging.info(f"ETS large file receive progress: {total_received // (1024*1024)}MB")
                    
            except socket.timeout:
                logging.error("Error in ETS memory-optimized receive: timed out")
                break
        
        if not message_parts:
            return None
        
        # Combine all parts and remove end marker
        complete_message = ''.join(message_parts).rstrip('\r\n')
        logging.info(f"ETS memory-optimized receive complete: {total_received // (1024*1024)}MB")
        return complete_message
        
    except Exception as e:
        logging.error(f"Error in ETS memory-optimized receive: {str(e)}")
        return None

# Global shared storage for all workers - use Manager for multiprocessing
shared_file_storage = None
storage_lock = None

class MemoryOptimizedFileProtocol:
    """File protocol yang dioptimalkan untuk memori 50GB"""
    
    def __init__(self, use_cache=True, shared_storage=None, shared_lock=None):
        self.cache = MemoryOptimizedCache() if use_cache else None
        # Use shared storage from multiprocessing Manager
        self.debug_files = shared_storage if shared_storage is not None else {}
        self.storage_lock = shared_lock if shared_lock is not None else threading.RLock()
        self.stats = defaultdict(int)
        logging.info("MemoryOptimizedFileProtocol initialized with multiprocessing shared storage")
    
    def proses_string(self, command_string):
        """Process commands dengan optimasi memori maksimal"""
        try:
            parts = command_string.strip().split(None, 2)  # Max 3 parts untuk performa
            if not parts:
                return '{"status": "ERROR", "data": "Empty command"}'
            
            command = parts[0].upper()
            self.stats[command] += 1
            
            if command == "LIST":
                with self.storage_lock:
                    file_list = list(self.debug_files.keys())
                    if self.cache:
                        cache_files = list(self.cache.cache.keys())
                        file_list.extend([f for f in cache_files if f not in file_list])
                
                logging.info(f"LIST command: Found {len(file_list)} files: {file_list}")
                return f'{{"status": "OK", "data": {file_list}}}'
            
            elif command == "UPLOAD":
                if len(parts) < 3:
                    return '{"status": "ERROR", "data": "Invalid upload command"}'
                
                filename = parts[1]
                encoded_content = parts[2]
                
                try:
                    # Decode dengan memory optimization
                    file_content = base64.b64decode(encoded_content.encode())
                    
                    # Store in both cache and shared storage for reliability
                    with self.storage_lock:
                        # Always store in shared debug storage as primary storage
                        self.debug_files[filename] = file_content
                        if self.cache:
                            self.cache.store(filename, file_content)
                    
                    logging.info(f"MEMORY-OPTIMIZED: File '{filename}' stored ({len(file_content)} bytes) in multiprocessing shared storage. Total files: {len(self.debug_files)}")
                    return f'{{"status": "OK", "data": "File uploaded to memory (optimized)"}}'
                    
                except Exception as e:
                    logging.error(f"Upload error for {filename}: {str(e)}")
                    return f'{{"status": "ERROR", "data": "Decode error: {str(e)}"}}'
            
            elif command == "GET":
                if len(parts) < 2:
                    return '{"status": "ERROR", "data": "Invalid get command"}'
                
                filename = parts[1]
                file_content = None
                
                logging.info(f"GET request for file: {filename}")
                
                with self.storage_lock:
                    # Try shared debug storage first (primary storage)
                    if filename in self.debug_files:
                        file_content = self.debug_files[filename]
                        logging.info(f"File {filename} found in shared storage ({len(file_content)} bytes)")
                    else:
                        logging.info(f"File {filename} not found in shared storage")
                        
                    # Try cache as fallback
                    if file_content is None and self.cache:
                        file_content = self.cache.retrieve(filename)
                        if file_content:
                            logging.info(f"File {filename} found in cache ({len(file_content)} bytes)")
                        else:
                            logging.info(f"File {filename} not found in cache")
                    
                    if file_content is None:
                        # Log available files for debugging
                        cache_files = list(self.cache.cache.keys()) if self.cache else []
                        debug_files = list(self.debug_files.keys())
                        logging.info(f"Available files - Shared: {debug_files}, Cache: {cache_files}")
                
                if file_content:
                    # Encode dengan memory optimization
                    try:
                        encoded_content = base64.b64encode(file_content).decode()
                        logging.info(f"MEMORY-OPTIMIZED: File '{filename}' retrieved ({len(file_content)} bytes), encoded to {len(encoded_content)} chars")
                        return f'{{"status": "OK", "data_file": "{encoded_content}"}}'
                    except Exception as encode_error:
                        logging.error(f"Failed to encode file {filename}: {str(encode_error)}")
                        return f'{{"status": "ERROR", "data": "Encoding error: {str(encode_error)}"}}'
                else:
                    return '{"status": "ERROR", "data": "File not found"}'
            
            elif command == "DELETE":
                if len(parts) < 2:
                    return '{"status": "ERROR", "data": "Invalid delete command"}'
                
                filename = parts[1]
                deleted = False
                
                with self.storage_lock:
                    # Delete from shared debug storage
                    if filename in self.debug_files:
                        del self.debug_files[filename]
                        deleted = True
                        logging.info(f"File {filename} deleted from shared storage")
                    
                    # Delete from cache
                    if self.cache:
                        cache_deleted = self.cache.delete(filename)
                        if cache_deleted:
                            logging.info(f"File {filename} deleted from cache")
                        deleted = deleted or cache_deleted
                
                if deleted:
                    logging.info(f"MEMORY-OPTIMIZED: File '{filename}' deleted from multiprocessing shared storage. Remaining files: {len(self.debug_files)}")
                    return f'{{"status": "OK", "data": "File deleted from memory"}}'
                else:
                    return '{"status": "ERROR", "data": "File not found"}'
            
            else:
                return '{"status": "ERROR", "data": "Unknown command"}'
        
        except Exception as e:
            logging.error(f"Memory-optimized protocol error: {str(e)}")
            return f'{{"status": "ERROR", "data": "Protocol error: {str(e)}"}}'

class FileServerProcessPool:
    def __init__(self, ipaddress='0.0.0.0', port=6665, max_processes=None, debug_mode=True):
        self.ipaddress = ipaddress
        self.port = port
        # Optimize process count for ETS requirements - support up to 50 workers
        if max_processes is None:
            cpu_count = multiprocessing.cpu_count()
            # Use more processes for ETS testing, but cap at 50
            self.max_processes = min(max(cpu_count, 50), 50)
        else:
            self.max_processes = min(max_processes, 50)  # Ensure we can handle 50 workers
            
        self.debug_mode = debug_mode
        self.server_socket = None
        self.process_pool = None
        self.is_running = False
        self.active_connections = 0
        self.connection_lock = threading.Lock()
        self.shutdown_event = threading.Event()
        
        # Create shared storage using Manager for multiprocessing - optimized for ETS
        self.manager = Manager()
        self.shared_storage = self.manager.dict()
        self.shared_lock = self.manager.RLock()
        
        # Performance monitoring for ETS
        self.stats = {
            'total_connections': 0,
            'active_connections': 0,
            'completed_tasks': 0,
            'failed_tasks': 0,
            'start_time': None,
            'large_file_transfers': 0,  # Track ETS large file transfers
            'total_bytes_processed': 0  # Track total bytes for ETS analysis
        }
        
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            logging.warning(f"Received signal {signum}, initiating immediate shutdown...")
            self.shutdown_event.set()
            self.is_running = False
            # Force immediate shutdown
            if self.process_pool:
                self.process_pool.terminate()
            if self.server_socket:
                self.server_socket.close()
            sys.exit(0)
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
    def start_server(self):
        """Start memory-optimized server untuk 50GB memory"""
        try:
            # Setup signal handlers
            self.setup_signal_handlers()
            
            # Create server socket dengan maximum optimization
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            
            # MAXIMUM buffer sizes untuk 50GB memory
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUFFER_SIZE)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCKET_BUFFER_SIZE)
            
            self.server_socket.bind((self.ipaddress, self.port))
            self.server_socket.listen(MAX_CONCURRENT_CONNECTIONS)
            
            logging.warning(f"MEMORY-OPTIMIZED server started on {self.ipaddress}:{self.port}")
            logging.warning(f"Using {self.max_processes} processes for 50GB memory")
            
            # Set multiprocessing start method to spawn
            multiprocessing.set_start_method('spawn', force=True)
            
            # Create process pool dengan optimasi
            self.process_pool = Pool(
                processes=self.max_processes,
                maxtasksperchild=1000,
                initializer=init_memory_optimized_worker,
                initargs=(self.debug_mode, PROCESS_BUFFER_SIZE, self.shared_storage, self.shared_lock)
            )
            
            self.is_running = True
            self.stats['start_time'] = time.time()
            
            # Start monitoring thread
            monitor_thread = threading.Thread(target=self._monitor_performance, daemon=True)
            monitor_thread.start()
            
            # Accept connections dengan minimal overhead
            while self.is_running and not self.shutdown_event.is_set():
                try:
                    # Set timeout for accept to allow checking shutdown_event
                    self.server_socket.settimeout(1.0)
                    try:
                        client_socket, client_address = self.server_socket.accept()
                    except socket.timeout:
                        continue
                        
                    logging.warning(f"Memory-optimized connection from {client_address}")
                    
                    with self.connection_lock:
                        self.active_connections += 1
                        self.stats['total_connections'] += 1
                        self.stats['active_connections'] = self.active_connections
                    
                    # Submit dengan high priority dan error handling
                    self.process_pool.apply_async(
                        handle_memory_optimized_connection, 
                        ((client_socket, client_address),),
                        callback=self._handle_success,
                        error_callback=self._handle_error
                    )
                    
                except socket.error as e:
                    if self.is_running and not self.shutdown_event.is_set():
                        logging.error(f"Socket error: {str(e)}")
                    break
                except Exception as e:
                    if self.is_running and not self.shutdown_event.is_set():
                        logging.error(f"Error accepting connection: {str(e)}")
                    
        except Exception as e:
            logging.error(f"Error starting memory-optimized server: {str(e)}")
        finally:
            self.shutdown()
    
    def _handle_success(self, result):
        """Handle successful task completion"""
        with self.connection_lock:
            self.active_connections -= 1
            self.stats['active_connections'] = self.active_connections
            self.stats['completed_tasks'] += 1
    
    def _handle_error(self, error):
        """Handle task errors"""
        with self.connection_lock:
            self.active_connections -= 1
            self.stats['active_connections'] = self.active_connections
            self.stats['failed_tasks'] += 1
        logging.error(f"Process pool error: {str(error)}")
    
    def _monitor_performance(self):
        """Monitor server performance with detailed worker tracking for ETS"""
        while self.is_running:
            try:
                uptime = time.time() - self.stats['start_time']
                tasks_per_second = (self.stats['completed_tasks'] + self.stats['failed_tasks']) / uptime if uptime > 0 else 0
                
                # Calculate success rate
                total_tasks = self.stats['completed_tasks'] + self.stats['failed_tasks']
                success_rate = (self.stats['completed_tasks'] / total_tasks * 100) if total_tasks > 0 else 0
                
                # ETS specific metrics
                total_gb_processed = self.stats['total_bytes_processed'] / (1024*1024*1024)
                
                logging.warning(
                    f"ETS Performance Stats - "
                    f"Uptime: {uptime:.1f}s, "
                    f"Active: {self.stats['active_connections']}, "
                    f"Total: {self.stats['total_connections']}, "
                    f"Completed: {self.stats['completed_tasks']}, "
                    f"Failed: {self.stats['failed_tasks']}, "
                    f"Success Rate: {success_rate:.1f}%, "
                    f"Tasks/s: {tasks_per_second:.1f}, "
                    f"Large Files: {self.stats['large_file_transfers']}, "
                    f"Total Data: {total_gb_processed:.2f}GB"
                )
                
                # Check for potential issues with ETS workload
                if self.stats['active_connections'] > self.max_processes * 0.9:
                    logging.warning("ETS High connection load detected")
                if self.stats['failed_tasks'] > self.stats['completed_tasks'] * 0.1:
                    logging.warning("ETS High failure rate detected")
                
                time.sleep(10)  # Update every 10 seconds for ETS monitoring
                
            except Exception as e:
                logging.error(f"Error in ETS performance monitoring: {str(e)}")
                time.sleep(10)
    
    def shutdown(self):
        """Shutdown the server gracefully"""
        if not self.is_running and not self.shutdown_event.is_set():
            return
            
        logging.warning("Initiating server shutdown...")
        self.is_running = False
        self.shutdown_event.set()
        
        # Close server socket
        if self.server_socket:
            try:
                self.server_socket.close()
                logging.warning("Server socket closed")
            except:
                pass
                
        # Shutdown process pool
        if self.process_pool:
            try:
                logging.warning("Terminating process pool...")
                self.process_pool.terminate()
                self.process_pool.join(timeout=1)  # Reduced timeout
                logging.warning("Process pool terminated")
            except:
                pass
                
        logging.warning("Server shutdown complete")
        sys.exit(0)  # Force exit

# Global variables untuk memory optimization
worker_debug_mode = True
worker_buffer_size = PROCESS_BUFFER_SIZE
worker_shared_storage = None
worker_shared_lock = None

def init_memory_optimized_worker(debug_mode, buffer_size, shared_storage, shared_lock):
    """Initialize worker dengan memory optimization"""
    global worker_debug_mode, worker_buffer_size, worker_shared_storage, worker_shared_lock
    worker_debug_mode = debug_mode
    worker_buffer_size = buffer_size
    worker_shared_storage = shared_storage
    worker_shared_lock = shared_lock
    logging.info(f"Memory-optimized worker initialized: {buffer_size // (1024*1024)}MB buffer with shared storage")

def handle_memory_optimized_connection(connection_data):
    """Handle connection dengan optimasi memori maksimal"""
    global worker_debug_mode, worker_buffer_size, worker_shared_storage, worker_shared_lock
    client_socket = None
    
    try:
        client_socket, client_address = connection_data
        process_id = os.getpid()
        logging.warning(f"Process {process_id} handling {client_address} (memory-optimized)")
        
        # Maximum socket optimization untuk 50GB memory
        client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUFFER_SIZE)  # 16MB
        client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCKET_BUFFER_SIZE)  # 16MB
        client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 0)
        
        # Use memory-optimized protocol with shared storage
        fp = MemoryOptimizedFileProtocol(use_cache=True, shared_storage=worker_shared_storage, shared_lock=worker_shared_lock)
        
        while True:
            try:
                # Memory-optimized message reception
                start_time = time.time()
                received_data = receive_complete_message_memory_optimized(client_socket)
                receive_time = time.time() - start_time
                
                if not received_data:
                    logging.info(f"Client {client_address} disconnected")
                    break
                
                command_parts = received_data.split()
                command_type = command_parts[0].upper() if command_parts else "UNKNOWN"
                
                logging.info(f"MEMORY-OPTIMIZED processing {command_type} from {client_address} ({receive_time:.3f}s)")
                
                # Process dengan memory optimization
                process_start = time.time()
                hasil = fp.proses_string(received_data)
                process_time = time.time() - process_start
                
                # Send response dengan memory optimization
                response = hasil + "\r\n\r\n"
                response_bytes = response.encode()
                
                send_start = time.time()
                client_socket.sendall(response_bytes)
                send_time = time.time() - send_start
                
                total_time = time.time() - start_time
                
                logging.warning(f"MEMORY-OPTIMIZED response to {client_address} - "
                              f"Receive: {receive_time:.3f}s, Process: {process_time:.3f}s, "
                              f"Send: {send_time:.3f}s, Total: {total_time:.3f}s")
                
                client_socket.settimeout(30.0)
                
            except socket.timeout:
                logging.warning(f"Client {client_address} timed out")
                break
            except ConnectionResetError:
                logging.info(f"Client {client_address} reset connection")
                break
            except Exception as e:
                logging.error(f"Error handling {client_address}: {str(e)}")
                break
        
    except Exception as e:
        logging.error(f"Error in memory-optimized connection: {str(e)}")
    finally:
        if client_socket:
            try:
                client_socket.close()
            except:
                pass

def main():
    """Main function dengan memory optimization untuk 50GB"""
    try:
        debug_mode = '--debug' in sys.argv or '-d' in sys.argv
        
        logging.warning("Starting MEMORY-OPTIMIZED File Server for 50GB")
        logging.warning(f"Chunk size: {CHUNK_SIZE // 1024}KB")
        logging.warning(f"Max memory usage: {MAX_MEMORY_USAGE // (1024*1024*1024)}GB")
        logging.warning(f"Socket buffer size: {SOCKET_BUFFER_SIZE // (1024*1024)}MB")
        
        # Create memory-optimized server with 50 max workers
        server = FileServerProcessPool(
            ipaddress='0.0.0.0', 
            port=6665,
            max_processes=50,  # Increased to 50 workers
            debug_mode=debug_mode
        )
        
        server.start_server()
        
    except KeyboardInterrupt:
        logging.warning("Server interrupted by user")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Server error: {str(e)}")
        sys.exit(1)
    finally:
        logging.warning("Server stopped")
        sys.exit(0)

if __name__ == "__main__":
    main()

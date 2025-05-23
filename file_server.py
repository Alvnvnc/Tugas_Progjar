from socket import *
import socket
import threading
import logging
import time
import sys
import signal


from file_protocol import FileProtocol
fp = FileProtocol()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set up a signal handler for graceful shutdown
def signal_handler(sig, frame):
    logging.warning('Received signal to terminate. Shutting down server...')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class ProcessTheClient(threading.Thread):
    def __init__(self, connection, address):
        self.connection = connection
        self.address = address
        self.connection.settimeout(30)  # 30 second timeout
        threading.Thread.__init__(self)
        
    def run(self):
        start_time = time.time()
        try:
            while True:
                # Initial chunk of data to determine the command
                data = self.connection.recv(32)
                if data:
                    d = data.decode()
                    
                    # Performance timestamp for monitoring
                    cmd_start_time = time.time()
                    command_type = d.split(' ', 1)[0].upper() if ' ' in d else d.upper()
                    
                    # Check if it's an upload command which needs more data
                    if d.lower().startswith('upload'):
                        # For upload, we need to receive more data
                        command_parts = d.split(' ', 2)  # Split into command, filename, and start of content
                        if len(command_parts) >= 2:
                            # Continue receiving data until we get the full content
                            while True:
                                more_data = self.connection.recv(8192)
                                if more_data:
                                    d += more_data.decode()
                                    # Check if we've received all the data
                                    if d.count(' ') >= 2:  # We have command, filename, and content
                                        break
                                else:
                                    break
                    # Special handling for delete command to ensure we have the filename parameter
                    elif d.lower().startswith('delete'):
                        command_parts = d.split(' ', 1)
                        if len(command_parts) < 2 or not command_parts[1].strip():
                            # If no filename provided, send error response
                            hasil = '{"status": "ERROR", "data": "Nama file diperlukan untuk delete"}'
                            hasil += "\r\n\r\n"
                            self.connection.sendall(hasil.encode())
                            continue
                        
                        # Make sure we received the entire command
                        if len(command_parts[1].strip()) < 1:
                            while True:
                                more_data = self.connection.recv(1024)
                                if more_data:
                                    d += more_data.decode()
                                    # If we have the filename parameter, we can process
                                    if len(d.split(' ', 1)[1].strip()) >= 1:
                                        break
                                else:
                                    break
                    
                    # Process the command
                    hasil = fp.proses_string(d)
                    hasil = hasil+"\r\n\r\n"
                    self.connection.sendall(hasil.encode())
                    
                    # Log performance metrics
                    cmd_end_time = time.time()
                    logging.info(f"Command {command_type} processed in {cmd_end_time - cmd_start_time:.4f} seconds")
                else:
                    break
        except socket.timeout:
            logging.warning(f"Connection from {self.address} timed out")
        except Exception as e:
            logging.error(f"Error handling client {self.address}: {str(e)}")
        finally:
            total_time = time.time() - start_time
            self.connection.close()
            logging.info(f"Connection from {self.address} closed after {total_time:.2f} seconds")


class Server(threading.Thread):
    def __init__(self, ipaddress='0.0.0.0', port=8889):
        self.ipinfo = (ipaddress, port)
        self.the_clients = []
        self.active_connections = 0
        self.running = True
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        threading.Thread.__init__(self)

    def run(self):
        logging.warning(f"Server starting on {self.ipinfo}")
        logging.warning(f"Standard Thread Server - one thread per connection")
        
        try:
            self.my_socket.bind(self.ipinfo)
            self.my_socket.listen(5)  # Increased backlog for more pending connections
            self.my_socket.settimeout(1)  # Allow interrupting the accept call
            
            # Start a monitoring thread
            monitor = threading.Thread(target=self.monitor_server)
            monitor.daemon = True
            monitor.start()
            
            while self.running:
                try:
                    connection, client_address = self.my_socket.accept()
                    self.active_connections += 1
                    logging.warning(f"Connection from {client_address} (Active: {self.active_connections})")
                    
                    clt = ProcessTheClient(connection, client_address)
                    clt.daemon = True  # Make thread a daemon so it exits when main thread exits
                    clt.start()
                    self.the_clients.append(clt)
                    
                    # Clean up completed threads
                    self.the_clients = [c for c in self.the_clients if c.is_alive()]
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
            time.sleep(60)  # Log stats every minute
            active_threads = len([c for c in self.the_clients if c.is_alive()])
            logging.info(f"Server stats - Active connections: {self.active_connections}, Active threads: {active_threads}")
    
    def shutdown(self):
        """Gracefully shut down the server"""
        self.running = False
        logging.warning("Shutting down server...")
        for _ in range(5):  # Try for 5 seconds
            active_clients = len([c for c in self.the_clients if c.is_alive()])
            if active_clients == 0:
                break
            logging.warning(f"Waiting for {active_clients} clients to finish...")
            time.sleep(1)
            
        self.my_socket.close()
        logging.warning("Server shutdown complete")


def main():
    # Ensure the files directory exists
    import os
    if not os.path.exists('files'):
        os.makedirs('files')
        logging.info("Created files directory")
        
    svr = Server(ipaddress='0.0.0.0', port=6665)
    svr.daemon = True  # Make thread a daemon so it exits when main thread exits
    svr.start()
    
    try:
        # Keep the main thread running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.warning("Server shutting down...")


if __name__ == "__main__":
    main()


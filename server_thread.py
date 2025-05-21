from socket import *
import socket
import threading
import logging
import time
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ProcessTheClient(threading.Thread):
	def __init__(self,connection,address):
		self.connection = connection
		self.address = address
		threading.Thread.__init__(self)

	def run(self):
		rcv = ""
		while True:
			try:
				data = self.connection.recv(32).decode('utf-8')
				if data:
					# Accumulate received data
					rcv = rcv + data
					
					# Check if request is complete (ends with CR+LF)
					if rcv.endswith("\r\n"):
						# Process TIME request
						if rcv.startswith("TIME\r\n"):
							# Get current time in hh:mm:ss format
							current_time = datetime.now().strftime('%H:%M:%S')
							response = f"JAM {current_time}\r\n"
							logging.info(f"Sending time: {current_time} to {self.address}")
							self.connection.sendall(response.encode('utf-8'))
							rcv = ""
						# Process QUIT request
						elif rcv.startswith("QUIT\r\n"):
							logging.info(f"Client {self.address} requested to quit")
							break
						else:
							# Invalid request
							logging.warning(f"Invalid request from {self.address}: {rcv}")
							rcv = ""
				else:
					# Connection closed by client
					break
			except Exception as e:
				logging.error(f"Error handling client {self.address}: {str(e)}")
				break
				
		logging.info(f"Connection with {self.address} closed")
		self.connection.close()

class Server(threading.Thread):
	def __init__(self, port=45000):
		self.the_clients = []
		self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.port = port
		threading.Thread.__init__(self)

	def run(self):
		self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.my_socket.bind(('0.0.0.0', self.port))
		self.my_socket.listen(5)
		logging.info(f"Time Server started on port {self.port}")
		
		while True:
			self.connection, self.client_address = self.my_socket.accept()
			logging.info(f"New connection from {self.client_address}")
			
			clt = ProcessTheClient(self.connection, self.client_address)
			clt.start()
			self.the_clients.append(clt)
	

def main():
	try:
		# Create and start the time server
		port = 45000
		svr = Server(port)
		svr.start()
		
		# Keep the main thread running
		while True:
			time.sleep(1)
			
	except KeyboardInterrupt:
		logging.info("Server shutting down...")
		sys.exit(0)
	except Exception as e:
		logging.error(f"Server error: {str(e)}")
		sys.exit(1)

if __name__=="__main__":
	main()


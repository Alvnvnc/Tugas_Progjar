from socket import *
import socket
import threading
import logging
import time
import sys


from file_protocol import  FileProtocol
fp = FileProtocol()


class ProcessTheClient(threading.Thread):
    def __init__(self, connection, address):
        self.connection = connection
        self.address = address
        threading.Thread.__init__(self)

    def run(self):
        while True:
            # Increase buffer size to handle larger data for file uploads
            # Initial chunk of data to determine the command
            data = self.connection.recv(32)
            if data:
                d = data.decode()
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
                
                hasil = fp.proses_string(d)
                hasil = hasil+"\r\n\r\n"
                self.connection.sendall(hasil.encode())
            else:
                break
        self.connection.close()


class Server(threading.Thread):
    def __init__(self,ipaddress='0.0.0.0',port=8889):
        self.ipinfo=(ipaddress,port)
        self.the_clients = []
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        threading.Thread.__init__(self)

    def run(self):
        logging.warning(f"server berjalan di ip address {self.ipinfo}")
        self.my_socket.bind(self.ipinfo)
        self.my_socket.listen(1)
        while True:
            self.connection, self.client_address = self.my_socket.accept()
            logging.warning(f"connection from {self.client_address}")

            clt = ProcessTheClient(self.connection, self.client_address)
            clt.start()
            self.the_clients.append(clt)


def main():
    svr = Server(ipaddress='0.0.0.0',port=6665)
    svr.start()


if __name__ == "__main__":
    main()


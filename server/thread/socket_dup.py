"""
Socket duplication helper module for multiprocessing environments.

This module provides utilities for proper socket duplication when transferring
sockets between processes, which is crucial for stability in process pool servers.
"""

import os
import socket
import logging
import multiprocessing

def duplicate_socket_fd(sock):
    """
    Safely duplicate a socket file descriptor for transfer to another process.
    
    Args:
        sock: The socket object to duplicate
    
    Returns:
        tuple: (dup_fd, family, type, proto) containing all info needed to reconstruct the socket
    """
    sock_fd = sock.fileno()
    dup_fd = os.dup(sock_fd)
    family = sock.family
    type = sock.type
    proto = sock.proto
    
    return (dup_fd, family, type, proto)

def create_socket_from_info(sock_info):
    """
    Create a new socket from duplicated file descriptor and socket information.
    
    Args:
        sock_info: tuple of (fd, family, type, proto) with socket info
        
    Returns:
        socket.socket: A new socket object connected to the same endpoint
    """
    fd, family, type, proto = sock_info
    
    # Create the socket directly with the file descriptor
    sock = socket.socket(family, type, proto, fileno=fd)
    sock.setblocking(True)  # Ensure the socket is in blocking mode
    
    return sock

def recvall(sock, n):
    """
    Reliably receive exactly n bytes from a socket.
    
    Args:
        sock: Socket to receive from
        n: Number of bytes to receive
        
    Returns:
        bytes: The received data
    """
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            # Connection closed before receiving all data
            return None
        data.extend(packet)
    return bytes(data)

def sendall_with_retry(sock, data, max_retries=3):
    """
    Send all data with retries in case of temporary failures.
    
    Args:
        sock: Socket to send data on
        data: Data to send
        max_retries: Maximum number of retries
        
    Returns:
        bool: True if successful, False otherwise
    """
    for attempt in range(max_retries):
        try:
            sock.sendall(data)
            return True
        except (socket.error, BrokenPipeError, ConnectionResetError) as e:
            if attempt == max_retries - 1:
                # Last attempt failed
                logging.error(f"Failed to send data after {max_retries} attempts: {e}")
                return False
            logging.warning(f"Send attempt {attempt+1} failed: {e}, retrying...")
            
            # Short delay before retry
            import time
            time.sleep(0.1 * (attempt + 1))
            
            # Try to check if socket is still valid
            try:
                sock.getpeername()
            except:
                # Socket is invalid, can't retry
                logging.error("Socket is no longer valid, aborting sendall")
                return False
                
    return False

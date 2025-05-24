import socket
import logging

def create_socket_from_fd(sock_fd, family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0):
    """Create a socket object properly from a file descriptor
    
    This is a safer alternative to socket.fromfd() which doesn't actually 
    duplicate the underlying file descriptor properly.
    """
    logger = logging.getLogger()
    logger.debug(f"Creating socket from fd={sock_fd}, family={family}, type={type}")
    
    try:
        # Create the socket directly from the file descriptor
        sock = socket.socket(family, type, proto, fileno=sock_fd)
        sock.setblocking(True)  # Ensure socket is in blocking mode
        
        # Get socket info for debugging
        try:
            local_addr = sock.getsockname()
            remote_addr = sock.getpeername()
            logger.debug(f"Created socket: local={local_addr}, remote={remote_addr}")
        except Exception as e:
            logger.debug(f"Couldn't get socket address info: {e}")
            
        return sock
    except Exception as e:
        logger.error(f"Error creating socket from fd={sock_fd}: {e}")
        # Try alternate method as fallback
        fallback_sock = socket.fromfd(sock_fd, family, type)
        logger.debug("Created socket using fallback method")
        return fallback_sock

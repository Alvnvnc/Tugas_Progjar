#!/usr/bin/env python3
"""
Helper script to start the file server with different worker pool sizes
"""
import os
import sys
import subprocess
import argparse

def main():
    parser = argparse.ArgumentParser(description='Launch File Server with specified worker pool')
    parser.add_argument('server_type', choices=['thread', 'process'], 
                       help='Server type: "thread" for ThreadPool, "process" for ProcessPool')
    parser.add_argument('--workers', type=int, default=50,
                       help='Number of workers in the pool (default: 50)')
    parser.add_argument('--port', type=int, default=6665,
                       help='Port to listen on (default: 6665)')
    args = parser.parse_args()

    # Choose server script based on type
    if args.server_type.lower() == 'thread':
        server_script = 'file_server_threadpool.py'
    else:
        server_script = 'file_server_processpool.py'
    
    # Build command
    cmd = [
        sys.executable,
        server_script,
        '--workers', str(args.workers),
        '--port', str(args.port)
    ]
    
    # Show what we're doing
    print(f"Starting {args.server_type}pool server with {args.workers} workers on port {args.port}")
    print(f"Command: {' '.join(cmd)}")
    
    # Execute the server
    try:
        subprocess.run(cmd)
    except KeyboardInterrupt:
        print("\nServer stopped by user")
    
if __name__ == "__main__":
    main()

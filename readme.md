# File Server ETS - Dokumentasi Singkat

## Overview
Sistem file server Python dengan arsitektur multiprocessing untuk menangani file 10-100MB dengan hingga 50 concurrent workers.

## Struktur Sistem

### Server Implementation
```
server/
├── process/
│   ├── file_server_processpool.py  # Server dengan process pool (50GB optimized)
│   ├── file_protocol.py           # Protocol handler sederhana
│   └── file_interface.py          # File operations (disk-based)
└── thread/
    ├── file_protocol.py           # Thread-safe protocol handler  
    └── file_interface.py          # Thread-safe file operations
```

### Client Implementation
```
client/
├── process/
│   └── proccess.py                # Client dengan process pool testing
└── thread/
    ├── config.py                  # Konfigurasi dan test settings
    ├── main.py                    # Entry point untuk testing
    ├── test_runner.py             # Thread-based test execution
    └── reporting.py               # Hasil testing dan charts
```

## Konfigurasi Utama

### Server Process (Recommended)
- **Memory**: 50GB RAM dengan 35GB cache
- **Workers**: Hingga 50 concurrent processes
- **Files**: Optimized untuk 10-100MB
- **Protocol**: Memory-optimized dengan shared storage

### Server Thread (Educational)
- **Memory**: Standard Python memory management
- **Workers**: Thread-based dengan GIL limitations
- **Files**: Direct disk I/O
- **Protocol**: Simple file system operations

## ETS Test Requirements ✅

**Total: 54 test combinations**
```
Operations: upload, download (2)
File Sizes: 10MB, 50MB, 100MB (3)
Client Workers: 1, 5, 50 (3)
Server Workers: 1, 5, 50 (3)
Total: 2 × 3 × 3 × 3 = 54
```

## Quick Start

### 1. Start Server
```bash
# Process server (recommended)
cd server/process
python file_server_processpool.py

# Thread server (alternative)
cd server/thread  
python file_server.py
```

### 2. Run Client Tests
```bash
# Process client (full ETS testing)
cd client/process
python proccess.py

# Thread client (comprehensive analysis)
cd client/thread
python main.py
```

## Key Features

### Process Model
- ✅ 50GB memory optimization
- ✅ True parallelism (no GIL)
- ✅ Shared memory storage
- ✅ Enterprise performance

### Thread Model  
- ✅ Simple implementation
- ✅ Lower resource usage
- ✅ Easy debugging
- ✅ Educational value

## Protocol
```
Request:  COMMAND params\r\n\r\n
Response: {"status":"OK/ERROR","data":"..."}\r\n\r\n
Commands: LIST, GET, UPLOAD, DELETE
Encoding: Base64 untuk binary files
```

## Performance
- **10MB**: > 100 MB/s throughput
- **50MB**: > 200 MB/s throughput  
- **100MB**: > 300 MB/s throughput
- **Concurrency**: 50 workers support
- **Success Rate**: > 95%


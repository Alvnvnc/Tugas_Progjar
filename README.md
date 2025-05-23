# File Server dengan Thread Pool dan Process Pool

Implementasi server file yang mendukung dua metode konkurensi: Thread Pool dan Process Pool. Server ini dirancang untuk menangani multiple client secara bersamaan dengan efisiensi tinggi.

## Struktur Program

### File Utama
1. `file_server_threadpool.py` - Implementasi server menggunakan Thread Pool
2. `file_server_processpool.py` - Implementasi server menggunakan Process Pool
3. `run_server.py` - Script untuk menjalankan server

### File Pendukung
1. `file_protocol.py` - Implementasi protokol komunikasi
2. `file_interface.py` - Interface untuk operasi file
3. `file_server.py` - Implementasi dasar server file

## Cara Menjalankan Server

### 1. Thread Pool Server

```bash
python file_server_threadpool.py --ip 0.0.0.0 --port 6665 --pool 5
```

Options:
- `--ip`: IP address untuk binding (default: 0.0.0.0)
- `--port`: Port untuk binding (default: 6665)
- `--pool`: Ukuran thread pool (pilihan: 1, 5, 50, 100, 200)

### 2. Process Pool Server

```bash
python file_server_processpool.py --ip 0.0.0.0 --port 6665 --pool 5
```

Options sama dengan Thread Pool Server.

## Perbedaan Implementasi

### Thread Pool Server (`file_server_threadpool.py`)

1. **Arsitektur**:
   - Menggunakan `concurrent.futures.ThreadPoolExecutor`
   - Semua thread berbagi memori dalam satu proses
   - Cocok untuk I/O bound operations

2. **Fitur**:
   - Thread pool dengan ukuran yang dapat dikonfigurasi
   - Monitoring thread aktif
   - Statistik performa per thread
   - Logging detail untuk setiap thread

3. **Keuntungan**:
   - Lebih ringan dalam penggunaan memori
   - Komunikasi antar thread lebih cepat
   - Cocok untuk operasi I/O seperti file transfer

4. **Keterbatasan**:
   - Terbatas oleh Global Interpreter Lock (GIL)
   - Tidak dapat memanfaatkan multiple CPU cores secara penuh

### Process Pool Server (`file_server_processpool.py`)

1. **Arsitektur**:
   - Menggunakan `multiprocessing.Process`
   - Setiap client ditangani oleh proses terpisah
   - Memanfaatkan multiple CPU cores

2. **Fitur**:
   - Process pool dengan ukuran yang dapat dikonfigurasi
   - Monitoring CPU dan memori per proses
   - Statistik performa per proses
   - Logging detail untuk setiap proses

3. **Keuntungan**:
   - Dapat memanfaatkan multiple CPU cores
   - Isolasi memori antar proses
   - Lebih stabil untuk operasi CPU-intensive

4. **Keterbatasan**:
   - Penggunaan memori lebih tinggi
   - Overhead komunikasi antar proses lebih besar

## Monitoring dan Logging

Kedua implementasi menyediakan:

1. **Statistik Server**:
   - Jumlah request per detik
   - Jumlah thread/proses aktif
   - Waktu uptime server

2. **Statistik Per Client**:
   - Total data yang diproses
   - Waktu pemrosesan
   - Throughput (KB/s)

3. **Logging Detail**:
   - Thread Pool: `%(asctime)s - %(threadName)s - %(levelname)s - %(message)s`
   - Process Pool: `%(asctime)s - %(processName)s - %(levelname)s - %(message)s`

## Protokol Komunikasi

Server mendukung operasi file berikut:

1. **UPLOAD**:
   ```
   UPLOAD <filename> <base64_content>
   ```

2. **GET**:
   ```
   GET <filename>
   ```

3. **DELETE**:
   ```
   DELETE <filename>
   ```

4. **LIST**:
   ```
   LIST
   ```

## Troubleshooting

1. **Server Tidak Merespon**:
   - Periksa port yang digunakan
   - Pastikan tidak ada firewall yang memblokir
   - Cek log untuk error

2. **Performa Lambat**:
   - Sesuaikan ukuran pool dengan beban
   - Monitor penggunaan CPU dan memori
   - Periksa ukuran buffer transfer

3. **Memory Issues**:
   - Kurangi ukuran pool
   - Monitor penggunaan memori per thread/proses
   - Sesuaikan chunk size untuk transfer file

## Best Practices

1. **Thread Pool**:
   - Gunakan untuk operasi I/O bound
   - Sesuaikan pool size dengan jumlah core CPU
   - Monitor thread leaks

2. **Process Pool**:
   - Gunakan untuk operasi CPU bound
   - Pertimbangkan overhead memori
   - Monitor child process cleanup 
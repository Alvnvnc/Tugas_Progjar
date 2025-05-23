# File Server Stress Testing Tool

Alat stress testing untuk menguji performa server file dengan berbagai kombinasi parameter. Program ini mendukung pengujian menggunakan threading dan multiprocessing.

## Struktur Program

Program terdiri dari beberapa komponen utama:

1. `main.py` - Program utama untuk stress testing menggunakan threading
2. `stress_test_process.py` - Program untuk stress testing menggunakan multiprocessing
3. `config.py` - Konfigurasi dan utilitas untuk stress testing
4. `test_runner.py` - Implementasi logika pengujian
5. `file_operations.py` - Operasi file (upload/download)
6. `reporting.py` - Pembuatan laporan dan visualisasi hasil

## Persyaratan

- Python 3.6 atau lebih baru
- Dependencies:
  - matplotlib
  - numpy
  - tabulate

Install dependencies dengan:
```bash
pip install matplotlib numpy tabulate
```

## Cara Menjalankan Program

### 1. Stress Testing dengan Threading

Program ini menggunakan threading untuk menjalankan multiple client secara bersamaan.

```bash
python main.py [options]
```

Options yang tersedia:
- `-o, --operations`: Operasi yang akan diuji (upload, download)
- `-f, --file-sizes`: Ukuran file dalam MB
- `-c, --client-workers`: Jumlah client worker
- `-s, --server-workers`: Jumlah server worker
- `-v, --verbose`: Aktifkan logging verbose
- `-l, --log-file`: File untuk menyimpan log
- `-r, --report-dir`: Direktori untuk menyimpan laporan

Contoh:
```bash
python main.py -o upload download -f 10 50 100 -c 1 5 50 -s 1 5 50
```

### 2. Stress Testing dengan Multiprocessing

Program ini menggunakan multiprocessing untuk menjalankan multiple client dalam proses terpisah.

```bash
python stress_test_process.py [options]
```

Options yang tersedia sama dengan versi threading.

Contoh:
```bash
python stress_test_process.py -o upload download -f 10 50 100 -c 1 5 50 -s 1 5 50
```

## Kombinasi Test

Program akan menjalankan kombinasi test berikut:
- Operasi: upload dan download
- Ukuran file: 10MB, 50MB, 100MB
- Jumlah client worker: 1, 5, 50
- Jumlah server worker: 1, 5, 50

Total kombinasi: 2 x 3 x 3 x 3 = 54 test (27 upload + 27 download)

## Output

Program akan menghasilkan:

1. File CSV dengan hasil test yang berisi:
   - Nomor test
   - Operasi
   - Volume file
   - Jumlah client worker pool
   - Jumlah server worker pool
   - Waktu total per client
   - Throughput per client
   - Jumlah worker client yang sukses dan gagal

2. File log yang berisi detail proses testing

3. Grafik visualisasi hasil test:
   - Throughput chart untuk setiap operasi
   - Time chart untuk setiap operasi
   - Perbandingan operasi

## Cara Kerja Program

### Threading Version (main.py)

1. Program membuat thread pool untuk menjalankan client
2. Setiap client thread menjalankan operasi upload/download
3. Hasil dikumpulkan dan dianalisis
4. Laporan dibuat dalam format CSV dan grafik

### Multiprocessing Version (stress_test_process.py)

1. Program membuat process pool untuk menjalankan client
2. Setiap client process berjalan secara independen
3. Hasil dikumpulkan dan dianalisis
4. Laporan dibuat dalam format CSV dan grafik

## Perbedaan Threading vs Multiprocessing

1. Threading:
   - Menggunakan satu proses dengan multiple thread
   - Berbagi memori antar thread
   - Cocok untuk I/O bound operations
   - Lebih ringan dalam penggunaan memori

2. Multiprocessing:
   - Menggunakan multiple proses
   - Setiap proses memiliki memori sendiri
   - Cocok untuk CPU bound operations
   - Lebih berat dalam penggunaan memori
   - Lebih baik untuk paralelisasi sejati

## Troubleshooting

1. Jika server tidak merespon:
   - Pastikan server berjalan di port yang benar
   - Periksa firewall settings
   - Pastikan IP address server benar

2. Jika test gagal:
   - Periksa log file untuk detail error
   - Pastikan ada cukup disk space
   - Pastikan ada cukup memori untuk file besar

3. Jika performa lambat:
   - Kurangi jumlah worker
   - Kurangi ukuran file
   - Periksa beban sistem 
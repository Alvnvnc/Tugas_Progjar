# Implementasi Time Server

Dokumen ini menjelaskan implementasi Time Server konkuren sesuai dengan persyaratan yang ditentukan.

## Gambaran Umum

Time Server adalah server TCP yang mendengarkan pada port 45000 dan merespons permintaan klien dengan waktu saat ini. Server menggunakan multithreading untuk menangani beberapa koneksi klien secara bersamaan.

## Detail Implementasi

### A. Mendengarkan pada Port 45000 dengan TCP

```python
def __init__(self, port=45000):
    self.the_clients = []
    self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.port = port
    threading.Thread.__init__(self)
```

Pada bagian ini, saya mengimplementasikan server TCP yang mendengarkan pada port 45000. Server dibuat dengan membuat objek socket menggunakan `socket.AF_INET` untuk pengalamatan IPv4 dan `socket.SOCK_STREAM` untuk protokol TCP. Ini memungkinkan komunikasi yang andal dan berorientasi koneksi antara server dan klien, memastikan bahwa data ditransmisikan tanpa kesalahan dan dalam urutan yang benar. Nomor port diatur sebagai parameter dengan nilai default 45000, membuatnya dapat dikonfigurasi jika diperlukan.

### B. Penanganan Permintaan Konkuren dengan Multithreading

```python
clt = ProcessTheClient(self.connection, self.client_address)
clt.start()
self.the_clients.append(clt)
```

Untuk menangani koneksi klien secara bersamaan, saya menggunakan modul threading Python. Ketika klien baru terhubung, server membuat thread `ProcessTheClient` baru untuk menangani koneksi tersebut. Dengan memulai thread terpisah untuk setiap klien, server dapat terus menerima koneksi baru sementara klien yang sudah ada dilayani. Pendekatan ini memungkinkan server untuk menangani beberapa klien secara bersamaan tanpa memblokir thread utama. Server menyimpan semua thread klien dalam daftar `the_clients` untuk tujuan manajemen.

### C. Pemrosesan Permintaan

```python
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
```

Server mengenali dua jenis permintaan: "TIME" dan "QUIT", keduanya diakhiri dengan karakter carriage return dan line feed (\r\n). Ketika permintaan "TIME" diterima, server mengambil waktu saat ini dan mengirimkannya kembali ke klien. Untuk permintaan "QUIT", server menghentikan loop komunikasi, yang mengakibatkan penutupan koneksi. Saya mengimplementasikan mekanisme akumulasi buffer untuk menangani data yang mungkin tiba secara terfragmentasi, memastikan bahwa permintaan lengkap diproses dengan benar. Server mempertahankan buffer ini hingga menerima karakter akhir (\r\n) yang menandakan permintaan lengkap.

### D. Format Respons Waktu
```python
# Get current time in hh:mm:ss format
current_time = datetime.now().strftime('%H:%M:%S')
response = f"JAM {current_time}\r\n"
```

Untuk respons waktu, saya menggunakan modul datetime Python untuk mendapatkan waktu saat ini dan memformatnya sesuai kebutuhan (jam:menit:detik). Respons dibuat sebagai string dalam format UTF-8, dimulai dengan "JAM " diikuti oleh waktu yang diformat, dan diakhiri dengan karakter carriage return dan line feed. Ini mengikuti spesifikasi secara tepat, memberikan klien cara standar untuk mengurai respons dari server. Penggunaan `strftime` memastikan bahwa waktu selalu diformat secara konsisten terlepas dari representasi waktu default sistem.

## Penanganan Kesalahan dan Pencatatan

Implementasi ini mencakup penanganan kesalahan dan pencatatan yang komprehensif untuk memastikan ketangguhan:

```python
try:
    # Client handling code
except Exception as e:
    logging.error(f"Error handling client {self.address}: {str(e)}")
```

Penanganan kesalahan diimplementasikan menggunakan blok try-except untuk menangkap dan mencatat setiap pengecualian yang mungkin terjadi selama komunikasi klien. Ini mencegah server crash karena masalah spesifik klien. Selain itu, pencatatan digunakan di seluruh kode untuk memberikan visibilitas ke dalam operasi server, termasuk informasi tentang koneksi baru, permintaan waktu, dan setiap kesalahan yang terjadi. Ini memudahkan debugging dan pemantauan perilaku server secara real-time.

## Cara Menjalankan

Untuk menjalankan Time Server, jalankan perintah berikut:

```bash
python server_thread.py
```

Server akan dimulai dan mendengarkan koneksi masuk pada port 45000.

## Pengujian

Anda dapat menguji server menggunakan klien telnet sederhana:

```bash
telnet localhost 45000
```

Kemudian ketik:
```
TIME
```

Server akan merespons dengan:
```
JAM jam:menit:detik
```

Untuk menutup koneksi, ketik:
```
QUIT
```

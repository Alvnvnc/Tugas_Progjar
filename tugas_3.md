# Implementasi Protokol Server File

Repositori ini berisi implementasi server file dengan komunikasi berbasis protokol antara klien dan server. Implementasi ini mencakup fungsionalitas untuk membuat daftar, mengambil, mengunggah, dan menghapus file.

## Spesifikasi Protokol

Protokol server file sekarang menyertakan dua perintah baru selain perintah LIST dan GET yang sudah ada:

### UPLOAD Command
```
UPLOAD
* TUJUAN: untuk mengunggah file ke server
* PARAMETER:
  - PARAMETER1 : nama file yang akan disimpan
  - PARAMETER2 : isi file dalam bentuk base64
* RESULT:
- BERHASIL:
  - status: OK
  - data: file berhasil disimpan
- GAGAL:
  - status: ERROR
  - data: pesan kesalahan
```

### DELETE Command
```
DELETE
* TUJUAN: untuk menghapus file dari server
* PARAMETER:
  - PARAMETER1 : nama file yang akan dihapus
* RESULT:
- BERHASIL:
  - status: OK
  - data: file berhasil dihapus
- GAGAL:
  - status: ERROR
  - data: pesan kesalahan
```

Ekstensi protokol ini memungkinkan klien memiliki kemampuan manajemen file yang lengkap, tidak hanya untuk mengambil tetapi juga membuat dan menghapus file. Semua operasi mempertahankan format respons JSON dengan kode status dan pesan yang sesuai, memastikan konsistensi dengan desain protokol yang ada. Perintah UPLOAD memerlukan pengkodean base64 untuk memastikan data biner dapat ditransmisikan sebagai teks dalam format berbasis string dari protokol.

## Operasi Client-Server

### Operasi Upload dan Delete File

Screenshot yang disediakan menunjukkan implementasi yang berhasil dari operasi upload dan delete file dalam protokol server file. Screenshot pertama menunjukkan menu klien file dengan operasi delete yang sedang berlangsung. Kita dapat melihat bahwa klien pertama-tama menampilkan daftar file yang tersedia di server (termasuk readme.md, pokijan.jpg, donalbebek.jpg, dan rfc2616.pdf), kemudian memproses permintaan delete untuk file readme.md. Server merespon dengan konfirmasi bahwa file berhasil dihapus, yang dapat diverifikasi pada screenshot kedua yang menampilkan output terminal server. Screenshot kedua juga menunjukkan fungsi upload, dengan konten yang dikodekan base64 diterima dan diproses oleh server. Screenshot ketiga menunjukkan isi folder setelah operasi berhasil, membuktikan bahwa readme.md pertama dihapus dan kemudian di-upload kembali, mengonfirmasi bahwa operasi berhasil dilakukan. Screenshot keempat secara khusus menampilkan operasi upload dari sisi klien, di mana pengguna mengunggah file README.MD, dan klien mengonfirmasi upload berhasil setelah menerima respons server yang sesuai. Screenshot-screenshot ini secara kolektif menunjukkan bahwa kedua fungsi upload dan delete bekerja sebagaimana yang diharapkan dalam arsitektur client-server, dengan pemrosesan perintah yang tepat, pengkodean/dekode base64, dan penanganan respons.

## Detail Implementasi

### Modifikasi Sisi Server

Implementasi server memerlukan dua modifikasi penting untuk mendukung operasi baru. Pertama, kelas `FileInterface` diperluas dengan dua metode baru: `upload()` dan `delete()`. Metode `upload()` menangani penerimaan file yang dikodekan base64, mendekodenya, dan menuliskannya ke sistem file server dengan penanganan kesalahan yang tepat. Metode `delete()` memvalidasi parameter nama file, memeriksa apakah file ada, dan menghapusnya jika memungkinkan, juga memberikan pesan kesalahan yang sesuai.

Selain itu, modul `file_server.py` dimodifikasi untuk menangani transmisi data yang lebih besar yang diperlukan untuk upload file. Implementasi asli memiliki ukuran buffer tetap 32 byte, yang cukup untuk perintah sederhana tetapi tidak memadai untuk menangani konten file yang berpotensi besar yang dikodekan base64. Server yang dimodifikasi sekarang menggunakan mekanisme chunking untuk secara iteratif membaca data sampai seluruh perintah dan konten file diterima, diidentifikasi dengan format perintah yang tepat.

### Modifikasi Sisi Klien

Di sisi klien, implementasi ditingkatkan untuk menyediakan antarmuka interaktif untuk operasi manajemen file. Modul `file_client_cli.py` sekarang menyertakan dua fungsi baru: `remote_upload()` dan `remote_delete()`. Fungsi upload membaca file lokal, mengubahnya ke format base64, dan mengirimkan konten yang dikodekan bersama dengan nama file ke server. Fungsi delete mengirimkan perintah DELETE sederhana dengan nama file target.

Klien juga mengimplementasikan sistem menu interaktif yang memungkinkan pengguna memilih operasi mana yang akan dilakukan dan file mana yang akan digunakan. Pendekatan yang ramah pengguna ini memudahkan untuk menguji dan mendemonstrasikan semua operasi file, termasuk membuat daftar file sebelum dan sesudah operasi untuk memverifikasi keberhasilannya. Penanganan kesalahan telah diimplementasikan di seluruh kode untuk memberikan umpan balik yang berarti ketika operasi gagal, seperti saat mencoba mengunggah file yang tidak ada atau menghapus file yang tidak ada di server.

## Kesimpulan

Protokol server file yang diperluas dan implementasinya menyediakan serangkaian operasi manajemen file yang lengkap melalui protokol berbasis teks yang sederhana. Penggunaan pengkodean base64 memastikan file biner dapat ditransmisikan sebagai teks, mempertahankan kesederhanaan protokol sambil mendukung semua jenis file. Penanganan kesalahan protokol dan format respons yang konsisten membuatnya tangguh dan mudah digunakan dari perspektif server dan klien.

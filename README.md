# Tugas Besar Sistem Paralel dan Terdistribusi IF3230 

> Consensus Protocol: Raft

## Anggota Kelompok
| Nama | NIM |
| ----------- | ----------- |
| Bintang Hijriawan | 13521003 |
| Jason Rivalino | 13521008 |
| Laila Bilbina Khoiru Nisa | 13521016 |
| Agsha Athalla Nurkareem | 13521027 |
| Jauza Lathifah Annassalafi | 13521030 |

## Deskripsi Program
Program sederhana untuk mengimplementasikan protokol konsensus Raft sederhana. Program dibuat dengan menggunakan bahasa pemrograman Python dan dapat menerima layanan antara lain `ping, get, set, strln, del, dan append` . Adapun untuk implementasi yang disediakan dalam protokol ini antara lain yaitu:
- Membership Change	(Mechanism for adding another server node)
- Log Replication		(Cluster action logging system)
- Heartbeat			(Node health monitoring & periodic messages)
- Leader Election		(Leader node failover mechanism)

## Library Program
- xmlrpc		(ServerProxy untuk client, SimpleXMLRPCServer untuk server)
- json		(Parser pesan)
- asyncio	(Untuk asynchronous RPC)
- time		(Digunakan untuk kepentingan logging)
- socket		(Timeout untuk semua RPC)
- threading	(Background thread)
- sys		(Hanya untuk argumen)
- Library pendukung lainnya (os, enum, typing, random, traceback, signal, HTTP)

## Cara Menjalankan Program
<b>1. Clone repository ini terlebih dahulu</b>

<b>2. Membuka terminal dan sesuaikan directory dengan tempat clone _source code_ ini.</b>

<b>3. Menjalankan perintah berikut untuk menjalankan server untuk node Leader.</b>
```
python lib/server.py localhost [node leader port]
Contoh: python .\src\server.py localhost 8000
```

Keterangan:
- Node leader Port: port yang digunakan untuk Node dari leader
<br>


<b>4. Buka terminal baru dan menjalankan perintah berikut untuk menjalankan server untuk node Follower.</b>
```
python lib/server.py localhost [node follower port] [node leader port]
Contoh: python .\src\server.py localhost 8003 localhost 8000
```

Keterangan:
- Node follower Port: port yang digunakan untuk Node dari follower
- Node leader Port: port yang digunakan untuk Node dari leader
<br>

<b>5. Buka terminal baru dan jalankan perintah berikut untuk menjalankan koneksi client dengan server</b>
```
python lib/client.py localhost [node server port]
Contoh: python .\src\client.py localhost 8000
```

Keterangan:
- Node server port: node yang akan terhubung dari client ke server

## Acknowledgements
- Tuhan Yang Maha Esa
- Dosen Pengampu Mata Kuliah IF3230 Sistem Paralel dan Terdistribusi
- Kakak-Kakak Asisten Mata Kuliah IF3230 Sistem Paralel dan Terdistribusi

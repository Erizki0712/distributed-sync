---

# Distributed Sync System

Sistem sinkronisasi terdistribusi untuk demonstrasi **Distributed Lock Manager (Raft)**, **Distributed Queue (at-least-once)**, dan **Distributed Cache (MESI + LRU)**. Mendukung **3 node cluster** dan **single node** untuk perbandingan performa. Metrik diekspos via **/metrics** (Prometheus format). Uji beban menggunakan **Locust**. Uji fungsional menggunakan **Postman** (collection & environment disertakan di repo).

---

## Prasyarat

* Python 3.11+
* Docker & Docker Compose
* Locust: `pip install locust`
* Postman

> Port default:
>
> * Cluster 3 node: **8101/8102/8103** (service), **6379** (Redis)
> * Single node: **8201** (service), **6380** (Redis single)

---

## 1) Clone Project

```bash
git clone https://github.com/Erizki0712/distributed-sync.git 
cd distributed-sync
```

---

## 2) Buat Virtual Environment

```bash
python -m venv .venv
# Windows
.\.venv\Scripts\activate
# Linux/Mac
# source .venv/bin/activate
```

---

## 3) Install Requirements

```bash
pip install -r requirements.txt
```

---

## 4) Pindah ke Folder Docker

```bash
cd docker
```

---

## 5) Jalankan Layanan

### 5.1 Cluster **3 Node**

```bash
docker compose up --build
```

Akses:

* Node-1: [http://localhost:8101](http://localhost:8101)
* Node-2: [http://localhost:8102](http://localhost:8102)
* Node-3: [http://localhost:8103](http://localhost:8103)
* Redis:   redis://localhost:6379

### 5.2 **Single Node** (berjalan berdampingan, port berbeda)

```bash
docker compose -f docker-compose.single.yml -p dss-single up --build -d
```

Akses:

* Single node: [http://localhost:8201](http://localhost:8201)
* Redis single: redis://localhost:6380

### 5.3 Hentikan & Bersihkan

```bash
# cluster 3 node
docker compose down -v
# single node
docker compose -f docker-compose.single.yml -p dss-single down -v
```

---

## 6) Benchmark

> Pastikan Locust terpasang: `pip install locust`

### 6.1 Versi **3 Node**

```bash
locust -f benchmarks/load_test_scenarios.py \
  --host http://localhost:8101 \
  --headless -u 200 -r 50 -t 60s \
  --csv cluster_run
```

### 6.2 Versi **Single Node**

```bash
locust -f benchmarks/load_test_scenarios.py \
  --host http://localhost:8201 \
  --headless -u 200 -r 50 -t 60s \
  --csv single_run
```

**Output:**

* Ringkasan hasil tampil di akhir run.
* File CSV (`*_stats.csv`, `*_failures.csv`, dll.) tersimpan di **root folder** proyek.

> **Tips:** Jalankan keduanya secara terpisah.

---

## 7) Uji Fitur via Postman

1. **Import** file **Postman Collection** dan **Environment** dari folder `postman/` 
2. Set nilai environment:

   * `baseUrlNode1 = http://localhost:8101`
   * `baseUrlNode2 = http://localhost:8102`
   * `baseUrlNode3 = http://localhost:8103`
   * `timeoutMs = 1000`
3. Jalankan request sesuai folder:

   * **Raft**: `GET /raft/status` untuk melihat node **LEADER/FOLLOWER**.
   * **Lock Manager**:

     * `POST /lock/acquire` (mode `exclusive` atau `shared`)
     * `POST /lock/release`
   * **Queue**:

     * `POST /queue/publish`
     * `POST /queue/consume` (ambil `msg_id`)
     * `POST /queue/ack` (acknowledge `msg_id`)
   * **Cache (MESI)**:

     * `POST /cache/put`
     * `GET  /cache/get?key=...`

> **Catatan penting:**
> * Untuk **ACK**, kirim `Ack` sebelum **visibility timeout** berakhir (default 15 detik).

---

## 8) Observabilitas (Metrics)

Setiap node mengekspor **Prometheus metrics** di:

```
GET /metrics
```

Metrik utama antara lain:

* **Raft**: `raft_role`, `raft_append_entries_total`, `raft_request_vote_total`
* **Lock**: `lock_acquire_total`, `lock_release_total`, `lock_held`
* **Queue**: `queue_publish_total`, `queue_consume_total{result=...}`, `queue_redeliver_total`, `queue_inflight`
* **Cache**: `cache_put_total`, `cache_get_total{hit=...}`, `cache_inval_total`, `cache_items`
* **Process**: `process_cpu_seconds_total`, `process_resident_memory_bytes`, dll.

---

## 9) Link Demo

https://youtu.be/jieXH2K_diw
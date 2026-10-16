# 🚀 kpub (Kafka Producer CLI)

**`kpub`** adalah CLI tool modern, ringan, dan berkinerja tinggi berbasis **Go (Golang)** untuk mempublikasikan pesan ke **Apache Kafka**.

Dibuat menggunakan driver **Pure Go (`twmb/franz-go`)** sehingga **CGO-Free** (tidak membutuhkan instalasi library C `librdkafka`), cepat di-compile, hemat memori, dan mudah di-distribusikan ke berbagai platform (macOS Apple Silicon/Intel, Linux, Windows).

---

## ✨ Fitur Utama

- **Zero CGO**: Pure Go, tanpa dependensi library C sistem (`librdkafka`).
- **Metode Input Fleksibel**:
  - Flag terminal langsung (`-m`, `-k`, `-H`)
  - File input (`-f`)
  - Pipa STDIN (`cat data.json | kpub publish -t topic`)
  - Mode interaktif / prompt wizard (`kpub interactive` / `kpub -i`)
- **Batch & Streaming Simulation**:
  - Pengiriman batch dari file baris-per-baris / JSON Lines (`--batch` / `.jsonl`)
  - Pengulangan pengiriman (`--repeat N`) dengan jeda (`--interval 500ms`) atau rate-limit (`--rate 10`)
- **Metadata Lengkap**:
  - Mendukung partition key (`-k`)
  - Custom headers (`-H "key=value"`)
  - Partisi spesifik (`-p 0`) atau auto hash partitioning
  - Validasi payload JSON (`--validate-json`)
- **Keamanan Lengkap**:
  - Plaintext (tanpa autentikasi)
  - SASL/PLAIN
  - SASL/SCRAM-SHA-256 & SASL/SCRAM-SHA-512
  - Enkripsi TLS/SSL & mutual TLS (mTLS)
- **Konfigurasi Mudah**:
  - Otomatis membaca file `.env` di direktori saat ini
  - Variabel lingkungan (*environment variables*)
  - Override langsung melalui flag CLI

---

## 🛠️ Instalasi & Kompilasi

### Prasyarat
- Go 1.20+

### Kompilasi Binary
```bash
# Clone atau masuk ke direktori proyek
cd "kafka-producer"

# Build binary ke folder bin/
make build

# Cek hasil build
./bin/kpub --help
```

### Install ke sistem (`$GOPATH/bin`)
```bash
make install
```

---

## 📖 Cara Penggunaan

### 1. Publish Single Message
```bash
kpub publish -t user-events -m '{"user_id": 101, "action": "checkout"}' -k "user-101"
```

### 2. Publish dengan Custom Headers
```bash
kpub publish -t audit-logs \
  -m '{"level": "INFO", "msg": "user signed in"}' \
  -k "audit-1" \
  -H "source=auth-service" \
  -H "env=production"
```

### 3. Publish dari File
```bash
# Satu file utuh sebagai 1 pesan
kpub publish -t order-created -f ./sample_order.json
```

### 4. Batch Publish dari File (JSON Lines / Multi-line)
```bash
# Kirim setiap baris dalam file sebagai pesan terpisah
kpub publish -t telemetry-data -f ./events.jsonl --batch
```

### 5. Pipe dari STDIN
```bash
echo '{"message": "piped from terminal"}' | kpub publish -t debug-topic
# atau
cat payload.json | kpub publish -t debug-topic
```

### 6. Streaming / Repeat Simulation Mode
```bash
# Kirim 10 pesan berulang setiap 500 milidetik
kpub publish -t sensor-stream -m '{"temp": 24.5}' --repeat 10 --interval 500ms

# Kirim dengan rate-limit (maksimal 20 pesan per detik)
kpub publish -t load-test -m '{"test": true}' --repeat 100 --rate 20
```

### 7. Mode Interaktif (Wizard Prompt)
```bash
kpub interactive
# atau
kpub -i
```
Menampilkan panduan form interaktif di terminal:
1. Memasukkan / memilih Broker Kafka
2. Memasukkan Topic tujuan
3. Memasukkan Partition (opsional)
4. Memasukkan Key (opsional)
5. Memasukkan Headers (opsional)
6. Memilih sumber payload (ketik/paste langsung atau ambil dari file)
7. Konfirmasi pengiriman sebelum dieksekusi

### 8. Ping / Cek Koneksi Broker
```bash
kpub ping -b localhost:9092
```

---

## ⚙️ Konfigurasi Environment (`.env`)

Anda dapat menyalin file `.env.example` menjadi `.env`:
```bash
cp .env.example .env
```

| Variabel | Flag CLI | Default | Deskripsi |
| :--- | :--- | :--- | :--- |
| `KAFKA_BROKERS` | `-b`, `--brokers` | `localhost:9092` | Alamat broker (pisahkan dengan koma) |
| `KAFKA_TOPIC` | `-t`, `--topic` | `""` | Nama default topic Kafka |
| `KAFKA_CLIENT_ID` | N/A | `kpub` | Client ID yang dikirim ke Kafka |
| `KAFKA_SASL_MECHANISM` | `--sasl-mechanism` | `none` | `none`, `plain`, `scram-sha-256`, `scram-sha-512` |
| `KAFKA_SASL_USERNAME` | `--sasl-username` | `""` | Username autentikasi SASL |
| `KAFKA_SASL_PASSWORD` | `--sasl-password` | `""` | Password autentikasi SASL |
| `KAFKA_TLS_ENABLED` | `--tls` | `false` | Aktifkan enkripsi TLS/SSL |
| `KAFKA_TLS_INSECURE_SKIP_VERIFY` | `--tls-insecure` | `false` | Abaikan verifikasi sertifikat SSL |
| `KAFKA_TLS_CA_CERT` | `--tls-ca` | `""` | Path ke file sertifikat CA kustom (PEM) |
| `KAFKA_TLS_CLIENT_CERT` | `--tls-cert` | `""` | Path ke sertifikat client untuk mTLS |
| `KAFKA_TLS_CLIENT_KEY` | `--tls-key` | `""` | Path ke private key client untuk mTLS |

---

## 🔒 Contoh Konfigurasi Keamanan

### Confluent Cloud / SASL PLAIN
```bash
kpub publish -t orders \
  -b "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092" \
  --sasl-mechanism plain \
  --sasl-username "<API_KEY>" \
  --sasl-password "<API_SECRET>" \
  --tls \
  -m '{"status": "ok"}'
```

### AWS MSK / SCRAM-SHA-512
```bash
kpub publish -t telemetry \
  -b "b-1.xxx.kafka.us-east-1.amazonaws.com:9096" \
  --sasl-mechanism scram-sha-512 \
  --sasl-username "msk-user" \
  --sasl-password "msk-secret" \
  --tls \
  -m '{"ping": true}'
```

---

## 🧪 Testing
```bash
make test
```

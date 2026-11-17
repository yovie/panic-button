# Panic Button — Live Tracking

Aplikasi web untuk memantau lokasi device panic button secara realtime, dengan dukungan
ingest Meshtastic (MQTT JSON), HTTP, dan simulator bawaan. Backend Rust (Axum), frontend
Leaflet + OpenStreetMap, semua berjalan lewat Docker Compose.

## Fitur

- Live tracking posisi device di peta (WebSocket, tanpa refresh).
- Alert panic realtime: marker merah berkedip, banner, auto-pan ke lokasi, dan suara.
- Daftar device dengan status, kecepatan, baterai, dan waktu terakhir terlihat.
- Ingest Meshtastic via MQTT (nodeinfo, position, telemetry, detection_sensor, text keyword).
- Ingest HTTP generik untuk perangkat lain/simulator.
- Simulator dua mode (HTTP dan Meshtastic MQTT) untuk demo tanpa hardware.
- Mode publik: tanpa login, CORS terbuka, broker MQTT anonim.

## Arsitektur

### Diagram Komponen

```text
                          ┌─────────────────── Docker Compose ───────────────────┐
                          │                                                      │
  Meshtastic node         │   ┌────────────┐        ┌───────────────────────┐   │
  (LoRa) ──> gateway ─────┼──>│ mosquitto  │──MQTT─>│ server (Rust/tokio)   │   │
  (ESP32, JSON MQTT)      │   │  :1883     │        │  Axum HTTP + WS       │   │
                          │   └────────────┘        │  MQTT subscriber      │   │
                          │                         │  Registry (in-memory) │   │
  simulator ──────────────┼── HTTP /api/ingest ────>│  broadcast channel    │   │
  (profile: sim)          │                         └───────────┬───────────┘   │
                          │                                     │               │
                          └─────────────────────────────────────┼───────────────┘
                                                                │ WebSocket /ws
                                                     ┌──────────▼──────────┐
                                                     │ Browser             │
                                                     │ Leaflet + OSM       │
                                                     └─────────────────────┘
```

### Komponen Utama

| Komponen | Teknologi | Peran |
|---|---|---|
| `server` | Rust 2024, Axum 0.8, tokio | HTTP API, WebSocket, serve UI statis, subscriber MQTT |
| `mosquitto` | Eclipse Mosquitto 2 | Broker MQTT anonim, menerima publish dari gateway/simulator |
| `simulator` | Rust (binary yang sama) | Device palsu: bergerak di rute, memicu panic/resolve |
| Browser | Leaflet 1.9, OpenStreetMap, vanilla JS | Peta live, marker, alert panic, suara, auto-reconnect |
| Gateway Meshtastic | ESP32 (T-Beam, Heltec, dll) | Menjembatani trafik LoRa mesh ke MQTT JSON |

### Alur Data

```text
1. Gateway Meshtastic publish JSON ke mosquitto: msh/<region>/2/json/<channel>/<userid>
2. server subscribe wildcard, mem-parse pesan menjadi event ternormalisasi
3. HTTP ingest (simulator/device lain) masuk ke registry yang sama
4. Registry menyimpan state terakhir per device di memori
5. Setiap perubahan di-broadcast ke semua browser lewat WebSocket
6. UI memperbarui marker, sidebar, dan alert panic tanpa refresh
```

Semua sumber data (MQTT, HTTP, dan nanti TCP Teltonika) bermuara pada satu
`Registry`. Dengan begitu penambahan protokol baru tidak mengubah API WebSocket
maupun frontend.

### Siklus Hidup Panic

```text
detection_sensor(state=true) ─┐
teks mengandung PANIC/SOS ────┼─> set_panic(id, true)  ─> status = panic
POST /api/devices/{id}/panic ─┘                          panic_since = sekarang
                                                         broadcast "panic"

detection_sensor(state=false) ─┐
POST /api/devices/{id}/resolve ─┼─> set_panic(id, false) ─> status = normal
event panic_clear (HTTP) ───────┘                           broadcast "panic_cleared"
```

- `panic` hanya di-broadcast sekali saat transisi `normal -> panic`; posisi tetap
  diperbarui selama panic tanpa memicu alert berulang.
- Jika panic tiba sebelum posisi device diketahui, event disimpan sebagai
  *pending panic* dan diterapkan saat posisi pertama masuk.
- Panic tidak hilang karena update posisi; harus di-resolve eksplisit.

### State Device

State disimpan in-memory di `Registry` (`RwLock<HashMap<String, Device>>`) dengan
metadata tambahan untuk nama dan pending panic.

| Field | Tipe | Keterangan |
|---|---|---|
| `id` | string | Node ID Meshtastic (`!a1b2c3d4`) atau ID bebas untuk HTTP |
| `name` | string | Nama dari `nodeinfo`, fallback ke ID |
| `lat`, `lon` | float | Posisi terakhir (derajat) |
| `speed_kmh`, `heading` | float/null | Kecepatan dan arah |
| `battery` | float/null | Baterai dari `telemetry` |
| `last_seen` | timestamp | Waktu update terakhir (UTC) |
| `status` | `normal` / `panic` | Status panic saat ini |
| `panic_since` | timestamp/null | Waktu panic dimulai |

### Protokol WebSocket

Client terhubung ke `/ws`. Saat connect server mengirim `snapshot`, lalu stream
perubahan. Semua pesan memakai field pembeda `type`.

```json
{ "type": "snapshot", "devices": [ { "id": "!0000c001", "name": "Sim Alpha", "lat": -6.1934, "lon": 106.8231, "speed_kmh": 40.6, "battery": 90.0, "last_seen": "2026-11-17T11:51:48.068Z", "status": "normal", "panic_since": null } ] }
```

| `type` | Payload | Kapan dikirim |
|---|---|---|
| `snapshot` | `devices[]` | Saat client connect / client tertinggal |
| `update` | `device` | Update posisi/telemetry/status normal |
| `panic` | `device` | Transisi normal -> panic |
| `panic_cleared` | `device` | Panic diselesaikan |

Fan-out memakai `tokio::sync::broadcast` (kapasitas 1024). Jika satu client
terlambat, server mengirim snapshot baru agar state tetap konsisten. Beberapa tab
browser dapat terhubung bersamaan.

### Pemetaan Pesan MQTT Meshtastic

Topik: `msh/<region>/2/json/<channel>/<userid>` (subscribe default
`msh/+/2/json/+/#`). Device ID diturunkan dari field `from` (desimal) menjadi
`!xxxxxxxx`.

| `type` | Field yang dipakai | Aksi |
|---|---|---|
| `nodeinfo` | `payload.longname` / `shortname` | Simpan nama device |
| `position` | `latitude_i`, `longitude_i` (× 1e-7), `ground_speed` (m/s), `ground_track`, `battery_level` | Update posisi (heartbeat) |
| `telemetry` | `payload.battery_level` | Update baterai |
| `detection_sensor` | `payload.state` / `detected` | Set panic / resolve |
| `text` | `payload.text` | Panic jika mengandung `PANIC`/`SOS` |

### Deployment

| Container | Image | Port internal | Port host (default) | Healthcheck |
|---|---|---|---|---|
| `server` | `panic-button-server:latest` (build lokal) | 8080 | `SERVER_PORT` (18080 via Makefile) | `GET /health` |
| `mosquitto` | `eclipse-mosquitto:2` | 1883 | `MQTT_PORT` (1883) | `mosquitto_sub` ke `$SYS` |
| `simulator` | sama dengan server | - | - | dinonaktifkan |

`server` dan `mosquitto` selalu berjalan; `simulator` memakai profile `sim` agar
tidak ikut di produksi. Resource limit per service dijelaskan di bagian
[Resource Limit](#resource-limit).

### Keputusan Desain dan Batasan

- **State in-memory** — cepat dan sederhana; restart server menghapus posisi/panic.
  Riwayat perjalanan dan audit belum disimpan (kandidat: SQLite/PostGIS).
- **Protokol-agnostic** — ingest MQTT dan HTTP memakai model event yang sama;
  listener TCP Teltonika/MQTT vendor lain bisa ditambahkan tanpa mengubah UI.
- **Publik tanpa auth** — dashboard, API panic/resolve, dan broker terbuka. Hanya
  untuk demo/operasi internal; lihat [Catatan Keamanan](#catatan-keamanan).
- **Tanpa database** — device baru muncul saat pesan pertama diterima; nama dari
  `nodeinfo` disimpan selama proses berjalan.

## Quick Start (Docker Compose)

```bash
docker compose up --build -d
```

Buka `http://<host>:8080`.

Demo dengan simulator (auto panic tiap 45 detik):

```bash
docker compose --profile sim up -d simulator
docker compose logs -f simulator
```

Simulator interaktif (ketik `p` lalu Enter untuk panic, `r` untuk resolve, `q` untuk keluar):

```bash
docker compose --profile sim run --rm -it simulator
```

Hentikan semua:

```bash
docker compose down -v
```

## Deploy ke Mesin Ini (Makefile)

```bash
make deploy-dev
```

Perintah ini build image, lalu menjalankan `mosquitto`, `server`, dan `simulator`
secara background. Web UI tersedia di `http://localhost:18080` dan broker MQTT di
`localhost:1883`. Port 18080 dipakai sebagai default karena port 8080 sudah dipakai
service lain di mesin ini.

```bash
make deploy-dev SERVER_PORT=8080   # jika port 8080 bebas
make ps                            # status container
make logs                          # ikuti log
make sim                           # simulator interaktif (p/r/q + Enter)
make stop-dev                      # hentikan service
make clean                         # hentikan + hapus volume
make check                         # cargo fmt + clippy + test
```

## Menghubungkan Device Meshtastic

1. Gunakan gateway berbasis **ESP32** (mis. T-Beam, Heltec V3, T-Deck). Board nRF52
   (RAK, T-Echo) tidak mendukung output JSON MQTT.
2. Aktifkan modul MQTT di gateway:
   - Server: IP/domain VPS, port `1883`
   - Username/password: kosong (broker anonim)
   - Uplink: aktif
   - JSON output: aktif
   - Channel/region: sesuaikan dengan mesh Anda
3. Server berlangganan wildcard `msh/+/2/json/+/#` secara default, jadi semua region dan
   channel akan diterima. Untuk membatasi, set `MQTT_TOPIC`, misal
   `msh/ID/2/json/Panic/#`.
4. Sumber panic yang didukung:
   - **Detection Sensor** (tombol GPIO) → paket `detection_sensor` dengan `state: true`.
   - **Pesan teks** yang mengandung keyword `PANIC` atau `SOS` (bisa diubah via
     `PANIC_KEYWORD`).
5. Posisi diambil dari paket `position`, nama device dari `nodeinfo`, baterai dari
   `telemetry`.

Verifikasi pesan masuk ke broker:

```bash
docker compose exec mosquitto mosquitto_sub -t 'msh/+/2/json/+/#' -v
```

Pastikan port `1883` VPS terbuka di firewall agar gateway bisa publish. Jika memakai
broker Meshtastic publik (`mqtt.meshtastic.org`), ganti `MQTT_URL` dan `MQTT_TOPIC`
sesuai region/channel yang dipakai.

## API

| Method | Path | Keterangan |
|---|---|---|
| POST | `/api/ingest` | Kirim event device (JSON) |
| GET | `/api/devices` | Snapshot semua device |
| POST | `/api/devices/{id}/panic` | Tandai device panic (lokasi terakhir) |
| POST | `/api/devices/{id}/resolve` | Selesaikan panic |
| GET | `/ws` | WebSocket realtime |
| GET | `/health` | Health check + jumlah device |

Contoh ingest:

```bash
curl -X POST http://localhost:8080/api/ingest \
  -H 'content-type: application/json' \
  -d '{
    "device_id": "!0000c001",
    "name": "Tim Alpha",
    "lat": -6.2,
    "lon": 106.8,
    "speed_kmh": 12.5,
    "heading": 90,
    "battery": 88,
    "event": "heartbeat"
  }'
```

`event` menerima `heartbeat`, `panic`, atau `panic_clear`.

## Konfigurasi

| Env | Default | Keterangan |
|---|---|---|
| `SERVER_PORT` | `8080` | Port host untuk web UI/API |
| `MQTT_PORT` | `1883` | Port host untuk broker MQTT |
| `RUST_LOG` | `info` | Level log server |
| `MQTT_ENABLED` | `true` | Aktifkan subscriber Meshtastic |
| `MQTT_URL` | `mqtt://mosquitto:1883` | URL broker |
| `MQTT_TOPIC` | `msh/+/2/json/+/#` | Topik langganan |
| `PANIC_KEYWORD` | `PANIC,SOS` | Keyword teks pemicu panic |

Salin `.env.example` menjadi `.env` jika ingin mengubah nilai default.

## Resource Limit

Dikonfigurasi di `docker-compose.yml` (`deploy.resources`):

| Service | CPU request | CPU limit | Memori request | Memori limit |
|---|---|---|---|---|
| server | 0.25 | 1.0 | 128 MB | 512 MB |
| mosquitto | 0.10 | 0.50 | 64 MB | 128 MB |
| simulator | 0.05 | 0.25 | 32 MB | 256 MB |

`limits` ditegakkan oleh Docker Compose v2 (`HostConfig`). `reservations` hanya
digunakan sebagai scheduling hint di Docker Swarm dan tidak dienforce di single host.

## Development Lokal

```bash
cargo run --bin server
cargo run --bin simulator -- --mode http --devices 3 --auto-panic 10
```

Server lokal default tidak menyalakan MQTT (`MQTT_ENABLED=false`). Untuk uji MQTT lokal:

```bash
docker compose up -d mosquitto
MQTT_ENABLED=true MQTT_URL=mqtt://localhost:1883 cargo run --bin server
cargo run --bin simulator -- --mode mqtt --mqtt mqtt://localhost:1883 --devices 3
```

Tes dan lint:

```bash
cargo test
cargo clippy --all-targets
cargo fmt --check
```

## Struktur Proyek

```
├── Cargo.toml
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── .env.example
├── mosquitto/mosquitto.conf
├── src/
│   ├── lib.rs            # Device registry, state panic, broadcast
│   ├── mqtt.rs           # Subscriber Meshtastic JSON
│   └── bin/
│       ├── server.rs     # HTTP API, WebSocket, static files
│       └── simulator.rs  # Simulator device (HTTP/MQTT)
└── static/
    ├── index.html
    ├── app.js
    └── style.css
```

## Catatan Keamanan

Mode ini sengaja dibuat publik: tanpa autentikasi, CORS `*`, dan broker MQTT anonim
tanpa TLS. Cocok untuk demo/operasi internal, tetapi jangan dipakai untuk data sensitif
di internet terbuka. Untuk produksi, tambahkan autentikasi, TLS, dan batasi topik broker.

## Troubleshooting

- **Port 8080/1883 sudah dipakai**: jalankan dengan `SERVER_PORT=18080 MQTT_PORT=11883 docker compose up -d`.
- **Device tidak muncul**: pastikan uplink + JSON MQTT aktif, topik cocok, region/channel
  benar, dan firewall VPS membuka port 1883. Board nRF52 tidak mendukung JSON MQTT.
- **Panic detection sensor tidak terdeteksi**: cek payload JSON dari
  `mosquitto_sub`; server menerima `state`, `detected`, atau menganggap detection
  sebagai panic jika field state tidak ada.
- **Suara tidak berbunyi**: browser memblokir audio sebelum interaksi pengguna; klik
  tombol `Suara: OFF` sekali untuk mengaktifkan.
- **Simulator di Docker tidak merespons tombol**: jalankan dengan `run --rm -it` agar
  stdin terhubung, lalu ketik `p`/`r`/`q` diikuti Enter.

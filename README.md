# Panic Button — Live Tracking

Aplikasi web untuk memantau lokasi device panic button secara realtime, dengan dukungan
ingest Meshtastic (MQTT JSON), HTTP, dan simulator bawaan. Backend Rust (Axum), frontend
Leaflet + OpenStreetMap, semua berjalan lewat Docker Compose.

## Fitur

- Live tracking posisi device di peta (WebSocket, tanpa refresh).
- Panduan setup device per tipe/protokol di `/panduan.html`: endpoint HTTP, topik MQTT,
  registrasi, panic/resolve, simulator, dan status dukungan Teltonika.
- Alert panic realtime: marker merah berkedip, banner, auto-pan ke lokasi, dan suara.
- Daftar device dengan status, kecepatan, baterai, dan waktu terakhir terlihat.
- Menu **Kelola Device**: registrasi device, label, tipe, protokol, serial/IMEI, pemilik,
  tag, catatan, dan posisi awal opsional. Konfigurasi tersimpan di JSON dan bertahan
  setelah restart.
- **IMEI sebagai Device ID** untuk protokol non-Meshtastic: Device ID boleh dikosongkan,
  IMEI 15–16 digit dipakai sebagai ID. Serial/IMEI unik dan ikut tercari di modal.
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
| Browser | Leaflet 1.9, OpenStreetMap, vanilla JS | Peta live, marker, alert panic, suara, auto-reconnect, modal Kelola Device |
| Gateway Meshtastic | ESP32 (T-Beam, Heltec, dll) | Menjembatani trafik LoRa mesh ke MQTT JSON |
| Device store | JSON (`data/devices.json`) | Konfigurasi device yang diregistrasi manual (label, tipe, protokol, metadata) |

### Alur Data

```text
1. Gateway Meshtastic publish JSON ke mosquitto: msh/<region>/2/json/<channel>/<userid>
2. server subscribe wildcard, mem-parse pesan menjadi event ternormalisasi
3. HTTP ingest (simulator/device lain) masuk ke registry yang sama
4. Device yang diregistrasi lewat UI dimuat dari data/devices.json saat startup
5. Registry menyimpan state terakhir per device di memori
6. Setiap perubahan di-broadcast ke semua browser lewat WebSocket
7. UI memperbarui marker, sidebar, dan alert panic tanpa refresh
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

State live disimpan in-memory di `Registry` (`RwLock<HashMap<String, Device>>`)
dengan metadata tambahan untuk nama dan pending panic. Konfigurasi yang dikelola
lewat UI disimpan terpisah di `DeviceStore` (`data/devices.json`) dan dimuat saat
startup, sehingga label/tipe/protokol tidak hilang saat restart.

| Field | Tipe | Keterangan |
|---|---|---|
| `id` | string | Node ID Meshtastic (`!a1b2c3d4`), IMEI, atau ID bebas (huruf, angka, `! - _ . :`, maks 64) |
| `name` | string | Nama dari `nodeinfo`, fallback ke ID |
| `label` | string/null | Label yang diberikan operator lewat Kelola Device; diprioritaskan di UI |
| `device_type` | enum | `panic_button` / `tracker` / `gateway` / `sensor` / `other` |
| `protocol` | enum | `meshtastic` / `http` / `simulator` / `teltonika` / `other` |
| `serial` | string/null | Serial/IMEI unik; untuk protokol non-Meshtastic dapat menjadi `id` |
| `owner`, `notes` | string/null | Pemilik/unit dan catatan bebas |
| `tags` | string[] | Tag untuk pengelompokan/pencarian (maks 32 karakter per tag) |
| `lat`, `lon` | float/null | Posisi terakhir (derajat); `null` jika device belum pernah mengirim posisi |
| `speed_kmh`, `heading` | float/null | Kecepatan dan arah |
| `battery` | float/null | Baterai dari `telemetry` |
| `last_seen` | timestamp/null | Waktu update terakhir (UTC); `null` untuk device yang baru diregistrasi |
| `status` | `normal` / `panic` | Status panic saat ini |
| `panic_since` | timestamp/null | Waktu panic dimulai |

`label` sengaja terpisah dari `name`: `name` berasal dari perangkat (`nodeinfo`),
sedangkan `label` adalah nama operasional yang bisa diubah kapan saja tanpa
menunggu device mengirim data.

### Protokol WebSocket

Client terhubung ke `/ws`. Saat connect server mengirim `snapshot`, lalu stream
perubahan. Semua pesan memakai field pembeda `type`.

```json
{ "type": "snapshot", "devices": [ { "id": "!0000c001", "name": "Sim Alpha", "label": "Tim Alpha", "device_type": "panic_button", "protocol": "meshtastic", "serial": "SN-1", "owner": "Satgas", "tags": ["regu-1"], "notes": null, "lat": -6.1934, "lon": 106.8231, "speed_kmh": 40.6, "battery": 90.0, "last_seen": "2026-11-17T11:51:48.068Z", "status": "normal", "panic_since": null } ] }
```

| `type` | Payload | Kapan dikirim |
|---|---|---|
| `snapshot` | `devices[]` | Saat client connect / client tertinggal |
| `update` | `device` | Update posisi/telemetry/metadata/status normal |
| `panic` | `device` | Transisi normal -> panic |
| `panic_cleared` | `device` | Panic diselesaikan |
| `removed` | `id` | Device dihapus lewat API/UI |

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
tidak ikut di produksi. Konfigurasi device disimpan pada volume Docker
`device-data` (`/app/data/devices.json`), sehingga `docker compose down` tanpa `-v`
tidak menghapus data. Resource limit per service dijelaskan di bagian
[Resource Limit](#resource-limit).

### Keputusan Desain dan Batasan

- **State live in-memory** — cepat dan sederhana; restart server menghapus posisi,
  baterai, dan panic. Konfigurasi device yang diregistrasi lewat UI tetap tersimpan
  di JSON. Riwayat perjalanan dan audit belum disimpan (kandidat: SQLite/PostGIS).
- **Config JSON + atomic write** — `DeviceStore` menulis ke file sementara lalu
  `rename`, sehingga file tidak korup jika proses berhenti di tengah penulisan.
- **Protokol-agnostic** — ingest MQTT dan HTTP memakai model event yang sama;
  listener TCP Teltonika/MQTT vendor lain bisa ditambahkan tanpa mengubah UI.
- **Publik tanpa auth** — dashboard, API panic/resolve/CRUD, dan broker terbuka.
  Hanya untuk demo/operasi internal; lihat [Catatan Keamanan](#catatan-keamanan).
- **Device otomatis vs terkelola** — device tetap muncul otomatis saat pesan pertama
  diterima; registrasi manual dipakai untuk memberi label/metadata sebelum device
  online atau untuk perangkat non-Meshtastic.

## Quick Start (Docker Compose)

```bash
docker compose up --build -d
```

Buka `http://<host>:8080` (lokal: <http://panicbutton.localhost:8080>). Panduan setup
device tersedia di `http://<host>:8080/panduan.html`.

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
secara background. Web UI tersedia di **<http://panicbutton.localhost:18080>**, panduan
di <http://panicbutton.localhost:18080/panduan.html>, dan broker MQTT di
`panicbutton.localhost:1883`. Port 18080 dipakai sebagai default karena port 80/8080
sudah dipakai service lain di mesin ini.

Domain `*.localhost` otomatis diarahkan ke loopback oleh browser dan systemd-resolved.
Jika `panicbutton.localhost` tidak bisa di-resolve di mesin Anda, tambahkan sekali
(butuh sudo):

```bash
echo '127.0.0.1 panicbutton.localhost' | sudo tee -a /etc/hosts
```

```bash
make deploy-dev                    # deploy dev di panicbutton.localhost:18080
make deploy-dev SERVER_PORT=8080   # jika port 8080 bebas
make deploy-dev DEV_DOMAIN=panicbutton.test   # ganti domain dev
make ps                            # status container
make logs                          # ikuti log
make sim                           # simulator interaktif (p/r/q + Enter)
make stop-dev                      # hentikan service
make clean                         # hentikan + hapus volume
make check                         # cargo fmt + clippy + test
```

## Deploy Staging via SSH

Target ini build image secara lokal, mengirim source dari working tree saat ini
dan image tersebut ke server staging, lalu menjalankan `mosquitto` + `server`
di sana (tanpa simulator):

```bash
make deploy-staging
```

Prasyarat: alias SSH `devserver` bisa dipakai (`ssh devserver`), `rsync` tersedia
di lokal dan server, Docker + Docker Compose di lokal dan server. Server staging
tidak perlu toolchain Rust karena image dikirim lewat `docker save | docker load`.
Default staging:

| Variabel | Default | Keterangan |
|---|---|---|
| `STAGING_HOST` | `devserver` | Alias/host SSH |
| `STAGING_DIR` | `panic-button-staging` | Direktori relatif terhadap home user SSH |
| `STAGING_PROJECT` | `panic-button-staging` | Nama project Docker Compose di staging |
| `STAGING_PORT` | `18080` | Port web UI/API di server staging |
| `STAGING_MQTT_PORT` | `1883` | Port broker MQTT di server staging |

Web UI dapat dibuka langsung di `http://devserver:18080` jika hostname tersebut
bisa di-resolve dari browser. Jika tidak, gunakan tunnel SSH:

```bash
ssh -L 18080:127.0.0.1:18080 devserver
# lalu buka http://localhost:18080
```

Deploy mengikuti isi working tree (termasuk perubahan yang belum di-commit),
tetapi `.env`, `.git`, `target`, dan `data` lokal tidak disalin. Data device
staging disimpan di volume Docker sehingga tetap ada saat container dibuat ulang.
Hentikan staging dengan:

```bash
make stop-staging
```

Override contoh:

```bash
make deploy-staging STAGING_PORT=18081 STAGING_MQTT_PORT=11883
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
6. Meshtastic **tidak memiliki IMEI**. Untuk node Meshtastic, wajib memakai Node ID
   `!xxxxxxxx` dari aplikasi Meshtastic (Node Info) sebagai Device ID; kolom Serial/IMEI
   boleh diisi nomor seri perangkat, bukan IMEI.

Verifikasi pesan masuk ke broker:

```bash
docker compose exec mosquitto mosquitto_sub -t 'msh/+/2/json/+/#' -v
```

Pastikan port `1883` VPS terbuka di firewall agar gateway bisa publish. Jika memakai
broker Meshtastic publik (`mqtt.meshtastic.org`), ganti `MQTT_URL` dan `MQTT_TOPIC`
sesuai region/channel yang dipakai.

> **Kompatibilitas firmware:** firmware Meshtastic 2.8 menghapus output JSON MQTT.
> Gunakan firmware 2.7.x atau bridge `meshtastic/mqtt-converter` agar server ini tetap
> menerima JSON. Server saat ini juga baru mendukung MQTT plaintext (`mqtt://`); MQTT
> TLS 8883 memerlukan konfigurasi broker dan dukungan transport TLS di server. Jika
> domain publik hanya mengekspos port 443, gateway tidak bisa terhubung langsung —
> gunakan VPN/MQTT Client Proxy atau buka port MQTT dengan pembatasan firewall.

## API

| Method | Path | Keterangan |
|---|---|---|
| POST | `/api/ingest` | Kirim event device (JSON) |
| GET | `/api/devices` | Snapshot semua device (live + terdaftar) |
| POST | `/api/devices` | Registrasi device baru; `id` opsional untuk non-Meshtastic jika IMEI diisi (`409` jika ID/IMEI sudah ada) |
| PATCH | `/api/devices/{id}` | Update label/tipe/protokol/metadata/posisi awal |
| DELETE | `/api/devices/{id}` | Hapus device dari registry dan penyimpanan |
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
    "device_type": "panic_button",
    "protocol": "http",
    "event": "heartbeat"
  }'
```

`event` menerima `heartbeat`, `panic`, atau `panic_clear`. `device_type`/`protocol`
opsional dan hanya dipakai saat device pertama kali dibuat; `protocol` default
`http` untuk ingest HTTP. `serial` opsional dan disimpan bila device belum punya
serial. Jika `device_id` kosong tetapi `serial` berisi IMEI 15–16 digit, server
memakai IMEI tersebut sebagai ID.

Registrasi device (posisi opsional, `lat` dan `lon` harus berpasangan):

Aturan identitas:
- **Meshtastic** — `id` wajib berupa Node ID `!xxxxxxxx`.
- **HTTP/Teltonika/simulator/lainnya** — `id` boleh dikosongkan jika `serial` berisi
  IMEI 15–16 digit; server memakai IMEI sebagai `id` (spasi dan tanda hubung
  dinormalisasi).
- Jika `id` diisi IMEI pada protokol non-Meshtastic dan `serial` kosong, `serial`
  diisi otomatis.
- Serial/IMEI harus unik; duplikat ditolak dengan `409`.

```bash
curl -X POST http://localhost:8080/api/devices \
  -H 'content-type: application/json' \
  -d '{
    "id": "!a1b2c3d4",
    "label": "Tim Alpha",
    "device_type": "panic_button",
    "protocol": "meshtastic",
    "serial": "SN-001",
    "owner": "Satgas",
    "tags": ["regu-1", "malam"],
    "notes": "Unit pengamanan lapangan",
    "lat": -6.2,
    "lon": 106.8
  }'
```

Contoh tracker seluler tanpa Device ID (IMEI menjadi ID):

```bash
curl -X POST http://localhost:8080/api/devices \
  -H 'content-type: application/json' \
  -d '{
    "label": "Truk Logistik 01",
    "device_type": "tracker",
    "protocol": "teltonika",
    "serial": "357 123456 789012",
    "owner": "Logistik"
  }'
```

Respons akan memiliki `id` dan `serial` bernilai `357123456789012`. Device harus
mengirim IMEI tersebut sebagai `device_id` pada ingest (atau `device_id` kosong +
`serial` IMEI).

Update metadata (kirim string kosong untuk menghapus nilai opsional; `tags`
menggantikan seluruh daftar tag):

```bash
curl -X PATCH http://localhost:8080/api/devices/!a1b2c3d4 \
  -H 'content-type: application/json' \
  -d '{"label": "Tim Alpha 2", "owner": "", "tags": ["regu-2"]}'
```

Hapus device:

```bash
curl -X DELETE http://localhost:8080/api/devices/!a1b2c3d4
```

Operasi tulis menyimpan perubahan ke `DEVICES_FILE` terlebih dahulu, lalu
memperbarui registry dan menyiarkan `update`/`removed` via WebSocket.

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
| `DEVICES_FILE` | `data/devices.json` | Lokasi penyimpanan konfigurasi device |

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

Server lokal dapat diakses di <http://panicbutton.localhost:8080>. Default tidak
menyalakan MQTT (`MQTT_ENABLED=false`). Untuk uji MQTT lokal:

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
│   ├── store.rs          # Penyimpanan konfigurasi device (JSON)
│   ├── mqtt.rs           # Subscriber Meshtastic JSON
│   └── bin/
│       ├── server.rs     # HTTP API, WebSocket, static files
│       └── simulator.rs  # Simulator device (HTTP/MQTT)
└── static/
    ├── index.html
    ├── panduan.html      # Panduan setup device
    ├── app.js
    └── style.css
```

## Catatan Keamanan

Mode ini sengaja dibuat publik: tanpa autentikasi, CORS `*`, dan broker MQTT anonim
tanpa TLS. Cocok untuk demo/operasi internal, tetapi jangan dipakai untuk data sensitif
di internet terbuka. Untuk produksi, tambahkan autentikasi, TLS, dan batasi topik broker.

## Troubleshooting

- **Port 8080/1883 sudah dipakai**: jalankan dengan `SERVER_PORT=18080 MQTT_PORT=11883 docker compose up -d`.
- **`panicbutton.localhost` tidak resolve**: browser modern dan systemd-resolved sudah
  memetakan `*.localhost` ke loopback; jika tidak, tambahkan `127.0.0.1 panicbutton.localhost`
  ke `/etc/hosts`. Untuk dev via Makefile gunakan port `18080` (`:8080` untuk `cargo run`).
- **Device tidak muncul**: pastikan uplink + JSON MQTT aktif, topik cocok, region/channel
  benar, dan firewall VPS membuka port 1883. Board nRF52 tidak mendukung JSON MQTT.
- **Panic detection sensor tidak terdeteksi**: cek payload JSON dari
  `mosquitto_sub`; server menerima `state`, `detected`, atau menganggap detection
  sebagai panic jika field state tidak ada.
- **Suara tidak berbunyi**: browser memblokir audio sebelum interaksi pengguna; klik
  tombol `Suara: OFF` sekali untuk mengaktifkan.
- **Tracker ditolak "Device ID wajib diisi"**: untuk perangkat non-Meshtastic, isi
  Serial/IMEI 15–16 digit dan kosongkan Device ID, atau isi Device ID manual.
- **`409 serial/IMEI already registered`**: IMEI sudah dipakai device lain. Cari IMEI
  tersebut di kolom pencarian modal Kelola Device (pencarian mencakup IMEI).
- **Simulator di Docker tidak merespons tombol**: jalankan dengan `run --rm -it` agar
  stdin terhubung, lalu ketik `p`/`r`/`q` diikuti Enter.

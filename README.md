# Panic Button — Live Tracking

Aplikasi web untuk memantau lokasi device panic button secara realtime, dengan dukungan
ingest Meshtastic (MQTT JSON), HTTP, dan simulator bawaan. Backend Rust (Axum), frontend
Leaflet + OpenStreetMap, semua berjalan lewat Docker Compose.

## Fitur

- Live tracking posisi device di peta (WebSocket, tanpa refresh).
- Panduan setup device per tipe/protokol di `/panduan.html`: endpoint HTTP, topik MQTT,
  registrasi, panic/resolve, simulator, dan status dukungan Teltonika.
- Alert panic realtime: marker merah berkedip, banner, auto-pan ke lokasi, dan suara.
- Indikator device: merah berkedip = panic, merah statis = tidak kirim data lebih dari
  1 jam (termasuk yang belum pernah kirim), biru/hijau = aktif.
- Daftar device dengan status, kecepatan, baterai, dan waktu terakhir terlihat.
- Menu **Kelola Device**: registrasi device, label, tipe, protokol, serial/IMEI, pemilik,
  tag, catatan, dan posisi awal opsional. Konfigurasi tersimpan di JSON dan bertahan
  setelah restart.
- **IMEI sebagai Device ID** untuk protokol non-Meshtastic: Device ID boleh dikosongkan,
  IMEI 15–16 digit dipakai sebagai ID. Serial/IMEI unik dan ikut tercari di modal.
- Ingest Meshtastic via MQTT (JSON dan protobuf: nodeinfo, position, telemetry,
  detection_sensor, text/alert keyword), termasuk listener TLS 8883 dengan
  username/password untuk development.
- Ingest HTTP generik untuk perangkat lain/simulator.
- Menu **Data Mentah**: mencatat payload MQTT/HTTP yang tidak cocok dengan adapter atau
  gagal diproses (decode/dekripsi), dikelompokkan per device dengan history payload,
  tersimpan di `data/unmatched.json` dengan batas 500 entri / 50 per device / 8 KB / 7 hari.
  Pesan kontrol OwnTracks (`_type: lwt`, *Last Will and Testament*) dikenali adapter dan
  diabaikan, sehingga tidak tercatat sebagai `no_adapter`.
- Simulator dua mode (HTTP dan Meshtastic MQTT) untuk demo tanpa hardware.
- Mode publik: tanpa login dan CORS terbuka; broker MQTT `1883` anonim untuk lokal,
  `8883` TLS + auth untuk device eksternal.

## Arsitektur

### Diagram Komponen

```text
                          ┌─────────────────── Docker Compose ───────────────────┐
                          │                                                      │
  Meshtastic node         │   ┌────────────┐        ┌───────────────────────┐   │
  (LoRa) ──> gateway ─────┼──>│ mosquitto  │──MQTT─>│ server (Rust/tokio)   │   │
  (ESP32, JSON MQTT)      │   │ 1883/8883  │        │  Axum HTTP + WS       │   │
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
| `mosquitto` | Eclipse Mosquitto 2 | Broker MQTT: 1883 anonim (internal/lokal), 8883 TLS + auth (device eksternal), 9001 WebSocket (MQTT over WS) |
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

Server mendukung JSON dan protobuf:

- JSON: `msh/<region>/2/json/<channel>/<userid>`
- Protobuf: `msh/<region>/2/e/<channel>/<userid>` (legacy `/2/c/`)
- Subscribe default: `#`. Device ID diturunkan dari field `from` (desimal) menjadi
  `!xxxxxxxx`.

| Format | `type` / portnum | Field yang dipakai | Aksi |
|---|---|---|---|
| JSON | `nodeinfo` | `payload.longname` / `shortname` | Simpan nama device |
| JSON | `position` | `latitude_i`, `longitude_i` (× 1e-7), `ground_speed` (m/s), `ground_track`, `battery_level` | Update posisi (heartbeat) |
| JSON | `telemetry` | `payload.battery_level` | Update baterai |
| JSON | `detection_sensor` | `payload.state` / `detected` | Set panic / resolve |
| JSON | `text` | `payload.text` | Panic jika mengandung `PANIC`/`SOS` |
| Proto | `POSITION_APP` | `latitude_i`, `longitude_i` (× 1e-7), `ground_speed` (m/s), `ground_track` (1/100°) | Update posisi (heartbeat) |
| Proto | `NODEINFO_APP` | `long_name` / `short_name` | Simpan nama device |
| Proto | `TELEMETRY_APP` | `device_metrics.battery_level` | Update baterai |
| Proto | `TEXT_MESSAGE_APP` | payload UTF-8 | Panic jika mengandung `PANIC`/`SOS` |
| Proto | `ALERT_APP`, `DETECTION_SENSOR_APP` | payload | Set panic |
| Proto | `/PKI/` | terenkripsi X25519 | Dilewati; tidak bisa didekripsi tanpa private key node |

### Deployment

| Container | Image | Port internal | Port host (default) | Healthcheck |
|---|---|---|---|---|
| `server` | `panic-button-server:latest` (build lokal) | 8080 | `SERVER_PORT` (18080 via Makefile) | `GET /health` |
| `mosquitto` | `eclipse-mosquitto:2` | 1883, 8883 | `MQTT_PORT` (1883), `MQTT_TLS_PORT` (8883) | `mosquitto_sub` ke `$SYS` |
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

Buka `http://<host>:8080` (lokal: <http://localhost:8080>). Panduan setup device
tersedia di `http://<host>:8080/panduan.html`. Untuk dev stack Makefile, gunakan
<http://panicbutton.localhost> tanpa port (nginx mem-proxy ke `127.0.0.1:18080`).

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
secara background. Web UI tersedia di **<http://panicbutton.localhost>** (tanpa port),
panduan di <http://panicbutton.localhost/panduan.html>, dan broker MQTT di
`panicbutton.localhost:1883`. Container `server` tetap listen di port host 18080,
tetapi diakses lewat vhost nginx `nginx/panicbutton.localhost.conf` yang mem-proxy
ke `127.0.0.1:18080`.

Aktifkan vhost sekali (butuh sudo):

```bash
make nginx
```

Atau manual:

```bash
sudo ln -sfn "$PWD/nginx/panicbutton.localhost.conf" \
  /etc/nginx/sites-enabled/panicbutton.localhost.conf
sudo nginx -t && sudo systemctl reload nginx
```

Domain `*.localhost` otomatis diarahkan ke loopback oleh browser dan systemd-resolved.
Jika `panicbutton.localhost` tidak bisa di-resolve di mesin Anda, tambahkan sekali
(butuh sudo):

```bash
echo '127.0.0.1 panicbutton.localhost' | sudo tee -a /etc/hosts
```

Jika `SERVER_PORT` diubah, sesuaikan `proxy_pass` di vhost nginx agar tetap cocok.

```bash
make deploy-dev                    # deploy dev di http://panicbutton.localhost
make deploy-dev MQTT_PORT=11883    # jika port MQTT 1883 sudah dipakai
make nginx                         # aktifkan vhost nginx (sekali saja)
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
| `STAGING_MQTT_PORT` | `1883` | Port broker MQTT plaintext di server staging |
| `STAGING_MQTT_TLS_PORT` | `8883` | Port broker MQTT TLS di server staging |

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
   (RAK, T-Echo) tidak menyediakan modul MQTT.
2. Aktifkan modul MQTT di gateway:
   - Server: untuk dev gunakan `mqtt.jokocloud.my.id` port `8883` dengan TLS aktif;
     untuk publik gunakan IP/domain VPS port `1883` (plaintext) atau `8883` (TLS)
     setelah port dibuka
   - Username/password: `meshtastic` dan isi `mosquitto/.mqtt-password` untuk dev;
     untuk broker publik gunakan kredensial broker Anda
   - Uplink: aktif
   - Channel/region: sesuaikan dengan mesh Anda
3. Server berlangganan wildcard `#` secara default, sehingga menerima JSON maupun
   protobuf. Untuk membatasi, set `MQTT_TOPIC`, misal `msh/ID/2/#` atau
   `msh/ID/2/json/Panic/#`.
4. Server mendukung dua format MQTT Meshtastic:
   - **JSON** (`msh/<region>/2/json/<channel>/<node>`) untuk firmware 2.7.x dengan
     *JSON Output* aktif.
   - **Protobuf** (`msh/<region>/2/e/<channel>/<node>`, legacy `/2/c/`) untuk firmware
     2.8+ atau saat JSON Output dimatikan. Paket `decoded` langsung diproses; paket
     channel terenkripsi didekripsi dengan PSK default `AQ==` atau `MESHTASTIC_PSK`.
   - **PKI** (`/2/e/PKI/<node>`) memakai X25519 + AES-256-CCM dan **tidak bisa
     didekripsi** tanpa private key node. Server hanya mencatat peringatan dan
     melewatinya; minta node mengirim posisi sebagai broadcast di channel biasa.
5. Sumber panic yang didukung:
   - **Detection Sensor** (tombol GPIO) → paket `detection_sensor` dengan `state: true`
     (JSON) atau `DETECTION_SENSOR_APP` (protobuf).
   - **Pesan teks** yang mengandung keyword `PANIC` atau `SOS` (bisa diubah via
     `PANIC_KEYWORD`).
   - **Alert** (`ALERT_APP`) di protobuf selalu dianggap panic.
6. Posisi diambil dari paket `position`, nama device dari `nodeinfo`, baterai dari
   `telemetry`.
7. Meshtastic **tidak memiliki IMEI**. Untuk node Meshtastic, wajib memakai Node ID
   `!xxxxxxxx` dari aplikasi Meshtastic (Node Info) sebagai Device ID; kolom Serial/IMEI
   boleh diisi nomor seri perangkat, bukan IMEI.

Verifikasi pesan masuk ke broker:

```bash
docker compose exec mosquitto mosquitto_sub -t 'msh/#' -v
```

Pastikan port MQTT (`1883` plaintext atau `8883` TLS) terbuka di firewall agar gateway
bisa publish. Jika memakai broker Meshtastic publik (`mqtt.meshtastic.org`), ganti
`MQTT_URL` dan `MQTT_TOPIC` sesuai region/channel yang dipakai.

### PSK channel

PSK default Meshtastic adalah `AQ==`. Jika channel memakai PSK custom, isi
`MESHTASTIC_PSK` dengan nilai base64 dari aplikasi (Settings → Radio Config →
Channels → PSK) atau CLI:

```bash
meshtastic --ch-index 0 --ch-get psk
```

Beberapa PSK dapat dipisahkan dengan koma, misalnya
`MESHTASTIC_PSK=AQ==,BASE64_PSK_LAIN`. Nilai 1-byte (`AQ==`, `Ag==`, dst.) diperluas
sesuai aturan firmware Meshtastic.

> **Kompatibilitas firmware:** firmware Meshtastic 2.8 menghapus output JSON MQTT,
> tetapi server ini tetap bisa menerima topik protobuf `/2/e/` dan `/2/c/`. Bridge
> `meshtastic/mqtt-converter` hanya diperlukan jika ingin memaksa format JSON. Di dev
> stack, Mosquitto sudah menyediakan listener TLS 8883 dengan auth untuk device; server
> tetap berlangganan lewat `1883` di jaringan Docker dan belum mendukung `mqtts://`
> untuk broker remote. Jika domain publik hanya mengekspos port 443, gateway tidak bisa
> terhubung langsung — gunakan VPN/MQTT Client Proxy atau buka port MQTT dengan
> pembatasan firewall.

## API

| Method | Path | Keterangan |
|---|---|---|
| POST | `/api/ingest` | Kirim event device (JSON) |
| GET/POST | `/api/ingest/auto` | Auto-detect format tracker (OwnTracks, OsmAnd, Traccar, TTN, Generic) |
| GET | `/api/devices` | Snapshot semua device (live + terdaftar) |
| POST | `/api/devices` | Registrasi device baru; `id` opsional untuk non-Meshtastic jika IMEI diisi (`409` jika ID/IMEI sudah ada) |
| PATCH | `/api/devices/{id}` | Update label/tipe/protokol/metadata/posisi awal |
| DELETE | `/api/devices/{id}` | Hapus device dari registry dan penyimpanan |
| POST | `/api/devices/{id}/panic` | Tandai device panic (lokasi terakhir) |
| POST | `/api/devices/{id}/resolve` | Selesaikan panic |
| GET | `/api/unmatched` | Daftar device dengan payload yang belum didukung |
| GET | `/api/unmatched/payloads?key=...` | History payload untuk satu device (terbaru dulu) |
| DELETE | `/api/unmatched?key=...` | Hapus history satu device; tanpa `key` menghapus semua |
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
| `SERVER_PORT` | `8080` | Port host container server (Makefile: 18080, diakses via nginx) |
| `MQTT_PORT` | `1883` | Port host broker MQTT plaintext (lokal/Docker) |
| `MQTT_TLS_PORT` | `8883` | Port host broker MQTT TLS + auth |
| `MQTT_WS_PORT` | `9001` | Port host broker MQTT over WebSocket |
| `RUST_LOG` | `info` | Level log server |
| `MQTT_ENABLED` | `true` | Aktifkan subscriber Meshtastic |
| `MQTT_URL` | `mqtt://mosquitto:1883` | URL broker |
| `MQTT_TOPIC` | `#` | Topik langganan (universal adapter) |
| `PANIC_KEYWORD` | `PANIC,SOS` | Keyword teks pemicu panic |
| `MESHTASTIC_PSK` | `AQ==` | PSK channel protobuf, base64; pisahkan dengan koma untuk beberapa channel |
| `DEVICES_FILE` | `data/devices.json` | Lokasi penyimpanan konfigurasi device |
| `UNMATCHED_FILE` | `data/unmatched.json` | File history payload yang belum didukung |
| `UNMATCHED_MAX` | `500` | Maksimum total payload yang disimpan |
| `UNMATCHED_PER_DEVICE` | `50` | Maksimum payload per device |
| `UNMATCHED_MAX_BYTES` | `8192` | Ukuran payload mentah sebelum dipotong |
| `UNMATCHED_TTL_DAYS` | `7` | Usia maksimum payload (hari) |

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

Server lokal (`cargo run`) dapat diakses langsung di <http://localhost:8080>. Jika dev
stack Docker/Makefile berjalan, gunakan <http://panicbutton.localhost> (nginx mem-proxy
ke port 18080). Default tidak menyalakan MQTT (`MQTT_ENABLED=false`). Untuk uji MQTT
lokal:

```bash
docker compose up -d mosquitto
MQTT_ENABLED=true MQTT_URL=mqtt://localhost:1883 cargo run --bin server
cargo run --bin simulator -- --mode mqtt --mqtt mqtt://localhost:1883 --devices 3
```

### MQTT TLS Lokal

Mosquitto di dev stack membuka dua listener:

- `1883` — plaintext dan anonim; dipakai `server` dan `simulator` di jaringan Docker.
- `8883` — TLS dengan username/password; untuk device atau klien eksternal.

Buat sertifikat dan kredensial sekali (butuh `openssl` dan Docker):

```bash
make mqtt-setup
```

`make deploy-dev` juga menjalankan langkah ini otomatis. File yang dihasilkan bersifat
gitignored:

- `mosquitto/certs/server.crt` dan `server.key` — self-signed, SAN `mqtt.jokocloud.my.id`,
  `localhost`, dan `127.0.0.1`.
- `mosquitto/passwd` — hash password untuk user `meshtastic`.
- `mosquitto/.mqtt-password` — password plaintext; baca dengan
  `cat mosquitto/.mqtt-password`.

Tes subscribe lewat TLS:

```bash
docker exec -it panic-button-mosquitto-1 mosquitto_sub \
  -h 127.0.0.1 -p 8883 \
  --cafile /mosquitto/certs/server.crt \
  -u meshtastic -P "$(cat mosquitto/.mqtt-password)" \
  -t 'msh/+/2/json/+/#' -v
```

Tes publish posisi lewat TLS:

```bash
docker exec -it panic-button-mosquitto-1 mosquitto_pub \
  -h 127.0.0.1 -p 8883 \
  --cafile /mosquitto/certs/server.crt \
  -u meshtastic -P "$(cat mosquitto/.mqtt-password)" \
  -t 'msh/ID/2/json/Panic/!test' \
  -m '{"from":53261,"type":"position","payload":{"latitude_i":-62000000,"longitude_i":1068000000}}'
```

Catatan:

- Sertifikat self-signed. Firmware Meshtastic memakai TLS tanpa validasi sertifikat
  (`setInsecure`), jadi tetap bisa terhubung. Untuk `mosquitto_sub`, gunakan `--cafile`;
  `--insecure` hanya untuk tes cepat.
- Device di jaringan LAN yang sama dapat memakai `IP-LAN:8883` dengan TLS aktif dan
  kredensial yang sama.
- Port 8883 belum diekspos ke internet. Cloudflare Tunnel hanya mem-proxy HTTP/HTTPS
  dan origin berada di belakang NAT; jangan arahkan gateway publik ke port ini sebelum
  ada port-forward/UFW atau broker cloud.
- Untuk mengganti password, hapus `mosquitto/.mqtt-password` dan `mosquitto/passwd`,
  lalu jalankan `make mqtt-setup` lagi.

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
├── mosquitto/
│   ├── mosquitto.conf
│   └── certs/            # dibuat oleh make mqtt-setup (gitignored)
├── nginx/panicbutton.localhost.conf  # Vhost lokal, proxy port 80 -> 18080
├── scripts/mqtt-setup.sh # Generate sertifikat + kredensial MQTT TLS
├── src/
│   ├── lib.rs            # Device registry, state panic, broadcast
│   ├── store.rs          # Penyimpanan konfigurasi device (JSON)
│   ├── raw_capture.rs    # History payload yang belum didukung (JSON)
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

Mode ini sengaja dibuat publik: tanpa autentikasi pada dashboard/API dan CORS `*`.
Listener MQTT `1883` tetap anonim dan hanya untuk jaringan internal/lokal; listener
`8883` memakai TLS + username/password. Cocok untuk demo/operasi internal, tetapi
jangan dipakai untuk data sensitif di internet terbuka. Untuk produksi, tambahkan
autentikasi dashboard, TLS end-to-end, dan batasi topik broker dengan ACL.

## Troubleshooting

- **Port 8080/1883/8883 sudah dipakai**: jalankan dengan
  `SERVER_PORT=18080 MQTT_PORT=11883 MQTT_TLS_PORT=18883 docker compose up -d`.
- **Mosquitto gagal start / `Unable to open pwfile`**: jalankan `make mqtt-setup`
  untuk membuat `mosquitto/passwd` dan sertifikat TLS.
- **TLS ditolak `not authorised`**: username/password salah. Cek
  `mosquitto/.mqtt-password`; untuk merotasi, hapus `mosquitto/passwd` dan
  `mosquitto/.mqtt-password`, lalu jalankan `make mqtt-setup`.
- **`certificate verify failed` saat tes lokal**: gunakan
  `--cafile mosquitto/certs/server.crt` atau `--insecure` untuk tes cepat.
- **`panicbutton.localhost` tidak resolve atau 502**: pastikan vhost nginx aktif
  (`make nginx`), container server sehat (`make ps`), dan `proxy_pass` di
  `nginx/panicbutton.localhost.conf` cocok dengan `SERVER_PORT` (default 18080).
  Browser modern dan systemd-resolved biasanya sudah memetakan `*.localhost` ke
  loopback; jika tidak, tambahkan `127.0.0.1 panicbutton.localhost` ke `/etc/hosts`.
- **Device tidak muncul**: pastikan uplink + JSON MQTT aktif, topik cocok, region/channel
  benar, dan firewall VPS membuka port MQTT (`1883`/`8883`). Board nRF52 tidak
  mendukung JSON MQTT.
- **Data Mentah berisi `{"_type":"lwt"}` dari OwnTracks**: itu pesan *Last Will and
  Testament* yang dikirim saat koneksi perangkat ke broker terputus, bukan data posisi.
  Server mengabaikannya sebagai pesan kontrol; entri lama bisa dihapus lewat
  `DELETE /api/unmatched?key=<key yang tampil di menu>`.
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

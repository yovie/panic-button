use axum::{
    Json, Router,
    extract::{
        Path, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    routing::{get, patch, post},
};
use panic_button::adapters::HttpAdapter;
use panic_button::{
    Device, DeviceConfig, DeviceType, IngestEvent, Protocol, Registry, ServerMessage, adapters,
    mqtt, normalize_imei,
    raw_capture::{CaptureSource, RawCaptureLimits, RawCaptureStore, RawDevice, RawPayload},
    store::DeviceStore,
    valid_device_id, valid_position,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::HashSet, env, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::{Mutex, broadcast::error::RecvError};
use tower_http::{cors::CorsLayer, services::ServeDir, trace::TraceLayer};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AppState {
    registry: Arc<Registry>,
    store: Arc<Mutex<DeviceStore>>,
    http_adapters: Arc<Vec<Box<dyn HttpAdapter>>>,
    raw: Arc<RawCaptureStore>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let registry = Registry::shared();

    let devices_file = env_or("DEVICES_FILE", "data/devices.json");
    let store = DeviceStore::load(&devices_file).await;
    for config in store.configs() {
        registry.register(config).await;
    }
    info!(
        path = %devices_file,
        managed_devices = store.len(),
        "device store loaded"
    );

    let raw_file = env_or("UNMATCHED_FILE", "data/unmatched.json");
    let raw = Arc::new(
        RawCaptureStore::load(
            &raw_file,
            RawCaptureLimits {
                max_entries: env_usize("UNMATCHED_MAX", 500),
                max_per_device: env_usize("UNMATCHED_PER_DEVICE", 50),
                max_bytes: env_usize("UNMATCHED_MAX_BYTES", 8192),
                ttl: Duration::from_secs(env_u64("UNMATCHED_TTL_DAYS", 7) * 24 * 60 * 60),
            },
        )
        .await,
    );
    info!(
        path = %raw_file,
        entries = raw.count().await,
        "raw capture store loaded"
    );

    if env_flag("MQTT_ENABLED", false) {
        let panic_keywords: Vec<String> = env_or("PANIC_KEYWORD", "PANIC,SOS")
            .split(',')
            .map(str::trim)
            .filter(|keyword| !keyword.is_empty())
            .map(str::to_string)
            .collect();
        let meshtastic_psks: Vec<String> = env_or("MESHTASTIC_PSK", "AQ==")
            .split(',')
            .map(str::trim)
            .filter(|psk| !psk.is_empty())
            .map(str::to_string)
            .collect();
        let config = mqtt::MqttConfig {
            url: env_or("MQTT_URL", "mqtt://localhost:1883"),
            topic: env_or("MQTT_TOPIC", "#"),
            panic_keywords: panic_keywords.clone(),
            client_id: format!("panic-button-{}", std::process::id()),
        };
        let mqtt_registry = registry.clone();
        let mqtt_adapters = adapters::mqtt_adapters(panic_keywords, meshtastic_psks);
        let mqtt_raw = raw.clone();
        tokio::spawn(async move {
            if let Err(error) =
                adapters::run_mqtt_universal(mqtt_registry, config, mqtt_adapters, mqtt_raw).await
            {
                warn!(%error, "MQTT adapter subscriber stopped");
            }
        });
    }

    let state = AppState {
        registry: registry.clone(),
        store: Arc::new(Mutex::new(store)),
        http_adapters: Arc::new(adapters::http_adapters()),
        raw,
    };

    let static_dir = env_or("STATIC_DIR", "static");

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/ingest", post(ingest))
        .route(
            "/api/ingest/auto",
            get(ingest_auto_get).post(ingest_auto_post),
        )
        .route("/api/devices", get(list_devices).post(create_device))
        .route(
            "/api/devices/{id}",
            patch(update_device).delete(delete_device),
        )
        .route("/api/devices/{id}/panic", post(trigger_panic))
        .route("/api/devices/{id}/resolve", post(resolve_panic))
        .route(
            "/api/unmatched",
            get(list_unmatched).delete(clear_unmatched),
        )
        .route("/api/unmatched/payloads", get(unmatched_payloads))
        .route("/ws", get(ws_handler))
        .fallback_service(ServeDir::new(&static_dir).append_index_html_on_directories(true))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let port: u16 = env_or("PORT", "8080").parse()?;
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "panic-button server listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "status": "ok",
        "devices": state.registry.snapshot().await.len(),
    }))
}

async fn ingest(
    State(state): State<AppState>,
    Json(event): Json<IngestEvent>,
) -> Result<Json<Device>, (StatusCode, String)> {
    apply_ingested_event(&state, event).await
}

async fn capture_http_failure(state: &AppState, topic: &str, bytes: Vec<u8>) {
    let captured = state
        .raw
        .record(CaptureSource::Http, topic, "http_no_adapter", &bytes)
        .await;
    state
        .registry
        .notify_raw(captured.device_key, state.raw.count().await);
}

async fn ingest_auto_get(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Device>, (StatusCode, String)> {
    let raw_bytes = serde_json::to_vec(&json!({ "query": &query })).unwrap_or_default();
    let request = adapters::HttpRequest { query, body: None };
    let event = match adapters::detect_http(&request, &state.http_adapters) {
        Some(event) => event,
        None => {
            capture_http_failure(&state, "/api/ingest/auto", raw_bytes).await;
            return Err((StatusCode::BAD_REQUEST, "no adapter matched".to_string()));
        }
    };
    apply_ingested_event(&state, event).await
}

async fn ingest_auto_post(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<std::collections::HashMap<String, String>>,
    Json(body): Json<Value>,
) -> Result<Json<Device>, (StatusCode, String)> {
    let raw_bytes =
        serde_json::to_vec(&json!({ "query": &query, "body": &body })).unwrap_or_default();
    let request = adapters::HttpRequest {
        query,
        body: Some(body),
    };
    let event = match adapters::detect_http(&request, &state.http_adapters) {
        Some(event) => event,
        None => {
            capture_http_failure(&state, "/api/ingest/auto", raw_bytes).await;
            return Err((StatusCode::BAD_REQUEST, "no adapter matched".to_string()));
        }
    };
    apply_ingested_event(&state, event).await
}

async fn apply_ingested_event(
    state: &AppState,
    mut event: IngestEvent,
) -> Result<Json<Device>, (StatusCode, String)> {
    let mut id = event.device_id.trim().to_string();
    let serial = clean_serial(event.serial.take());
    if id.is_empty()
        && let Some(imei) = serial.as_deref().and_then(normalize_imei)
    {
        id = imei;
    }
    if id.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "device_id wajib diisi, atau sediakan serial/IMEI 15-16 digit".to_string(),
        ));
    }
    if !valid_device_id(&id) {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid device_id: use letters, digits, ! - _ . : (max 64)".to_string(),
        ));
    }
    if !valid_position(event.lat, event.lon) {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid coordinates".to_string(),
        ));
    }

    event.device_id = id;
    if event.protocol.is_none() {
        event.protocol = Some(Protocol::Http);
    }
    event.serial = match serial {
        Some(serial) => Some(serial),
        None if event.protocol != Some(Protocol::Meshtastic) => normalize_imei(&event.device_id),
        None => None,
    };

    Ok(Json(state.registry.apply(event).await))
}

async fn list_devices(State(state): State<AppState>) -> Json<Vec<Device>> {
    Json(state.registry.snapshot().await)
}

#[derive(Debug, Deserialize)]
struct RawKeyQuery {
    key: Option<String>,
}

async fn list_unmatched(State(state): State<AppState>) -> Json<Vec<RawDevice>> {
    Json(state.raw.devices().await)
}

async fn unmatched_payloads(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<RawKeyQuery>,
) -> Json<Vec<RawPayload>> {
    Json(state.raw.history(query.key.as_deref().unwrap_or("")).await)
}

async fn clear_unmatched(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<RawKeyQuery>,
) -> Json<Value> {
    let removed = match query.key {
        Some(key) => state.raw.clear(&key).await,
        None => state.raw.clear_all().await,
    };
    state
        .registry
        .notify_raw(String::new(), state.raw.count().await);
    Json(json!({ "removed": removed }))
}

#[derive(Debug, Deserialize)]
struct CreateDeviceRequest {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    device_type: Option<DeviceType>,
    #[serde(default)]
    protocol: Option<Protocol>,
    #[serde(default)]
    serial: Option<String>,
    #[serde(default)]
    owner: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    notes: Option<String>,
    #[serde(default)]
    lat: Option<f64>,
    #[serde(default)]
    lon: Option<f64>,
}

async fn create_device(
    State(state): State<AppState>,
    Json(request): Json<CreateDeviceRequest>,
) -> Result<(StatusCode, Json<Device>), (StatusCode, String)> {
    let protocol = request.protocol.unwrap_or_default();
    let (id, serial) = resolve_identity(protocol, request.id, request.serial)
        .map_err(|message| (StatusCode::BAD_REQUEST, message))?;

    if state.registry.device(&id).await.is_some() {
        return Err((StatusCode::CONFLICT, "device already exists".to_string()));
    }
    if let Some(serial) = serial.as_deref()
        && serial_in_use(&state, serial, None).await
    {
        return Err((
            StatusCode::CONFLICT,
            "serial/IMEI already registered".to_string(),
        ));
    }

    let (lat, lon) = position_pair(request.lat, request.lon)?;
    let config = DeviceConfig {
        id,
        label: clean_optional(request.label),
        device_type: request.device_type.unwrap_or_default(),
        protocol,
        serial,
        owner: clean_optional(request.owner),
        tags: clean_tags(request.tags),
        notes: clean_optional(request.notes),
        lat,
        lon,
    };

    persist_upsert(&state, config.clone()).await?;
    let device = state.registry.register(config).await;
    Ok((StatusCode::CREATED, Json(device)))
}

#[derive(Debug, Default, Deserialize)]
struct UpdateDeviceRequest {
    label: Option<String>,
    device_type: Option<DeviceType>,
    protocol: Option<Protocol>,
    serial: Option<String>,
    owner: Option<String>,
    tags: Option<Vec<String>>,
    notes: Option<String>,
    lat: Option<f64>,
    lon: Option<f64>,
}

async fn update_device(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<UpdateDeviceRequest>,
) -> Result<Json<Device>, (StatusCode, String)> {
    let Some(existing) = state.registry.device(&id).await else {
        return Err((StatusCode::NOT_FOUND, "device not found".to_string()));
    };

    let mut config = DeviceConfig {
        id: existing.id.clone(),
        label: existing.label.clone(),
        device_type: existing.device_type,
        protocol: existing.protocol,
        serial: existing.serial.clone(),
        owner: existing.owner.clone(),
        tags: existing.tags.clone(),
        notes: existing.notes.clone(),
        lat: existing.lat,
        lon: existing.lon,
    };

    if let Some(device_type) = request.device_type {
        config.device_type = device_type;
    }
    if let Some(protocol) = request.protocol {
        config.protocol = protocol;
    }
    if let Some(label) = request.label {
        config.label = clean_optional(Some(label));
    }
    if let Some(serial) = request.serial {
        config.serial = clean_serial(Some(serial));
    }
    if let Some(owner) = request.owner {
        config.owner = clean_optional(Some(owner));
    }
    if let Some(notes) = request.notes {
        config.notes = clean_optional(Some(notes));
    }
    if let Some(tags) = request.tags {
        config.tags = clean_tags(tags);
    }
    if request.lat.is_some() || request.lon.is_some() {
        let (lat, lon) = position_pair(request.lat, request.lon)?;
        config.lat = lat;
        config.lon = lon;
    }

    if let Some(serial) = config.serial.as_deref()
        && serial_in_use(&state, serial, Some(&id)).await
    {
        return Err((
            StatusCode::CONFLICT,
            "serial/IMEI already registered".to_string(),
        ));
    }

    persist_upsert(&state, config.clone()).await?;
    let device = state.registry.register(config).await;
    Ok(Json(device))
}

async fn delete_device(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    if state.registry.device(&id).await.is_none() {
        return Err((StatusCode::NOT_FOUND, "device not found".to_string()));
    }

    persist_remove(&state, &id).await?;
    state.registry.remove(&id).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn trigger_panic(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Device>, StatusCode> {
    state
        .registry
        .set_panic(&id, true)
        .await
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn resolve_panic(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Device>, StatusCode> {
    state
        .registry
        .set_panic(&id, false)
        .await
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn persist_upsert(
    state: &AppState,
    config: DeviceConfig,
) -> Result<(), (StatusCode, String)> {
    let mut store = state.store.lock().await;
    store.upsert(config);
    store.save().await.map_err(|error| {
        warn!(%error, "failed to persist device store");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to persist device store".to_string(),
        )
    })
}

async fn persist_remove(state: &AppState, id: &str) -> Result<(), (StatusCode, String)> {
    let mut store = state.store.lock().await;
    store.remove(id);
    store.save().await.map_err(|error| {
        warn!(%error, "failed to persist device store");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to persist device store".to_string(),
        )
    })
}

fn clean_serial(serial: Option<String>) -> Option<String> {
    let serial = clean_optional(serial)?;
    normalize_imei(&serial).or(Some(serial))
}

async fn serial_in_use(state: &AppState, serial: &str, except_id: Option<&str>) -> bool {
    state.registry.snapshot().await.iter().any(|device| {
        Some(device.id.as_str()) != except_id
            && device
                .serial
                .as_deref()
                .is_some_and(|value| value.eq_ignore_ascii_case(serial))
    })
}

fn resolve_identity(
    protocol: Protocol,
    raw_id: Option<String>,
    raw_serial: Option<String>,
) -> Result<(String, Option<String>), String> {
    let id = raw_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let serial = clean_serial(raw_serial);

    match id {
        Some(id) => {
            if !valid_device_id(&id) {
                return Err(
                    "invalid device id: use letters, digits, ! - _ . : (max 64)".to_string()
                );
            }

            let serial = match serial {
                Some(serial) => Some(serial),
                None if protocol != Protocol::Meshtastic => normalize_imei(&id),
                None => None,
            };
            Ok((id, serial))
        }
        None => {
            if protocol == Protocol::Meshtastic {
                return Err(
                    "Device ID (node ID Meshtastic) wajib diisi, contoh !a1b2c3d4".to_string(),
                );
            }

            let Some(serial) = serial else {
                return Err(
                    "Device ID wajib diisi, atau isi Serial/IMEI 15-16 digit untuk memakai IMEI sebagai ID"
                        .to_string(),
                );
            };
            let Some(imei) = normalize_imei(&serial) else {
                return Err("Serial/IMEI harus 15-16 digit bila Device ID dikosongkan".to_string());
            };

            Ok((imei.clone(), Some(imei)))
        }
    }
}

fn clean_optional(value: Option<String>) -> Option<String> {
    value
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
}

fn clean_tags(tags: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut cleaned = Vec::new();
    for tag in tags {
        let tag = tag.trim().to_string();
        if !tag.is_empty() && tag.len() <= 32 && seen.insert(tag.clone()) {
            cleaned.push(tag);
        }
    }
    cleaned
}

fn position_pair(
    lat: Option<f64>,
    lon: Option<f64>,
) -> Result<(Option<f64>, Option<f64>), (StatusCode, String)> {
    match (lat, lon) {
        (Some(lat), Some(lon)) if valid_position(lat, lon) => Ok((Some(lat), Some(lon))),
        (None, None) => Ok((None, None)),
        _ => Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            "lat and lon must both be provided and valid".to_string(),
        )),
    }
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl axum::response::IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state.registry))
}

async fn handle_socket(mut socket: WebSocket, registry: Arc<Registry>) {
    let snapshot = ServerMessage::Snapshot {
        devices: registry.snapshot().await,
    };
    if send_message(&mut socket, &snapshot).await.is_err() {
        return;
    }

    let mut receiver = registry.subscribe();
    loop {
        tokio::select! {
            message = receiver.recv() => match message {
                Ok(message) => {
                    if send_message(&mut socket, &message).await.is_err() {
                        break;
                    }
                }
                Err(RecvError::Lagged(skipped)) => {
                    warn!(skipped, "websocket client lagged, sending fresh snapshot");
                    let snapshot = ServerMessage::Snapshot {
                        devices: registry.snapshot().await,
                    };
                    if send_message(&mut socket, &snapshot).await.is_err() {
                        break;
                    }
                }
                Err(RecvError::Closed) => break,
            },
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                _ => {}
            },
        }
    }
}

async fn send_message(socket: &mut WebSocket, message: &ServerMessage) -> Result<(), axum::Error> {
    let text = serde_json::to_string(message).map_err(axum::Error::new)?;
    socket.send(Message::Text(text.into())).await
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_flag(key: &str, default: bool) -> bool {
    env::var(key)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "on"))
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_imei_as_id_for_non_meshtastic() {
        let (id, serial) = resolve_identity(
            Protocol::Teltonika,
            None,
            Some("35 123456 789012 3".to_string()),
        )
        .unwrap();

        assert_eq!(id, "351234567890123");
        assert_eq!(serial.as_deref(), Some("351234567890123"));
    }

    #[test]
    fn accepts_sixteen_digit_imei() {
        let (id, serial) =
            resolve_identity(Protocol::Http, None, Some("3512345678901234".to_string())).unwrap();

        assert_eq!(id, "3512345678901234");
        assert_eq!(serial.as_deref(), Some("3512345678901234"));
    }

    #[test]
    fn requires_device_id_for_meshtastic() {
        let error = resolve_identity(Protocol::Meshtastic, None, Some("352222333444555".into()))
            .unwrap_err();
        assert!(error.contains("node ID Meshtastic"));
    }

    #[test]
    fn requires_valid_imei_when_id_is_empty() {
        assert!(resolve_identity(Protocol::Teltonika, None, None).is_err());
        assert!(resolve_identity(Protocol::Teltonika, None, Some("SN-1".into())).is_err());
    }

    #[test]
    fn keeps_explicit_id_and_serial_for_non_meshtastic() {
        let (id, serial) = resolve_identity(
            Protocol::Http,
            Some("truck-01".into()),
            Some("SN-123".into()),
        )
        .unwrap();

        assert_eq!(id, "truck-01");
        assert_eq!(serial.as_deref(), Some("SN-123"));
    }

    #[test]
    fn mirrors_imei_id_into_serial() {
        let (id, serial) =
            resolve_identity(Protocol::Teltonika, Some("351234567890123".into()), None).unwrap();

        assert_eq!(id, "351234567890123");
        assert_eq!(serial.as_deref(), Some("351234567890123"));
    }

    #[test]
    fn rejects_invalid_device_id() {
        assert!(resolve_identity(Protocol::Http, Some("bad id".into()), None).is_err());
    }
}

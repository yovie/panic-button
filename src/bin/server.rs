use axum::{
    Json, Router,
    extract::{
        Path, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    routing::{get, post},
};
use panic_button::{Device, IngestEvent, Registry, ServerMessage, mqtt, valid_position};
use serde_json::{Value, json};
use std::{env, net::SocketAddr, sync::Arc};
use tokio::sync::broadcast::error::RecvError;
use tower_http::{cors::CorsLayer, services::ServeDir, trace::TraceLayer};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

type AppState = Arc<Registry>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let registry = Registry::shared();

    if env_flag("MQTT_ENABLED", false) {
        let config = mqtt::MqttConfig {
            url: env_or("MQTT_URL", "mqtt://localhost:1883"),
            topic: env_or("MQTT_TOPIC", "msh/+/2/json/+/#"),
            panic_keywords: env_or("PANIC_KEYWORD", "PANIC,SOS")
                .split(',')
                .map(str::trim)
                .filter(|keyword| !keyword.is_empty())
                .map(str::to_string)
                .collect(),
            client_id: format!("panic-button-{}", std::process::id()),
        };
        let mqtt_registry = registry.clone();
        tokio::spawn(async move {
            if let Err(error) = mqtt::run(mqtt_registry, config).await {
                warn!(%error, "MQTT subscriber stopped");
            }
        });
    }

    let static_dir = env_or("STATIC_DIR", "static");

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/ingest", post(ingest))
        .route("/api/devices", get(list_devices))
        .route("/api/devices/{id}/panic", post(trigger_panic))
        .route("/api/devices/{id}/resolve", post(resolve_panic))
        .route("/ws", get(ws_handler))
        .fallback_service(ServeDir::new(&static_dir).append_index_html_on_directories(true))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(registry);

    let port: u16 = env_or("PORT", "8080").parse()?;
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "panic-button server listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(registry): State<AppState>) -> Json<Value> {
    Json(json!({
        "status": "ok",
        "devices": registry.snapshot().await.len(),
    }))
}

async fn ingest(
    State(registry): State<AppState>,
    Json(event): Json<IngestEvent>,
) -> Result<Json<Device>, (StatusCode, String)> {
    if event.device_id.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "device_id is required".to_string()));
    }
    if !valid_position(event.lat, event.lon) {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid coordinates".to_string(),
        ));
    }

    Ok(Json(registry.apply(event).await))
}

async fn list_devices(State(registry): State<AppState>) -> Json<Vec<Device>> {
    Json(registry.snapshot().await)
}

async fn trigger_panic(
    State(registry): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Device>, StatusCode> {
    registry
        .set_panic(&id, true)
        .await
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn resolve_panic(
    State(registry): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Device>, StatusCode> {
    registry
        .set_panic(&id, false)
        .await
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(registry): State<AppState>,
) -> impl axum::response::IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, registry))
}

async fn handle_socket(mut socket: WebSocket, registry: AppState) {
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

fn env_flag(key: &str, default: bool) -> bool {
    env::var(key)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "on"))
        .unwrap_or(default)
}

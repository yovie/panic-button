use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};

pub mod adapters;
pub mod mqtt;
pub mod store;

pub const EVENT_CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceType {
    #[default]
    PanicButton,
    Tracker,
    Gateway,
    Sensor,
    Other,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    #[default]
    Meshtastic,
    Http,
    Simulator,
    Teltonika,
    OwnTracks,
    OsmAnd,
    Traccar,
    Lorawan,
    GenericMqtt,
    GenericHttp,
    Other,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    #[default]
    Heartbeat,
    Panic,
    PanicClear,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IngestEvent {
    pub device_id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub serial: Option<String>,
    pub lat: f64,
    pub lon: f64,
    #[serde(default)]
    pub speed_kmh: Option<f64>,
    #[serde(default)]
    pub heading: Option<f64>,
    #[serde(default)]
    pub battery: Option<f64>,
    #[serde(default)]
    pub device_type: Option<DeviceType>,
    #[serde(default)]
    pub protocol: Option<Protocol>,
    #[serde(default)]
    pub event: EventKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeviceConfig {
    pub id: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub device_type: DeviceType,
    #[serde(default)]
    pub protocol: Protocol,
    #[serde(default)]
    pub serial: Option<String>,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub notes: Option<String>,
    #[serde(default)]
    pub lat: Option<f64>,
    #[serde(default)]
    pub lon: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Normal,
    Panic,
}

#[derive(Debug, Clone, Serialize)]
pub struct Device {
    pub id: String,
    pub name: String,
    pub label: Option<String>,
    pub device_type: DeviceType,
    pub protocol: Protocol,
    pub serial: Option<String>,
    pub owner: Option<String>,
    pub tags: Vec<String>,
    pub notes: Option<String>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub speed_kmh: Option<f64>,
    pub heading: Option<f64>,
    pub battery: Option<f64>,
    pub last_seen: Option<DateTime<Utc>>,
    pub status: Status,
    pub panic_since: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    Snapshot { devices: Vec<Device> },
    Update { device: Device },
    Panic { device: Device },
    PanicCleared { device: Device },
    Removed { id: String },
}

#[derive(Debug, Default)]
struct Meta {
    names: HashMap<String, String>,
    pending_panics: HashSet<String>,
}

pub struct Registry {
    devices: RwLock<HashMap<String, Device>>,
    meta: RwLock<Meta>,
    tx: broadcast::Sender<ServerMessage>,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    pub fn new() -> Self {
        Self::with_capacity(EVENT_CHANNEL_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self {
            devices: RwLock::new(HashMap::new()),
            meta: RwLock::new(Meta::default()),
            tx,
        }
    }

    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ServerMessage> {
        self.tx.subscribe()
    }

    pub async fn snapshot(&self) -> Vec<Device> {
        let mut devices: Vec<Device> = self.devices.read().await.values().cloned().collect();
        devices.sort_by(|a, b| a.id.cmp(&b.id));
        devices
    }

    pub async fn device(&self, id: &str) -> Option<Device> {
        self.devices.read().await.get(id).cloned()
    }

    pub async fn apply(&self, event: IngestEvent) -> Device {
        let now = Utc::now();
        let (default_name, pending_panic) = {
            let meta = self.meta.read().await;
            (
                meta.names.get(&event.device_id).cloned(),
                meta.pending_panics.contains(&event.device_id),
            )
        };

        let mut devices = self.devices.write().await;
        let device = devices
            .entry(event.device_id.clone())
            .or_insert_with(|| Device {
                id: event.device_id.clone(),
                name: event
                    .name
                    .clone()
                    .or(default_name)
                    .unwrap_or_else(|| event.device_id.clone()),
                label: None,
                device_type: event.device_type.unwrap_or_default(),
                protocol: event.protocol.unwrap_or_default(),
                serial: event.serial.clone(),
                owner: None,
                tags: Vec::new(),
                notes: None,
                lat: Some(event.lat),
                lon: Some(event.lon),
                speed_kmh: event.speed_kmh,
                heading: event.heading,
                battery: event.battery,
                last_seen: Some(now),
                status: Status::Normal,
                panic_since: None,
            });

        if let Some(name) = event.name.clone() {
            device.name = name;
        }
        if device.serial.is_none() {
            device.serial = event.serial.clone();
        }
        device.lat = Some(event.lat);
        device.lon = Some(event.lon);
        device.speed_kmh = event.speed_kmh;
        device.heading = event.heading;
        if event.battery.is_some() {
            device.battery = event.battery;
        }
        device.last_seen = Some(now);

        let mut broadcasts = match event.event {
            EventKind::Panic if device.status != Status::Panic => {
                device.status = Status::Panic;
                device.panic_since = Some(now);
                vec![ServerMessage::Panic {
                    device: device.clone(),
                }]
            }
            EventKind::PanicClear if device.status == Status::Panic => {
                device.status = Status::Normal;
                device.panic_since = None;
                vec![ServerMessage::PanicCleared {
                    device: device.clone(),
                }]
            }
            _ => vec![ServerMessage::Update {
                device: device.clone(),
            }],
        };

        if pending_panic && device.status == Status::Normal {
            device.status = Status::Panic;
            device.panic_since = Some(now);
            broadcasts.push(ServerMessage::Panic {
                device: device.clone(),
            });
            self.meta
                .write()
                .await
                .pending_panics
                .remove(&event.device_id);
        }

        let result = device.clone();
        drop(devices);

        for message in broadcasts {
            let _ = self.tx.send(message);
        }
        result
    }

    pub async fn register(&self, config: DeviceConfig) -> Device {
        let mut devices = self.devices.write().await;
        let device = devices.entry(config.id.clone()).or_insert_with(|| Device {
            id: config.id.clone(),
            name: config.id.clone(),
            label: config.label.clone(),
            device_type: config.device_type,
            protocol: config.protocol,
            serial: config.serial.clone(),
            owner: config.owner.clone(),
            tags: config.tags.clone(),
            notes: config.notes.clone(),
            lat: config.lat,
            lon: config.lon,
            speed_kmh: None,
            heading: None,
            battery: None,
            last_seen: None,
            status: Status::Normal,
            panic_since: None,
        });

        device.label = config.label;
        device.device_type = config.device_type;
        device.protocol = config.protocol;
        device.serial = config.serial;
        device.owner = config.owner;
        device.tags = config.tags;
        device.notes = config.notes;
        if let (Some(lat), Some(lon)) = (config.lat, config.lon) {
            device.lat = Some(lat);
            device.lon = Some(lon);
        }

        let result = device.clone();
        drop(devices);
        let _ = self.tx.send(ServerMessage::Update {
            device: result.clone(),
        });
        result
    }

    pub async fn remove(&self, id: &str) -> bool {
        let removed = self.devices.write().await.remove(id);
        let mut meta = self.meta.write().await;
        meta.names.remove(id);
        meta.pending_panics.remove(id);
        drop(meta);

        if removed.is_some() {
            let _ = self.tx.send(ServerMessage::Removed { id: id.to_string() });
        }
        removed.is_some()
    }

    pub async fn set_name(&self, id: &str, name: &str) {
        self.meta
            .write()
            .await
            .names
            .insert(id.to_string(), name.to_string());

        let mut devices = self.devices.write().await;
        if let Some(device) = devices.get_mut(id) {
            device.name = name.to_string();
            let message = ServerMessage::Update {
                device: device.clone(),
            };
            drop(devices);
            let _ = self.tx.send(message);
        }
    }

    pub async fn set_battery(&self, id: &str, battery: f64) {
        let mut devices = self.devices.write().await;
        if let Some(device) = devices.get_mut(id) {
            device.battery = Some(battery);
            device.last_seen = Some(Utc::now());
            let message = ServerMessage::Update {
                device: device.clone(),
            };
            drop(devices);
            let _ = self.tx.send(message);
        }
    }

    pub async fn set_panic(&self, id: &str, panic: bool) -> Option<Device> {
        let now = Utc::now();
        let mut devices = self.devices.write().await;

        let Some(device) = devices.get_mut(id) else {
            let mut meta = self.meta.write().await;
            if panic {
                meta.pending_panics.insert(id.to_string());
            } else {
                meta.pending_panics.remove(id);
            }
            return None;
        };

        let message = if panic {
            if device.status == Status::Panic {
                ServerMessage::Update {
                    device: device.clone(),
                }
            } else {
                device.status = Status::Panic;
                device.panic_since = Some(now);
                ServerMessage::Panic {
                    device: device.clone(),
                }
            }
        } else if device.status == Status::Panic {
            device.status = Status::Normal;
            device.panic_since = None;
            ServerMessage::PanicCleared {
                device: device.clone(),
            }
        } else {
            ServerMessage::Update {
                device: device.clone(),
            }
        };

        let result = device.clone();
        drop(devices);
        let _ = self.tx.send(message);
        Some(result)
    }
}

pub fn valid_position(lat: f64, lon: f64) -> bool {
    lat.is_finite()
        && lon.is_finite()
        && (-90.0..=90.0).contains(&lat)
        && (-180.0..=180.0).contains(&lon)
        && !(lat.abs() < f64::EPSILON && lon.abs() < f64::EPSILON)
}

pub fn valid_device_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "!-_.:".contains(character))
}

pub fn normalize_imei(value: &str) -> Option<String> {
    let compact: String = value
        .chars()
        .filter(|character| !character.is_whitespace() && *character != '-')
        .collect();

    if matches!(compact.len(), 15 | 16)
        && compact.chars().all(|character| character.is_ascii_digit())
    {
        Some(compact)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(id: &str, lat: f64, lon: f64, kind: EventKind) -> IngestEvent {
        IngestEvent {
            device_id: id.to_string(),
            name: None,
            serial: None,
            lat,
            lon,
            speed_kmh: Some(10.0),
            heading: Some(90.0),
            battery: Some(80.0),
            device_type: None,
            protocol: None,
            event: kind,
        }
    }

    fn config(id: &str) -> DeviceConfig {
        DeviceConfig {
            id: id.to_string(),
            label: Some("Pos Pantau".to_string()),
            device_type: DeviceType::Tracker,
            protocol: Protocol::Http,
            serial: Some("SN-123".to_string()),
            owner: Some("Satgas".to_string()),
            tags: vec!["regu-1".to_string()],
            notes: Some("unit cadangan".to_string()),
            lat: None,
            lon: None,
        }
    }

    #[tokio::test]
    async fn heartbeat_creates_and_moves_device() {
        let registry = Registry::new();
        registry
            .apply(event("dev-1", -6.2, 106.8, EventKind::Heartbeat))
            .await;
        registry
            .apply(event("dev-1", -6.21, 106.81, EventKind::Heartbeat))
            .await;

        let snapshot = registry.snapshot().await;
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].id, "dev-1");
        assert!((snapshot[0].lat.unwrap() - -6.21).abs() < 1e-9);
        assert_eq!(snapshot[0].status, Status::Normal);
    }

    #[tokio::test]
    async fn ingest_sets_type_and_protocol_on_creation() {
        let registry = Registry::new();
        let mut event = event("dev-1", -6.2, 106.8, EventKind::Heartbeat);
        event.device_type = Some(DeviceType::Sensor);
        event.protocol = Some(Protocol::Simulator);

        let device = registry.apply(event).await;
        assert_eq!(device.device_type, DeviceType::Sensor);
        assert_eq!(device.protocol, Protocol::Simulator);
    }

    #[tokio::test]
    async fn panic_transitions_once_and_clears() {
        let registry = Registry::new();
        let mut rx = registry.subscribe();

        registry
            .apply(event("dev-1", -6.2, 106.8, EventKind::Heartbeat))
            .await;
        let first = registry
            .apply(event("dev-1", -6.2, 106.8, EventKind::Panic))
            .await;
        assert_eq!(first.status, Status::Panic);
        assert!(first.panic_since.is_some());

        let second = registry
            .apply(event("dev-1", -6.2, 106.8, EventKind::Panic))
            .await;
        assert_eq!(second.status, Status::Panic);

        let cleared = registry
            .apply(event("dev-1", -6.2, 106.8, EventKind::PanicClear))
            .await;
        assert_eq!(cleared.status, Status::Normal);
        assert!(cleared.panic_since.is_none());

        let mut panics = 0;
        while let Ok(message) = rx.try_recv() {
            if matches!(message, ServerMessage::Panic { .. }) {
                panics += 1;
            }
        }
        assert_eq!(panics, 1, "panic must only be broadcast on transition");
    }

    #[tokio::test]
    async fn set_panic_unknown_device_is_pending_until_position() {
        let registry = Registry::new();
        assert!(registry.set_panic("!deadbeef", true).await.is_none());

        let device = registry
            .apply(event("!deadbeef", -6.2, 106.8, EventKind::Heartbeat))
            .await;
        assert_eq!(device.status, Status::Panic);
    }

    #[tokio::test]
    async fn name_is_kept_for_late_positions() {
        let registry = Registry::new();
        registry.set_name("!deadbeef", "Tim Alpha").await;
        let device = registry
            .apply(event("!deadbeef", -6.2, 106.8, EventKind::Heartbeat))
            .await;
        assert_eq!(device.name, "Tim Alpha");
    }

    #[tokio::test]
    async fn register_device_without_position() {
        let registry = Registry::new();
        let mut rx = registry.subscribe();

        let device = registry.register(config("device-01")).await;
        assert_eq!(device.label.as_deref(), Some("Pos Pantau"));
        assert_eq!(device.device_type, DeviceType::Tracker);
        assert!(device.lat.is_none());
        assert!(device.last_seen.is_none());

        assert!(matches!(
            rx.try_recv().unwrap(),
            ServerMessage::Update { .. }
        ));
    }

    #[tokio::test]
    async fn register_updates_metadata_but_keeps_live_state() {
        let registry = Registry::new();
        registry
            .apply(event("device-01", -6.2, 106.8, EventKind::Heartbeat))
            .await;

        let mut updated = config("device-01");
        updated.label = Some("Pos Utama".to_string());
        updated.device_type = DeviceType::PanicButton;
        let device = registry.register(updated).await;

        assert_eq!(device.label.as_deref(), Some("Pos Utama"));
        assert_eq!(device.device_type, DeviceType::PanicButton);
        assert_eq!(device.lat, Some(-6.2));
        assert!(device.last_seen.is_some());
    }

    #[tokio::test]
    async fn label_is_separate_from_reported_name() {
        let registry = Registry::new();
        registry.register(config("device-01")).await;
        registry.set_name("device-01", "Node Reported").await;

        let device = registry.device("device-01").await.unwrap();
        assert_eq!(device.name, "Node Reported");
        assert_eq!(device.label.as_deref(), Some("Pos Pantau"));
    }

    #[tokio::test]
    async fn remove_device_broadcasts_event() {
        let registry = Registry::new();
        registry.register(config("device-01")).await;
        let mut rx = registry.subscribe();

        assert!(registry.remove("device-01").await);
        assert!(registry.snapshot().await.is_empty());
        assert!(matches!(
            rx.try_recv().unwrap(),
            ServerMessage::Removed { .. }
        ));
        assert!(!registry.remove("device-01").await);
    }

    #[test]
    fn rejects_invalid_coordinates() {
        assert!(valid_position(-6.2, 106.8));
        assert!(!valid_position(0.0, 0.0));
        assert!(!valid_position(91.0, 106.8));
        assert!(!valid_position(f64::NAN, 106.8));
    }

    #[test]
    fn normalizes_imei_values() {
        assert_eq!(
            normalize_imei("35 123456 789012 3"),
            Some("351234567890123".to_string())
        );
        assert_eq!(
            normalize_imei("35-123456-789012-3"),
            Some("351234567890123".to_string())
        );
        assert_eq!(
            normalize_imei("3512345678901234"),
            Some("3512345678901234".to_string())
        );
        assert_eq!(normalize_imei("35123456789012"), None);
        assert_eq!(normalize_imei("!a1b2c3d4"), None);
    }

    #[tokio::test]
    async fn ingest_fills_serial_for_new_device() {
        let registry = Registry::new();
        let mut event = event("imei-01", -6.2, 106.8, EventKind::Heartbeat);
        event.serial = Some("351234567890123".to_string());

        let device = registry.apply(event).await;
        assert_eq!(device.serial.as_deref(), Some("351234567890123"));
    }

    #[test]
    fn validates_device_ids() {
        assert!(valid_device_id("!a1b2c3d4"));
        assert!(valid_device_id("truck-01"));
        assert!(valid_device_id("gateway.jakarta:1"));
        assert!(!valid_device_id(""));
        assert!(!valid_device_id("ada spasi"));
        assert!(!valid_device_id("id/with/slash"));
    }
}

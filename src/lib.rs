use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};

pub mod mqtt;

pub const EVENT_CHANNEL_CAPACITY: usize = 1024;

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
    pub lat: f64,
    pub lon: f64,
    #[serde(default)]
    pub speed_kmh: Option<f64>,
    #[serde(default)]
    pub heading: Option<f64>,
    #[serde(default)]
    pub battery: Option<f64>,
    #[serde(default)]
    pub event: EventKind,
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
    pub lat: f64,
    pub lon: f64,
    pub speed_kmh: Option<f64>,
    pub heading: Option<f64>,
    pub battery: Option<f64>,
    pub last_seen: DateTime<Utc>,
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
                lat: event.lat,
                lon: event.lon,
                speed_kmh: event.speed_kmh,
                heading: event.heading,
                battery: event.battery,
                last_seen: now,
                status: Status::Normal,
                panic_since: None,
            });

        if let Some(name) = event.name.clone() {
            device.name = name;
        }
        device.lat = event.lat;
        device.lon = event.lon;
        device.speed_kmh = event.speed_kmh;
        device.heading = event.heading;
        if event.battery.is_some() {
            device.battery = event.battery;
        }
        device.last_seen = now;

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
            device.last_seen = Utc::now();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn event(id: &str, lat: f64, lon: f64, kind: EventKind) -> IngestEvent {
        IngestEvent {
            device_id: id.to_string(),
            name: None,
            lat,
            lon,
            speed_kmh: Some(10.0),
            heading: Some(90.0),
            battery: Some(80.0),
            event: kind,
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
        assert!((snapshot[0].lat - -6.21).abs() < 1e-9);
        assert_eq!(snapshot[0].status, Status::Normal);
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

    #[test]
    fn rejects_invalid_coordinates() {
        assert!(valid_position(-6.2, 106.8));
        assert!(!valid_position(0.0, 0.0));
        assert!(!valid_position(91.0, 106.8));
        assert!(!valid_position(f64::NAN, 106.8));
    }
}

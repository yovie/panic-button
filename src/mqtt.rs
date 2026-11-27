use crate::{EventKind, IngestEvent, Protocol, Registry, valid_position};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub struct MqttConfig {
    pub url: String,
    pub topic: String,
    pub tracking_topic: String,
    pub panic_keywords: Vec<String>,
    pub client_id: String,
}

impl MqttConfig {
    pub fn subscription_topics(&self) -> Vec<&str> {
        let mut topics = Vec::new();
        let topic = self.topic.trim();
        if !topic.is_empty() {
            topics.push(topic);
        }

        let tracking = self.tracking_topic.trim();
        if !tracking.is_empty()
            && !topics
                .iter()
                .any(|existing| *existing == "#" || *existing == tracking)
        {
            topics.push(tracking);
        }

        topics
    }
}

pub struct BrokerAddress {
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
}

pub fn parse_broker_url(raw: &str) -> anyhow::Result<BrokerAddress> {
    let url = url::Url::parse(raw)?;
    let host = url.host_str().unwrap_or("localhost").to_string();
    let port = url.port().unwrap_or(1883);
    let username = if url.username().is_empty() {
        None
    } else {
        Some(url.username().to_string())
    };
    let password = url.password().map(str::to_string);
    Ok(BrokerAddress {
        host,
        port,
        username,
        password,
    })
}

pub async fn run(registry: Arc<Registry>, config: MqttConfig) -> anyhow::Result<()> {
    let broker = parse_broker_url(&config.url)?;

    loop {
        if let Err(error) = connect_and_poll(registry.clone(), &config, &broker).await {
            warn!(%error, "MQTT connection lost, reconnecting in 5s");
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn connect_and_poll(
    registry: Arc<Registry>,
    config: &MqttConfig,
    broker: &BrokerAddress,
) -> anyhow::Result<()> {
    let mut options = MqttOptions::new(&config.client_id, &broker.host, broker.port);
    options.set_keep_alive(Duration::from_secs(30));
    options.set_clean_session(true);

    if let Some(username) = &broker.username {
        options.set_credentials(username, broker.password.as_deref().unwrap_or(""));
    }

    let (client, mut eventloop) = AsyncClient::new(options, 64);
    info!(
        host = %broker.host,
        port = broker.port,
        topic = %config.topic,
        "connecting to MQTT broker"
    );

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                client
                    .subscribe(config.topic.clone(), QoS::AtLeastOnce)
                    .await?;
                info!(topic = %config.topic, "subscribed to Meshtastic JSON topic");
            }
            Ok(Event::Incoming(Packet::Publish(publish))) => {
                if let Err(error) =
                    handle_message(&registry, &config.panic_keywords, &publish.payload).await
                {
                    debug!(%error, "ignored MQTT message that could not be parsed");
                }
            }
            Ok(_) => {}
            Err(error) => return Err(error.into()),
        }
    }
}

pub async fn handle_message(
    registry: &Registry,
    panic_keywords: &[String],
    payload: &[u8],
) -> anyhow::Result<()> {
    let message: Value = serde_json::from_slice(payload)?;

    let Some(kind) = message.get("type").and_then(Value::as_str) else {
        return Ok(());
    };
    let Some(from) = message.get("from").and_then(json_node_number) else {
        return Ok(());
    };
    let device_id = format!("!{from:08x}");
    let payload = &message["payload"];

    match kind {
        "nodeinfo" => {
            if let Some(name) = payload
                .get("longname")
                .and_then(Value::as_str)
                .filter(|name| !name.trim().is_empty())
                .or_else(|| payload.get("shortname").and_then(Value::as_str))
            {
                registry.set_name(&device_id, name.trim()).await;
            }
        }
        "position" => {
            let Some(lat_i) = payload.get("latitude_i").and_then(Value::as_i64) else {
                return Ok(());
            };
            let Some(lon_i) = payload.get("longitude_i").and_then(Value::as_i64) else {
                return Ok(());
            };

            let lat = lat_i as f64 * 1e-7;
            let lon = lon_i as f64 * 1e-7;
            if !valid_position(lat, lon) {
                return Ok(());
            }

            let speed_kmh = payload
                .get("ground_speed")
                .and_then(Value::as_f64)
                .map(|meters_per_second| meters_per_second * 3.6)
                .or_else(|| payload.get("speed").and_then(Value::as_f64));
            let heading = payload.get("ground_track").and_then(Value::as_f64);
            let battery = payload.get("battery_level").and_then(Value::as_f64);

            registry
                .apply(IngestEvent {
                    device_id,
                    name: None,
                    lat,
                    lon,
                    speed_kmh,
                    heading,
                    battery,
                    device_type: None,
                    protocol: Some(Protocol::Meshtastic),
                    serial: None,
                    event: EventKind::Heartbeat,
                })
                .await;
        }
        "text" => {
            let text = payload
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let triggered = panic_keywords.iter().any(|keyword| {
                !keyword.is_empty() && text.to_lowercase().contains(&keyword.to_lowercase())
            });
            if triggered {
                registry.set_panic(&device_id, true).await;
            }
        }
        "detection_sensor" => {
            let state = payload
                .get("state")
                .and_then(Value::as_bool)
                .or_else(|| payload.get("detected").and_then(Value::as_bool))
                .unwrap_or(true);
            registry.set_panic(&device_id, state).await;
        }
        "telemetry" => {
            if let Some(battery) = payload.get("battery_level").and_then(Value::as_f64) {
                registry.set_battery(&device_id, battery).await;
            }
        }
        _ => {}
    }

    Ok(())
}

fn json_node_number(value: &Value) -> Option<u32> {
    if let Some(number) = value.as_u64() {
        Some(number as u32)
    } else {
        value.as_i64().map(|number| number as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Status;
    use serde_json::json;

    #[tokio::test]
    async fn position_message_creates_device() {
        let registry = Registry::new();
        let payload = json!({
            "from": 0x0000c001u32,
            "type": "position",
            "payload": {
                "latitude_i": -62000000,
                "longitude_i": 1068000000,
                "ground_speed": 10.0,
                "ground_track": 45.0
            }
        });

        handle_message(&registry, &[], payload.to_string().as_bytes())
            .await
            .unwrap();

        let device = registry.device("!0000c001").await.unwrap();
        assert!((device.lat.unwrap() - -6.2).abs() < 1e-6);
        assert!((device.lon.unwrap() - 106.8).abs() < 1e-6);
        assert!((device.speed_kmh.unwrap() - 36.0).abs() < 1e-6);
        assert_eq!(device.status, Status::Normal);
    }

    #[tokio::test]
    async fn nodeinfo_renames_device() {
        let registry = Registry::new();
        registry
            .apply(IngestEvent {
                device_id: "!0000c001".to_string(),
                name: None,
                lat: -6.2,
                lon: 106.8,
                speed_kmh: None,
                heading: None,
                battery: None,
                device_type: None,
                protocol: None,
                serial: None,
                event: EventKind::Heartbeat,
            })
            .await;

        let payload = json!({
            "from": 0x0000c001u32,
            "type": "nodeinfo",
            "payload": { "id": "!0000c001", "longname": "Tim Alpha", "shortname": "TA" }
        });
        handle_message(&registry, &[], payload.to_string().as_bytes())
            .await
            .unwrap();

        assert_eq!(
            registry.device("!0000c001").await.unwrap().name,
            "Tim Alpha"
        );
    }

    #[tokio::test]
    async fn panic_keyword_text_triggers_panic() {
        let registry = Registry::new();
        let position = json!({
            "from": 0x0000c001u32,
            "type": "position",
            "payload": { "latitude_i": -62000000, "longitude_i": 1068000000 }
        });
        handle_message(&registry, &[], position.to_string().as_bytes())
            .await
            .unwrap();

        let text = json!({
            "from": 0x0000c001u32,
            "type": "text",
            "payload": { "text": "tolong, ini PANIC!" }
        });
        handle_message(
            &registry,
            &["PANIC".to_string()],
            text.to_string().as_bytes(),
        )
        .await
        .unwrap();

        assert_eq!(
            registry.device("!0000c001").await.unwrap().status,
            Status::Panic
        );
    }

    #[tokio::test]
    async fn detection_sensor_state_controls_panic() {
        let registry = Registry::new();
        let position = json!({
            "from": 0x0000c001u32,
            "type": "position",
            "payload": { "latitude_i": -62000000, "longitude_i": 1068000000 }
        });
        handle_message(&registry, &[], position.to_string().as_bytes())
            .await
            .unwrap();

        let pressed = json!({
            "from": 0x0000c001u32,
            "type": "detection_sensor",
            "payload": { "state": true, "name": "button" }
        });
        handle_message(&registry, &[], pressed.to_string().as_bytes())
            .await
            .unwrap();
        assert_eq!(
            registry.device("!0000c001").await.unwrap().status,
            Status::Panic
        );

        let released = json!({
            "from": 0x0000c001u32,
            "type": "detection_sensor",
            "payload": { "state": false }
        });
        handle_message(&registry, &[], released.to_string().as_bytes())
            .await
            .unwrap();
        assert_eq!(
            registry.device("!0000c001").await.unwrap().status,
            Status::Normal
        );
    }

    fn config(topic: &str, tracking_topic: &str) -> MqttConfig {
        MqttConfig {
            url: "mqtt://localhost:1883".to_string(),
            topic: topic.to_string(),
            tracking_topic: tracking_topic.to_string(),
            panic_keywords: Vec::new(),
            client_id: "test".to_string(),
        }
    }

    #[test]
    fn subscribes_to_meshtastic_and_tracking_topics() {
        let config = config("msh/#", "gps/#");
        let topics = config.subscription_topics();
        assert_eq!(topics, vec!["msh/#", "gps/#"]);
    }

    #[test]
    fn skips_tracking_topic_when_wildcard_covers_it() {
        let config = config("#", "gps/#");
        let topics = config.subscription_topics();
        assert_eq!(topics, vec!["#"]);
    }

    #[test]
    fn deduplicates_identical_topics() {
        let config = config("gps/#", "gps/#");
        let topics = config.subscription_topics();
        assert_eq!(topics, vec!["gps/#"]);
    }

    #[test]
    fn ignores_empty_topics() {
        let config = config(" ", "");
        let topics = config.subscription_topics();
        assert!(topics.is_empty());
    }
}

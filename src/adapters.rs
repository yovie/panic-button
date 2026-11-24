use crate::{
    DeviceType, EventKind, IngestEvent, Protocol, Registry, normalize_imei,
    raw_capture::{CaptureSource, RawCaptureStore},
    valid_device_id, valid_position,
};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub enum AdapterAction {
    Apply(IngestEvent),
    SetName { id: String, name: String },
    SetBattery { id: String, battery: f64 },
    SetPanic { id: String, panic: bool },
}

pub async fn apply_actions(registry: &Registry, actions: Vec<AdapterAction>) {
    for action in actions {
        match action {
            AdapterAction::Apply(event) => {
                registry.apply(event).await;
            }
            AdapterAction::SetName { id, name } => {
                registry.set_name(&id, &name).await;
            }
            AdapterAction::SetBattery { id, battery } => {
                registry.set_battery(&id, battery).await;
            }
            AdapterAction::SetPanic { id, panic } => {
                registry.set_panic(&id, panic).await;
            }
        }
    }
}

#[derive(Debug)]
pub enum DetectOutcome {
    NoMatch,
    Handled(Vec<AdapterAction>),
    Skipped { reason: &'static str },
}

pub trait MqttAdapter: Send + Sync {
    fn name(&self) -> &'static str;
    fn detect(&self, topic: &str, payload: &[u8]) -> Option<Vec<AdapterAction>>;

    fn detect_outcome(&self, topic: &str, payload: &[u8]) -> DetectOutcome {
        match self.detect(topic, payload) {
            Some(actions) => DetectOutcome::Handled(actions),
            None => DetectOutcome::NoMatch,
        }
    }
}

pub trait HttpAdapter: Send + Sync {
    fn name(&self) -> &'static str;
    fn detect(&self, request: &HttpRequest) -> Option<IngestEvent>;
}

#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub query: HashMap<String, String>,
    pub body: Option<Value>,
}

impl HttpRequest {
    pub fn get_value(&self, key: &str) -> Option<Value> {
        if let Some(body) = &self.body
            && let Some(value) = body.get(key)
        {
            return Some(value.clone());
        }
        self.query.get(key).cloned().map(Value::String)
    }

    pub fn get_str(&self, key: &str) -> Option<String> {
        self.get_value(key).as_ref().and_then(as_string)
    }

    pub fn get_f64(&self, key: &str) -> Option<f64> {
        self.get_value(key).as_ref().and_then(as_f64)
    }
}

fn as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn as_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse::<f64>().ok()))
        .or_else(|| value.as_i64().map(|number| number as f64))
        .or_else(|| value.as_u64().map(|number| number as f64))
}

fn clean_id(raw: Option<String>) -> Option<String> {
    let mut id = raw
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())?;
    if let Some(imei) = normalize_imei(&id) {
        id = imei;
    }
    if valid_device_id(&id) { Some(id) } else { None }
}

#[allow(clippy::too_many_arguments)]
fn make_event(
    device_id: String,
    lat: f64,
    lon: f64,
    speed_kmh: Option<f64>,
    heading: Option<f64>,
    battery: Option<f64>,
    protocol: Protocol,
    device_type: DeviceType,
) -> IngestEvent {
    IngestEvent {
        device_id,
        name: None,
        lat,
        lon,
        speed_kmh,
        heading,
        battery,
        device_type: Some(device_type),
        protocol: Some(protocol),
        serial: None,
        event: EventKind::Heartbeat,
    }
}

fn parse_speed_kmh(request: &HttpRequest, speed_keys: &[&str]) -> Option<f64> {
    for key in speed_keys {
        if let Some(value) = request.get_f64(key) {
            return Some(value);
        }
    }
    if let Some(value) = request.get_f64("speed_ms") {
        return Some(value * 3.6);
    }
    if let Some(value) = request.get_f64("speed") {
        return Some(value);
    }
    None
}

fn parse_heading(request: &HttpRequest) -> Option<f64> {
    request
        .get_f64("heading")
        .or_else(|| request.get_f64("course"))
        .or_else(|| request.get_f64("bearing"))
        .or_else(|| request.get_f64("cog"))
}

fn parse_battery(request: &HttpRequest) -> Option<f64> {
    request
        .get_f64("battery")
        .or_else(|| request.get_f64("batt"))
        .or_else(|| request.get_f64("battery_level"))
}

// ---------- Meshtastic ----------

pub struct MeshtasticAdapter {
    pub panic_keywords: Vec<String>,
}

impl MqttAdapter for MeshtasticAdapter {
    fn name(&self) -> &'static str {
        "meshtastic"
    }

    fn detect(&self, _topic: &str, payload: &[u8]) -> Option<Vec<AdapterAction>> {
        let message: Value = serde_json::from_slice(payload).ok()?;
        let kind = message.get("type").and_then(Value::as_str)?;
        let from = message.get("from").and_then(as_u32)?;
        let device_id = format!("!{from:08x}");
        let payload = &message["payload"];

        match kind {
            "nodeinfo" => {
                let name = payload
                    .get("longname")
                    .and_then(Value::as_str)
                    .filter(|name| !name.trim().is_empty())
                    .or_else(|| payload.get("shortname").and_then(Value::as_str))
                    .map(str::trim)
                    .filter(|name| !name.is_empty());
                if let Some(name) = name {
                    Some(vec![AdapterAction::SetName {
                        id: device_id,
                        name: name.to_string(),
                    }])
                } else {
                    Some(Vec::new())
                }
            }
            "position" => {
                let lat_i = payload.get("latitude_i").and_then(Value::as_i64)?;
                let lon_i = payload.get("longitude_i").and_then(Value::as_i64)?;
                let lat = lat_i as f64 * 1e-7;
                let lon = lon_i as f64 * 1e-7;
                if !valid_position(lat, lon) {
                    return Some(Vec::new());
                }
                let speed_kmh = payload
                    .get("ground_speed")
                    .and_then(as_f64)
                    .map(|meters_per_second| meters_per_second * 3.6)
                    .or_else(|| payload.get("speed").and_then(as_f64));
                let heading = payload.get("ground_track").and_then(as_f64);
                let battery = payload.get("battery_level").and_then(as_f64);
                Some(vec![AdapterAction::Apply(make_event(
                    device_id,
                    lat,
                    lon,
                    speed_kmh,
                    heading,
                    battery,
                    Protocol::Meshtastic,
                    DeviceType::PanicButton,
                ))])
            }
            "text" => {
                let text = payload
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let triggered = self.panic_keywords.iter().any(|keyword| {
                    !keyword.is_empty() && text.to_lowercase().contains(&keyword.to_lowercase())
                });
                if triggered {
                    Some(vec![AdapterAction::SetPanic {
                        id: device_id,
                        panic: true,
                    }])
                } else {
                    Some(Vec::new())
                }
            }
            "detection_sensor" => {
                let state = payload
                    .get("state")
                    .and_then(Value::as_bool)
                    .or_else(|| payload.get("detected").and_then(Value::as_bool))
                    .unwrap_or(true);
                Some(vec![AdapterAction::SetPanic {
                    id: device_id,
                    panic: state,
                }])
            }
            "telemetry" => {
                if let Some(battery) = payload.get("battery_level").and_then(as_f64) {
                    Some(vec![AdapterAction::SetBattery {
                        id: device_id,
                        battery,
                    }])
                } else {
                    Some(Vec::new())
                }
            }
            _ => None,
        }
    }
}

fn as_u32(value: &Value) -> Option<u32> {
    value
        .as_u64()
        .map(|number| number as u32)
        .or_else(|| value.as_i64().map(|number| number as u32))
}

// ---------- OwnTracks ----------

pub struct OwnTracksAdapter;

impl MqttAdapter for OwnTracksAdapter {
    fn name(&self) -> &'static str {
        "owntracks"
    }

    fn detect(&self, topic: &str, payload: &[u8]) -> Option<Vec<AdapterAction>> {
        if std::str::from_utf8(payload).is_ok_and(|text| text.trim().eq_ignore_ascii_case("lwt")) {
            return Some(Vec::new());
        }
        let value: Value = serde_json::from_slice(payload).ok()?;
        let kind = value.get("_type").and_then(Value::as_str);
        if kind == Some("lwt") {
            return Some(Vec::new());
        }
        if kind != Some("location") {
            return None;
        }
        owntracks_event(topic, &value).map(|event| vec![AdapterAction::Apply(event)])
    }
}

impl HttpAdapter for OwnTracksAdapter {
    fn name(&self) -> &'static str {
        "owntracks"
    }

    fn detect(&self, request: &HttpRequest) -> Option<IngestEvent> {
        if let Some(body) = &request.body
            && body.get("_type").and_then(Value::as_str) == Some("location")
        {
            let id = request
                .get_str("tid")
                .or_else(|| request.get_str("device"))
                .or_else(|| request.get_str("user"))
                .or_else(|| body.get("tid").and_then(as_string));
            if let Some(event) = owntracks_event_from_value(id, body) {
                return Some(event);
            }
        }
        let tid = request.get_str("tid").or_else(|| request.get_str("device"));
        if tid.is_some() && request.get_f64("lat").is_some() && request.get_f64("lon").is_some() {
            let id = tid.map(|value| format!("owntracks:{}", value));
            if let Some(id) = id
                && valid_device_id(&id)
            {
                let lat = request.get_f64("lat").unwrap();
                let lon = request.get_f64("lon").unwrap();
                if valid_position(lat, lon) {
                    return Some(make_event(
                        id,
                        lat,
                        lon,
                        request.get_f64("vel").or_else(|| request.get_f64("speed")),
                        request
                            .get_f64("cog")
                            .or_else(|| request.get_f64("heading")),
                        request
                            .get_f64("batt")
                            .or_else(|| request.get_f64("battery")),
                        Protocol::OwnTracks,
                        DeviceType::Tracker,
                    ));
                }
            }
        }
        None
    }
}

fn owntracks_event(topic: &str, value: &Value) -> Option<IngestEvent> {
    let lat = as_f64(value.get("lat")?)?;
    let lon = as_f64(value.get("lon")?)?;
    if !valid_position(lat, lon) {
        return None;
    }
    let id = if topic.split('/').count() >= 3 {
        let mut parts = topic.split('/');
        let _ = parts.next();
        let user = parts.next()?;
        let device = parts.next()?;
        format!("owntracks:{}:{}", user, device)
    } else {
        let tid = value.get("tid").and_then(as_string)?;
        format!("owntracks:{}", tid)
    };
    if !valid_device_id(&id) {
        return None;
    }
    Some(make_event(
        id,
        lat,
        lon,
        value.get("vel").and_then(as_f64),
        value.get("cog").and_then(as_f64),
        value.get("batt").and_then(as_f64),
        Protocol::OwnTracks,
        DeviceType::Tracker,
    ))
}

fn owntracks_event_from_value(id: Option<String>, value: &Value) -> Option<IngestEvent> {
    let lat = as_f64(value.get("lat")?)?;
    let lon = as_f64(value.get("lon")?)?;
    if !valid_position(lat, lon) {
        return None;
    }
    let id = id
        .map(|value| {
            if value.starts_with("owntracks:") {
                value
            } else {
                format!("owntracks:{}", value)
            }
        })
        .or_else(|| {
            value
                .get("tid")
                .and_then(as_string)
                .map(|t| format!("owntracks:{}", t))
        })
        .filter(|value| valid_device_id(value))?;
    Some(make_event(
        id,
        lat,
        lon,
        value.get("vel").and_then(as_f64),
        value.get("cog").and_then(as_f64),
        value.get("batt").and_then(as_f64),
        Protocol::OwnTracks,
        DeviceType::Tracker,
    ))
}

// ---------- TTN / LoRaWAN ----------

pub struct TtnAdapter;

impl MqttAdapter for TtnAdapter {
    fn name(&self) -> &'static str {
        "ttn"
    }

    fn detect(&self, topic: &str, payload: &[u8]) -> Option<Vec<AdapterAction>> {
        let value: Value = serde_json::from_slice(payload).ok()?;
        ttn_event(topic, &value).map(|event| vec![AdapterAction::Apply(event)])
    }
}

impl HttpAdapter for TtnAdapter {
    fn name(&self) -> &'static str {
        "ttn"
    }

    fn detect(&self, request: &HttpRequest) -> Option<IngestEvent> {
        if let Some(body) = &request.body
            && let Some(event) = ttn_event("", body)
        {
            return Some(event);
        }
        None
    }
}

fn ttn_event(_topic: &str, value: &Value) -> Option<IngestEvent> {
    let end = value.get("end_device_ids")?;
    let raw_id = end.get("device_id").and_then(as_string)?;
    let uplink = value.get("uplink_message")?;
    let decoded = uplink
        .get("decoded_payload")
        .or_else(|| uplink.get("decodedPayload"))?;
    let lat = as_f64(
        decoded
            .get("latitude")
            .or_else(|| decoded.get("lat"))
            .or_else(|| decoded.get("Latitude"))?,
    )?;
    let lon = as_f64(
        decoded
            .get("longitude")
            .or_else(|| decoded.get("lon"))
            .or_else(|| decoded.get("Longitude"))?,
    )?;
    if !valid_position(lat, lon) {
        return None;
    }
    let id = format!("ttn:{}", raw_id);
    if !valid_device_id(&id) {
        return None;
    }
    Some(make_event(
        id,
        lat,
        lon,
        decoded
            .get("speed")
            .and_then(as_f64)
            .or_else(|| decoded.get("speed_kmh").and_then(as_f64)),
        decoded
            .get("heading")
            .and_then(as_f64)
            .or_else(|| decoded.get("course").and_then(as_f64)),
        decoded
            .get("battery")
            .and_then(as_f64)
            .or_else(|| decoded.get("batt").and_then(as_f64)),
        Protocol::Lorawan,
        DeviceType::Tracker,
    ))
}

// ---------- OsmAnd ----------

pub struct OsmAndAdapter;

impl HttpAdapter for OsmAndAdapter {
    fn name(&self) -> &'static str {
        "osmand"
    }

    fn detect(&self, request: &HttpRequest) -> Option<IngestEvent> {
        let lat = request
            .get_f64("lat")
            .or_else(|| request.get_f64("latitude"))?;
        let lon = request
            .get_f64("lon")
            .or_else(|| request.get_f64("longitude"))?;
        if !valid_position(lat, lon) {
            return None;
        }
        let raw_id = request
            .get_str("deviceid")
            .or_else(|| request.get_str("device"))
            .or_else(|| request.get_str("id"))
            .or_else(|| request.get_str("imei"))
            .or_else(|| request.get_str("tracker"))?;
        let id = clean_id(Some(raw_id))?;
        let speed_kmh = parse_speed_kmh(request, &["speed_kmh", "speed_ms"]);
        let heading = parse_heading(request);
        let battery = parse_battery(request);
        Some(make_event(
            id,
            lat,
            lon,
            speed_kmh,
            heading,
            battery,
            Protocol::OsmAnd,
            DeviceType::Tracker,
        ))
    }
}

// ---------- Traccar ----------

pub struct TraccarAdapter;

impl HttpAdapter for TraccarAdapter {
    fn name(&self) -> &'static str {
        "traccar"
    }

    fn detect(&self, request: &HttpRequest) -> Option<IngestEvent> {
        let lat = request
            .get_f64("lat")
            .or_else(|| request.get_f64("latitude"))?;
        let lon = request
            .get_f64("lon")
            .or_else(|| request.get_f64("longitude"))?;
        if !valid_position(lat, lon) {
            return None;
        }
        let raw_id = request
            .get_str("id")
            .or_else(|| request.get_str("device"))
            .or_else(|| request.get_str("deviceid"))
            .or_else(|| request.get_str("imei"))
            .or_else(|| request.get_str("tracker"))?;
        let id = clean_id(Some(raw_id))?;
        let speed_kmh = if let Some(value) = request.get_f64("speed_kmh") {
            Some(value)
        } else if let Some(value) = request.get_f64("speed_ms") {
            Some(value * 3.6)
        } else {
            request.get_f64("speed").map(|value| value * 1.852)
        };
        let heading = parse_heading(request);
        let battery = parse_battery(request);
        Some(make_event(
            id,
            lat,
            lon,
            speed_kmh,
            heading,
            battery,
            Protocol::Traccar,
            DeviceType::Tracker,
        ))
    }
}

// ---------- Generic HTTP ----------

pub struct GenericHttpAdapter;

impl HttpAdapter for GenericHttpAdapter {
    fn name(&self) -> &'static str {
        "generic_http"
    }

    fn detect(&self, request: &HttpRequest) -> Option<IngestEvent> {
        let lat = request
            .get_f64("lat")
            .or_else(|| request.get_f64("latitude"))?;
        let lon = request
            .get_f64("lon")
            .or_else(|| request.get_f64("longitude"))?;
        if !valid_position(lat, lon) {
            return None;
        }
        let raw_id = request
            .get_str("device_id")
            .or_else(|| request.get_str("id"))
            .or_else(|| request.get_str("deviceid"))
            .or_else(|| request.get_str("device"))
            .or_else(|| request.get_str("imei"))
            .or_else(|| request.get_str("tracker"))?;
        let id = clean_id(Some(raw_id))?;
        let speed_kmh = parse_speed_kmh(request, &["speed_kmh", "speed_ms"]);
        let heading = parse_heading(request);
        let battery = parse_battery(request);
        Some(make_event(
            id,
            lat,
            lon,
            speed_kmh,
            heading,
            battery,
            Protocol::GenericHttp,
            DeviceType::Tracker,
        ))
    }
}

// ---------- Generic MQTT ----------

pub struct GenericMqttAdapter;

impl MqttAdapter for GenericMqttAdapter {
    fn name(&self) -> &'static str {
        "generic_mqtt"
    }

    fn detect(&self, topic: &str, payload: &[u8]) -> Option<Vec<AdapterAction>> {
        let value: Value = serde_json::from_slice(payload).ok()?;
        let lat = as_f64(value.get("lat").or_else(|| value.get("latitude"))?)?;
        let lon = as_f64(value.get("lon").or_else(|| value.get("longitude"))?)?;
        if !valid_position(lat, lon) {
            return None;
        }
        let raw_id = value
            .get("device_id")
            .or_else(|| value.get("id"))
            .or_else(|| value.get("deviceid"))
            .or_else(|| value.get("device"))
            .or_else(|| value.get("imei"))
            .or_else(|| value.get("tracker"))
            .and_then(as_string)
            .or_else(|| topic.split('/').next_back().map(str::to_string));
        let id = clean_id(raw_id)?;
        let speed_kmh = value
            .get("speed_kmh")
            .and_then(as_f64)
            .or_else(|| {
                value
                    .get("speed_ms")
                    .and_then(as_f64)
                    .map(|value| value * 3.6)
            })
            .or_else(|| value.get("speed").and_then(as_f64));
        let heading = value
            .get("heading")
            .and_then(as_f64)
            .or_else(|| value.get("course").and_then(as_f64))
            .or_else(|| value.get("bearing").and_then(as_f64))
            .or_else(|| value.get("cog").and_then(as_f64));
        let battery = value
            .get("battery")
            .and_then(as_f64)
            .or_else(|| value.get("batt").and_then(as_f64))
            .or_else(|| value.get("battery_level").and_then(as_f64));
        Some(vec![AdapterAction::Apply(make_event(
            id,
            lat,
            lon,
            speed_kmh,
            heading,
            battery,
            Protocol::GenericMqtt,
            DeviceType::Tracker,
        ))])
    }
}

// ---------- HTTP adapter lists ----------

pub fn http_adapters() -> Vec<Box<dyn HttpAdapter>> {
    vec![
        Box::new(OwnTracksAdapter),
        Box::new(OsmAndAdapter),
        Box::new(TraccarAdapter),
        Box::new(TtnAdapter),
        Box::new(GenericHttpAdapter),
    ]
}

pub fn mqtt_adapters(
    panic_keywords: Vec<String>,
    meshtastic_psks: Vec<String>,
) -> Vec<Box<dyn MqttAdapter>> {
    vec![
        Box::new(MeshtasticAdapter {
            panic_keywords: panic_keywords.clone(),
        }),
        Box::new(crate::meshtastic_proto::MeshtasticProtoAdapter::new(
            panic_keywords,
            meshtastic_psks,
        )),
        Box::new(OwnTracksAdapter),
        Box::new(TtnAdapter),
        Box::new(GenericMqttAdapter),
    ]
}

pub fn detect_http(
    request: &HttpRequest,
    adapters: &[Box<dyn HttpAdapter>],
) -> Option<IngestEvent> {
    for adapter in adapters {
        if let Some(event) = adapter.detect(request) {
            return Some(event);
        }
    }
    None
}

// ---------- Universal MQTT runner ----------

pub async fn run_mqtt_universal(
    registry: Arc<Registry>,
    config: crate::mqtt::MqttConfig,
    adapters: Vec<Box<dyn MqttAdapter>>,
    raw: Arc<RawCaptureStore>,
) -> anyhow::Result<()> {
    let broker = crate::mqtt::parse_broker_url(&config.url)?;

    loop {
        if let Err(error) =
            connect_and_poll(registry.clone(), &config, &broker, &adapters, &raw).await
        {
            warn!(%error, "MQTT adapter subscriber lost connection, reconnecting in 5s");
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn connect_and_poll(
    registry: Arc<Registry>,
    config: &crate::mqtt::MqttConfig,
    broker: &crate::mqtt::BrokerAddress,
    adapters: &[Box<dyn MqttAdapter>],
    raw: &RawCaptureStore,
) -> anyhow::Result<()> {
    let mut options = MqttOptions::new(&config.client_id, &broker.host, broker.port);
    options.set_keep_alive(Duration::from_secs(30));
    options.set_clean_session(false);

    if let Some(username) = &broker.username {
        options.set_credentials(username, broker.password.as_deref().unwrap_or(""));
    }

    let (client, mut eventloop) = AsyncClient::new(options, 64);
    info!(
        host = %broker.host,
        port = broker.port,
        topic = %config.topic,
        "connecting universal MQTT adapter subscriber"
    );

    let mut unmatched_log: HashMap<String, Instant> = HashMap::new();

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                client
                    .subscribe(config.topic.clone(), QoS::AtLeastOnce)
                    .await?;
                info!(topic = %config.topic, "subscribed universal MQTT adapter topic");
            }
            Ok(Event::Incoming(Packet::Publish(publish))) => {
                let mut applied = false;
                let mut reason: Option<&'static str> = None;
                for adapter in adapters {
                    match adapter.detect_outcome(&publish.topic, &publish.payload) {
                        DetectOutcome::Handled(actions) => {
                            let has_actions = !actions.is_empty();
                            apply_actions(&registry, actions).await;
                            if has_actions {
                                info!(adapter = adapter.name(), topic = %publish.topic, "MQTT adapter matched");
                            } else {
                                debug!(
                                    adapter = adapter.name(),
                                    topic = %publish.topic,
                                    "MQTT adapter handled message without actions"
                                );
                            }
                            applied = true;
                            break;
                        }
                        DetectOutcome::Skipped { reason: skipped } => {
                            reason = Some(skipped);
                            applied = true;
                            break;
                        }
                        DetectOutcome::NoMatch => {}
                    }
                }

                if !applied {
                    reason = Some("no_adapter");
                }

                if let Some(reason) = reason {
                    let captured = raw
                        .record(
                            CaptureSource::Mqtt,
                            &publish.topic,
                            reason,
                            &publish.payload,
                        )
                        .await;
                    registry.notify_raw(captured.device_key, raw.count().await);

                    if should_log(&mut unmatched_log, &publish.topic, Duration::from_secs(60)) {
                        warn!(
                            topic = %publish.topic,
                            len = publish.payload.len(),
                            reason = reason,
                            preview = %payload_preview(&publish.payload),
                            "unsupported MQTT message captured"
                        );
                    }
                }
            }
            Ok(_) => {}
            Err(error) => return Err(error.into()),
        }
    }
}

fn should_log(logs: &mut HashMap<String, Instant>, key: &str, interval: Duration) -> bool {
    let now = Instant::now();
    match logs.get(key) {
        Some(last) if now.duration_since(*last) < interval => false,
        _ => {
            logs.insert(key.to_string(), now);
            true
        }
    }
}

fn payload_preview(payload: &[u8]) -> String {
    let limit = payload.len().min(64);
    match std::str::from_utf8(&payload[..limit]) {
        Ok(text) => text.escape_default().to_string(),
        Err(_) => payload[..limit]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn owntracks_lwt_is_handled_without_actions() {
        let adapter = OwnTracksAdapter;
        let outcome = adapter.detect_outcome("owntracks/meshtastic/a32x", br#"{"_type":"lwt"}"#);
        assert!(matches!(outcome, DetectOutcome::Handled(actions) if actions.is_empty()));

        let outcome = adapter.detect_outcome("owntracks/meshtastic/a32x", b"lwt\n");
        assert!(matches!(outcome, DetectOutcome::Handled(actions) if actions.is_empty()));
    }

    #[test]
    fn owntracks_location_still_produces_event() {
        let payload = json!({
            "_type": "location",
            "lat": -6.2,
            "lon": 106.8,
            "tid": "a32x"
        })
        .to_string();
        let outcome =
            OwnTracksAdapter.detect_outcome("owntracks/meshtastic/a32x", payload.as_bytes());
        let DetectOutcome::Handled(actions) = outcome else {
            panic!("expected location to be handled");
        };
        assert_eq!(actions.len(), 1);
        let AdapterAction::Apply(event) = &actions[0] else {
            panic!("expected apply action");
        };
        assert_eq!(event.device_id, "owntracks:meshtastic:a32x");
    }
}

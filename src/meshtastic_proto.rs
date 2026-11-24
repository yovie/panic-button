use crate::adapters::{AdapterAction, MqttAdapter};
use crate::{DeviceType, EventKind, IngestEvent, Protocol, valid_position};
use aes::cipher::{KeyIvInit, StreamCipher};
use aes::{Aes128, Aes256};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD};
use ctr::Ctr32BE;
use meshtastic_protobufs::meshtastic::{
    Data, PortNum, Position, ServiceEnvelope, Telemetry, User, mesh_packet, telemetry,
};
use prost::Message;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

const DEFAULT_PSK: [u8; 16] = [
    0xd4, 0xf1, 0xbb, 0x3a, 0x20, 0x29, 0x07, 0x59, 0xf0, 0xbc, 0xff, 0xab, 0xcf, 0x4e, 0x69, 0x01,
];

const PKI_LOG_INTERVAL: Duration = Duration::from_secs(300);
const DECRYPT_LOG_INTERVAL: Duration = Duration::from_secs(60);

pub struct MeshtasticProtoAdapter {
    panic_keywords: Vec<String>,
    keys: Vec<Vec<u8>>,
    logs: Mutex<HashMap<String, Instant>>,
}

impl MeshtasticProtoAdapter {
    pub fn new(panic_keywords: Vec<String>, psks: Vec<String>) -> Self {
        let mut keys = Vec::new();
        for psk in psks {
            match parse_psk(&psk) {
                Some(key) => keys.push(key),
                None => warn!(psk = %psk, "ignoring invalid Meshtastic PSK"),
            }
        }
        if keys.is_empty() {
            keys.push(DEFAULT_PSK.to_vec());
        }
        Self::with_keys(panic_keywords, keys)
    }

    pub fn with_keys(panic_keywords: Vec<String>, keys: Vec<Vec<u8>>) -> Self {
        Self {
            panic_keywords,
            keys,
            logs: Mutex::new(HashMap::new()),
        }
    }

    fn should_log(&self, key: &str, interval: Duration) -> bool {
        let now = Instant::now();
        let mut logs = self
            .logs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match logs.get(key) {
            Some(last) if now.duration_since(*last) < interval => false,
            _ => {
                logs.insert(key.to_string(), now);
                true
            }
        }
    }

    fn decrypt_data(&self, from: u32, packet_id: u32, encrypted: &[u8]) -> Option<Data> {
        let nonce = packet_nonce(packet_id, from);
        for key in &self.keys {
            if key.is_empty() {
                if let Ok(data) = Data::decode(encrypted) {
                    return Some(data);
                }
                continue;
            }
            let mut buffer = encrypted.to_vec();
            match key.len() {
                16 => {
                    let Ok(mut cipher) = Ctr32BE::<Aes128>::new_from_slices(key, &nonce) else {
                        continue;
                    };
                    cipher.apply_keystream(&mut buffer);
                }
                32 => {
                    let Ok(mut cipher) = Ctr32BE::<Aes256>::new_from_slices(key, &nonce) else {
                        continue;
                    };
                    cipher.apply_keystream(&mut buffer);
                }
                _ => continue,
            }
            if let Ok(data) = Data::decode(buffer.as_slice()) {
                return Some(data);
            }
        }
        None
    }

    fn data_actions(&self, device_id: &str, data: &Data) -> Vec<AdapterAction> {
        let mut actions = Vec::new();
        match data.portnum {
            port if port == PortNum::PositionApp as i32 => {
                if let Ok(position) = Position::decode(data.payload.as_slice())
                    && let Some(action) = position_action(device_id, position)
                {
                    actions.push(action);
                }
            }
            port if port == PortNum::NodeinfoApp as i32 => {
                if let Ok(user) = User::decode(data.payload.as_slice())
                    && let Some(action) = nodeinfo_action(device_id, user)
                {
                    actions.push(action);
                }
            }
            port if port == PortNum::TelemetryApp as i32 => {
                if let Ok(telemetry) = Telemetry::decode(data.payload.as_slice())
                    && let Some(action) = telemetry_action(device_id, telemetry)
                {
                    actions.push(action);
                }
            }
            port if port == PortNum::TextMessageApp as i32 => {
                if let Some(action) = self.text_action(device_id, &data.payload) {
                    actions.push(action);
                }
            }
            port if port == PortNum::AlertApp as i32
                || port == PortNum::DetectionSensorApp as i32 =>
            {
                actions.push(AdapterAction::SetPanic {
                    id: device_id.to_string(),
                    panic: true,
                });
            }
            _ => {
                debug!(
                    portnum = data.portnum,
                    device_id, "ignoring unsupported Meshtastic portnum"
                );
            }
        }
        actions
    }

    fn text_action(&self, device_id: &str, payload: &[u8]) -> Option<AdapterAction> {
        let text = String::from_utf8_lossy(payload).to_lowercase();
        let triggered = self
            .panic_keywords
            .iter()
            .any(|keyword| !keyword.is_empty() && text.contains(&keyword.to_lowercase()));
        triggered.then(|| AdapterAction::SetPanic {
            id: device_id.to_string(),
            panic: true,
        })
    }
}

impl MqttAdapter for MeshtasticProtoAdapter {
    fn name(&self) -> &'static str {
        "meshtastic_proto"
    }

    fn detect(&self, topic: &str, payload: &[u8]) -> Option<Vec<AdapterAction>> {
        if !is_meshtastic_proto_topic(topic) {
            return None;
        }
        let envelope = ServiceEnvelope::decode(payload).ok()?;
        let ServiceEnvelope {
            packet, channel_id, ..
        } = envelope;
        let Some(packet) = packet else {
            return Some(Vec::new());
        };
        let from = packet.from;
        if from == 0 {
            return Some(Vec::new());
        }
        let device_id = format!("!{from:08x}");
        let data = match packet.payload_variant {
            Some(mesh_packet::PayloadVariant::Decoded(data)) => data,
            Some(mesh_packet::PayloadVariant::Encrypted(encrypted)) => {
                if channel_id == "PKI" || packet.pki_encrypted {
                    if self.should_log(&format!("pki:{topic}"), PKI_LOG_INTERVAL) {
                        warn!(
                            topic,
                            from = %device_id,
                            to = packet.to,
                            channel = %channel_id,
                            "Meshtastic PKI packet cannot be decrypted without the node private key"
                        );
                    }
                    return Some(Vec::new());
                }
                match self.decrypt_data(from, packet.id, &encrypted) {
                    Some(data) => data,
                    None => {
                        if self.should_log(&format!("decrypt:{topic}"), DECRYPT_LOG_INTERVAL) {
                            warn!(
                                topic,
                                from = %device_id,
                                packet_id = packet.id,
                                len = encrypted.len(),
                                "Meshtastic channel packet decryption failed; check MESHTASTIC_PSK"
                            );
                        }
                        return Some(Vec::new());
                    }
                }
            }
            None => return Some(Vec::new()),
        };
        Some(self.data_actions(&device_id, &data))
    }
}

fn position_action(device_id: &str, position: Position) -> Option<AdapterAction> {
    let lat = position.latitude_i? as f64 * 1e-7;
    let lon = position.longitude_i? as f64 * 1e-7;
    if !valid_position(lat, lon) {
        return None;
    }
    let speed_kmh = position.ground_speed.map(|speed| speed as f64 * 3.6);
    let heading = position.ground_track.map(|track| track as f64 / 100.0);
    Some(AdapterAction::Apply(IngestEvent {
        device_id: device_id.to_string(),
        name: None,
        serial: None,
        lat,
        lon,
        speed_kmh,
        heading,
        battery: None,
        device_type: Some(DeviceType::PanicButton),
        protocol: Some(Protocol::Meshtastic),
        event: EventKind::Heartbeat,
    }))
}

fn nodeinfo_action(device_id: &str, user: User) -> Option<AdapterAction> {
    let name = if !user.long_name.trim().is_empty() {
        user.long_name.trim()
    } else if !user.short_name.trim().is_empty() {
        user.short_name.trim()
    } else {
        return None;
    };
    Some(AdapterAction::SetName {
        id: device_id.to_string(),
        name: name.to_string(),
    })
}

fn telemetry_action(device_id: &str, telemetry: Telemetry) -> Option<AdapterAction> {
    let Some(telemetry::Variant::DeviceMetrics(metrics)) = telemetry.variant else {
        return None;
    };
    let battery = metrics.battery_level? as f64;
    Some(AdapterAction::SetBattery {
        id: device_id.to_string(),
        battery,
    })
}

fn is_meshtastic_proto_topic(topic: &str) -> bool {
    if !topic.starts_with("msh/") {
        return false;
    }
    let mut previous = "";
    for part in topic.split('/') {
        if previous == "2" && (part == "e" || part == "c") {
            return true;
        }
        previous = part;
    }
    false
}

fn packet_nonce(packet_id: u32, from: u32) -> [u8; 16] {
    let mut nonce = [0u8; 16];
    nonce[..8].copy_from_slice(&(packet_id as u64).to_le_bytes());
    nonce[8..12].copy_from_slice(&from.to_le_bytes());
    nonce
}

fn parse_psk(raw: &str) -> Option<Vec<u8>> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    let decoded = STANDARD
        .decode(raw)
        .or_else(|_| STANDARD_NO_PAD.decode(raw))
        .ok()?;
    match decoded.len() {
        0 => Some(Vec::new()),
        1 => {
            let index = decoded[0];
            if index == 0 {
                Some(Vec::new())
            } else if index == 1 {
                Some(DEFAULT_PSK.to_vec())
            } else {
                let mut key = DEFAULT_PSK;
                key[15] = key[15].wrapping_add(index.wrapping_sub(1));
                Some(key.to_vec())
            }
        }
        16 | 32 => Some(decoded),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meshtastic_protobufs::meshtastic::MeshPacket;

    const FROM: u32 = 0x0000_c001;
    const PACKET_ID: u32 = 1234;
    const TOPIC: &str = "msh/US/2/e/LongFast/!0000c001";

    fn empty_data() -> Data {
        Data::decode(&[][..]).unwrap()
    }

    fn empty_position() -> Position {
        Position::decode(&[][..]).unwrap()
    }

    fn empty_user() -> User {
        User::decode(&[][..]).unwrap()
    }

    fn empty_telemetry() -> Telemetry {
        Telemetry::decode(&[][..]).unwrap()
    }

    fn empty_envelope() -> ServiceEnvelope {
        ServiceEnvelope::decode(&[][..]).unwrap()
    }

    fn position_data() -> Data {
        let mut position = empty_position();
        position.latitude_i = Some(-6_200_000);
        position.longitude_i = Some(106_800_000);
        position.ground_speed = Some(10);
        position.ground_track = Some(9_000);
        let mut data = empty_data();
        data.portnum = PortNum::PositionApp as i32;
        data.payload = position.encode_to_vec();
        data
    }

    fn envelope(data: Data, channel_id: &str) -> ServiceEnvelope {
        let mut packet = MeshPacket::decode(&[][..]).unwrap();
        packet.from = FROM;
        packet.to = u32::MAX;
        packet.id = PACKET_ID;
        packet.payload_variant = Some(mesh_packet::PayloadVariant::Decoded(data));
        let mut envelope = empty_envelope();
        envelope.packet = Some(packet);
        envelope.channel_id = channel_id.to_string();
        envelope.gateway_id = "!0000c001".to_string();
        envelope
    }

    fn encrypt(data: &Data, key: &[u8]) -> Vec<u8> {
        let nonce = packet_nonce(PACKET_ID, FROM);
        let mut buffer = data.encode_to_vec();
        match key.len() {
            16 => {
                let mut cipher = Ctr32BE::<Aes128>::new_from_slices(key, &nonce).unwrap();
                cipher.apply_keystream(&mut buffer);
            }
            32 => {
                let mut cipher = Ctr32BE::<Aes256>::new_from_slices(key, &nonce).unwrap();
                cipher.apply_keystream(&mut buffer);
            }
            _ => panic!("unsupported key length"),
        }
        buffer
    }

    fn encrypted_envelope(data: &Data, key: &[u8], channel_id: &str) -> ServiceEnvelope {
        let mut envelope = envelope(empty_data(), channel_id);
        let packet = envelope.packet.as_mut().unwrap();
        packet.payload_variant = Some(mesh_packet::PayloadVariant::Encrypted(encrypt(data, key)));
        envelope
    }

    fn adapter() -> MeshtasticProtoAdapter {
        MeshtasticProtoAdapter::with_keys(
            vec!["PANIC".into(), "SOS".into()],
            vec![DEFAULT_PSK.to_vec()],
        )
    }

    #[test]
    fn decodes_decoded_position() {
        let bytes = envelope(position_data(), "LongFast").encode_to_vec();
        let actions = adapter().detect(TOPIC, &bytes).unwrap();
        assert_eq!(actions.len(), 1);
        let AdapterAction::Apply(event) = &actions[0] else {
            panic!("expected position event");
        };
        assert_eq!(event.device_id, "!0000c001");
        assert!((event.lat - -0.62).abs() < 1e-9);
        assert!((event.lon - 10.68).abs() < 1e-9);
        assert_eq!(event.speed_kmh, Some(36.0));
        assert_eq!(event.heading, Some(90.0));
        assert_eq!(event.protocol, Some(Protocol::Meshtastic));
    }

    #[test]
    fn decrypts_position_with_default_psk() {
        let bytes = encrypted_envelope(&position_data(), &DEFAULT_PSK, "LongFast").encode_to_vec();
        let actions = adapter().detect(TOPIC, &bytes).unwrap();
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], AdapterAction::Apply(_)));
    }

    #[test]
    fn decrypts_position_with_custom_psk() {
        let key = [0x42u8; 16];
        let adapter = MeshtasticProtoAdapter::with_keys(vec![], vec![key.to_vec()]);
        let bytes = encrypted_envelope(&position_data(), &key, "Panic").encode_to_vec();
        let actions = adapter.detect(TOPIC, &bytes).unwrap();
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], AdapterAction::Apply(_)));
    }

    #[test]
    fn skips_pki_packets() {
        let bytes = encrypted_envelope(&position_data(), &DEFAULT_PSK, "PKI").encode_to_vec();
        let actions = adapter()
            .detect("msh/US/2/e/PKI/!0000c001", &bytes)
            .unwrap();
        assert!(actions.is_empty());
    }

    #[test]
    fn nodeinfo_sets_name() {
        let mut user = empty_user();
        user.long_name = "Tim Alpha".into();
        user.short_name = "TA".into();
        let mut data = empty_data();
        data.portnum = PortNum::NodeinfoApp as i32;
        data.payload = user.encode_to_vec();
        let bytes = envelope(data, "LongFast").encode_to_vec();
        let actions = adapter().detect(TOPIC, &bytes).unwrap();
        assert!(matches!(
            &actions[0],
            AdapterAction::SetName { name, .. } if name == "Tim Alpha"
        ));
    }

    #[test]
    fn telemetry_sets_battery() {
        let mut metrics = meshtastic_protobufs::meshtastic::DeviceMetrics::decode(&[][..]).unwrap();
        metrics.battery_level = Some(88);
        let mut telemetry = empty_telemetry();
        telemetry.variant = Some(telemetry::Variant::DeviceMetrics(metrics));
        let mut data = empty_data();
        data.portnum = PortNum::TelemetryApp as i32;
        data.payload = telemetry.encode_to_vec();
        let bytes = envelope(data, "LongFast").encode_to_vec();
        let actions = adapter().detect(TOPIC, &bytes).unwrap();
        assert!(matches!(
            &actions[0],
            AdapterAction::SetBattery { battery, .. } if *battery == 88.0
        ));
    }

    #[test]
    fn panic_keyword_sets_panic() {
        let mut data = empty_data();
        data.portnum = PortNum::TextMessageApp as i32;
        data.payload = b"tolong SOS sekarang".to_vec();
        let bytes = envelope(data, "LongFast").encode_to_vec();
        let actions = adapter().detect(TOPIC, &bytes).unwrap();
        assert!(matches!(
            &actions[0],
            AdapterAction::SetPanic { panic: true, .. }
        ));
    }

    #[test]
    fn alert_and_detection_sensor_set_panic() {
        for portnum in [PortNum::AlertApp, PortNum::DetectionSensorApp] {
            let mut data = empty_data();
            data.portnum = portnum as i32;
            data.payload = b"triggered".to_vec();
            let bytes = envelope(data, "LongFast").encode_to_vec();
            let actions = adapter().detect(TOPIC, &bytes).unwrap();
            assert!(matches!(
                &actions[0],
                AdapterAction::SetPanic { panic: true, .. }
            ));
        }
    }

    #[test]
    fn ignores_json_topic() {
        assert!(
            adapter()
                .detect("msh/US/2/json/LongFast/!0000c001", b"{}")
                .is_none()
        );
    }

    #[test]
    fn parses_default_and_custom_psks() {
        assert_eq!(parse_psk("AQ=="), Some(DEFAULT_PSK.to_vec()));
        let custom = STANDARD.encode([0x42u8; 16]);
        assert_eq!(parse_psk(&custom), Some(vec![0x42; 16]));
        let mut expanded = DEFAULT_PSK;
        expanded[15] = expanded[15].wrapping_add(1);
        assert_eq!(parse_psk("Ag=="), Some(expanded.to_vec()));
        assert_eq!(parse_psk("AA=="), Some(Vec::new()));
        assert_eq!(parse_psk("not-base64!"), None);
    }
}

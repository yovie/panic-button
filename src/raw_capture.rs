use base64::Engine;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tracing::warn;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureSource {
    Mqtt,
    Http,
}

impl CaptureSource {
    pub fn as_str(self) -> &'static str {
        match self {
            CaptureSource::Mqtt => "mqtt",
            CaptureSource::Http => "http",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawPayload {
    pub id: String,
    pub device_key: String,
    pub received_at: DateTime<Utc>,
    pub source: CaptureSource,
    pub topic: String,
    pub reason: String,
    #[serde(default)]
    pub payload_text: Option<String>,
    pub payload_b64: String,
    pub byte_count: usize,
    #[serde(default)]
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RawDevice {
    pub key: String,
    pub count: usize,
    pub last_seen: DateTime<Utc>,
    pub sources: Vec<String>,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RawCaptureLimits {
    pub max_entries: usize,
    pub max_per_device: usize,
    pub max_bytes: usize,
    pub ttl: Duration,
}

impl Default for RawCaptureLimits {
    fn default() -> Self {
        Self {
            max_entries: 500,
            max_per_device: 50,
            max_bytes: 8192,
            ttl: Duration::from_secs(7 * 24 * 60 * 60),
        }
    }
}

pub struct RawCaptureStore {
    path: PathBuf,
    limits: RawCaptureLimits,
    entries: RwLock<VecDeque<RawPayload>>,
    saving: Mutex<()>,
}

impl RawCaptureStore {
    pub async fn load(path: impl AsRef<Path>, limits: RawCaptureLimits) -> Self {
        let path = path.as_ref().to_path_buf();
        let mut entries = match tokio::fs::read(&path).await {
            Ok(bytes) => match serde_json::from_slice::<Vec<RawPayload>>(&bytes) {
                Ok(payloads) => payloads.into_iter().collect(),
                Err(error) => {
                    warn!(%error, path = %path.display(), "ignoring invalid raw capture file");
                    VecDeque::new()
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => VecDeque::new(),
            Err(error) => {
                warn!(%error, path = %path.display(), "could not read raw capture file");
                VecDeque::new()
            }
        };
        let cutoff = Utc::now() - ChronoDuration::seconds(limits.ttl.as_secs() as i64);
        entries.retain(|entry| entry.received_at >= cutoff);
        while entries.len() > limits.max_entries {
            entries.pop_front();
        }
        Self {
            path,
            limits,
            entries: RwLock::new(entries),
            saving: Mutex::new(()),
        }
    }

    pub async fn record(
        &self,
        source: CaptureSource,
        topic: &str,
        reason: &str,
        bytes: &[u8],
    ) -> RawPayload {
        let device_key = extract_device_key(topic, bytes);
        let (payload_text, redacted_bytes) = redact_payload(bytes, self.limits.max_bytes);

        let payload = RawPayload {
            id: format!(
                "{}-{}",
                Utc::now().timestamp_millis(),
                NEXT_ID.fetch_add(1, Ordering::Relaxed)
            ),
            device_key: device_key.clone(),
            received_at: Utc::now(),
            source,
            topic: sanitize(topic, 256),
            reason: sanitize(reason, 64),
            payload_text,
            payload_b64: base64::engine::general_purpose::STANDARD.encode(&redacted_bytes),
            byte_count: bytes.len(),
            truncated: bytes.len() > self.limits.max_bytes,
        };

        {
            let mut entries = self.entries.write().await;
            entries.push_back(payload.clone());

            let cutoff = Utc::now() - ChronoDuration::seconds(self.limits.ttl.as_secs() as i64);
            entries.retain(|entry| entry.received_at >= cutoff);

            let mut counts: HashMap<String, usize> = HashMap::new();
            let mut remove = Vec::new();
            for (index, entry) in entries.iter().enumerate().rev() {
                let count = counts.entry(entry.device_key.clone()).or_insert(0);
                if *count >= self.limits.max_per_device {
                    remove.push(index);
                } else {
                    *count += 1;
                }
            }
            for index in remove {
                entries.remove(index);
            }
            while entries.len() > self.limits.max_entries {
                entries.pop_front();
            }
        }

        if let Err(error) = self.save().await {
            warn!(%error, "failed to persist raw capture");
        }
        payload
    }

    pub async fn count(&self) -> usize {
        self.entries.read().await.len()
    }

    pub async fn devices(&self) -> Vec<RawDevice> {
        let entries = self.entries.read().await;
        let mut grouped: HashMap<String, RawDevice> = HashMap::new();
        for entry in entries.iter() {
            let group = grouped
                .entry(entry.device_key.clone())
                .or_insert_with(|| RawDevice {
                    key: entry.device_key.clone(),
                    count: 0,
                    last_seen: entry.received_at,
                    sources: Vec::new(),
                    reasons: Vec::new(),
                });
            group.count += 1;
            if entry.received_at > group.last_seen {
                group.last_seen = entry.received_at;
            }
            let source = entry.source.as_str().to_string();
            if !group.sources.contains(&source) {
                group.sources.push(source);
            }
            if !group.reasons.contains(&entry.reason) {
                group.reasons.push(entry.reason.clone());
            }
        }
        let mut devices: Vec<RawDevice> = grouped.into_values().collect();
        devices.sort_by(|a, b| {
            b.last_seen
                .cmp(&a.last_seen)
                .then_with(|| a.key.cmp(&b.key))
        });
        devices
    }

    pub async fn history(&self, key: &str) -> Vec<RawPayload> {
        let entries = self.entries.read().await;
        let mut list: Vec<RawPayload> = entries
            .iter()
            .filter(|entry| entry.device_key == key)
            .cloned()
            .collect();
        list.sort_by_key(|entry| std::cmp::Reverse(entry.received_at));
        list
    }

    pub async fn clear(&self, key: &str) -> usize {
        let removed = {
            let mut entries = self.entries.write().await;
            let before = entries.len();
            entries.retain(|entry| entry.device_key != key);
            before - entries.len()
        };
        if removed > 0
            && let Err(error) = self.save().await
        {
            warn!(%error, "failed to persist raw capture");
        }
        removed
    }

    pub async fn clear_all(&self) -> usize {
        let removed = {
            let mut entries = self.entries.write().await;
            let before = entries.len();
            entries.clear();
            before
        };
        if removed > 0
            && let Err(error) = self.save().await
        {
            warn!(%error, "failed to persist raw capture");
        }
        removed
    }

    pub async fn save(&self) -> anyhow::Result<()> {
        let _guard = self.saving.lock().await;
        let payloads: Vec<RawPayload> = self.entries.read().await.iter().cloned().collect();
        let json = serde_json::to_string_pretty(&payloads)?;

        if let Some(parent) = self.path.parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent).await?;
        }
        let temporary = self.path.with_extension("json.tmp");
        tokio::fs::write(&temporary, json).await?;
        tokio::fs::rename(&temporary, &self.path).await?;
        Ok(())
    }
}

pub fn extract_device_key(topic: &str, bytes: &[u8]) -> String {
    if let Ok(text) = std::str::from_utf8(bytes)
        && let Ok(value) = serde_json::from_str::<Value>(text)
        && let Some(found) = find_id(&value, 0)
    {
        return found;
    }

    let parts: Vec<&str> = topic.split('/').collect();
    if let Some(last) = parts.last()
        && is_plausible_id(last)
    {
        return sanitize(last, 128);
    }
    sanitize(topic, 128)
}

fn find_id(value: &Value, depth: usize) -> Option<String> {
    if depth > 4 {
        return None;
    }
    match value {
        Value::Object(map) => {
            for key in ["device_id", "imei", "node_id", "id", "from"] {
                let Some(found) = map.get(key) else { continue };
                let text = match found {
                    Value::String(text) => text.trim().to_string(),
                    Value::Number(number) => number.to_string(),
                    _ => continue,
                };
                if text.is_empty() {
                    continue;
                }
                if key == "from"
                    && text.chars().all(|digit| digit.is_ascii_digit())
                    && let Ok(number) = text.parse::<u32>()
                {
                    return Some(format!("!{number:08x}"));
                }
                return Some(sanitize(&text, 128));
            }
            for nested in map.values() {
                if let Some(found) = find_id(nested, depth + 1) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(items) => items.iter().find_map(|item| find_id(item, depth + 1)),
        _ => None,
    }
}

fn is_plausible_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "!-_.:".contains(character))
}

fn sanitize(value: &str, limit: usize) -> String {
    let cleaned: String = value.trim().chars().take(limit).collect();
    if cleaned.is_empty() {
        "unknown".to_string()
    } else {
        cleaned
    }
}

fn redact_payload(bytes: &[u8], max_bytes: usize) -> (Option<String>, Vec<u8>) {
    let mut prepared = bytes.to_vec();
    if let Ok(text) = std::str::from_utf8(bytes) {
        if let Ok(mut value) = serde_json::from_str::<Value>(text) {
            redact_json(&mut value);
            if let Ok(serialized) = serde_json::to_string(&value) {
                prepared = serialized.into_bytes();
            }
        } else if text.contains('=') {
            prepared = redact_query(text).into_bytes();
        }
    }

    if prepared.len() > max_bytes {
        prepared.truncate(max_bytes);
    }
    let text = String::from_utf8(prepared.clone()).ok();
    (text, prepared)
}

fn redact_json(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, nested) in map.iter_mut() {
                if is_sensitive_key(key) {
                    *nested = Value::String("***".to_string());
                } else {
                    redact_json(nested);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_json(item);
            }
        }
        _ => {}
    }
}

fn redact_query(text: &str) -> String {
    text.split('&')
        .map(|part| {
            let mut pieces = part.splitn(2, '=');
            let key = pieces.next().unwrap_or("");
            if is_sensitive_key(key) {
                format!("{key}=***")
            } else {
                part.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn is_sensitive_key(key: &str) -> bool {
    let lowered = key.to_ascii_lowercase();
    [
        "password",
        "passwd",
        "token",
        "authorization",
        "secret",
        "api_key",
        "apikey",
        "cookie",
        "credential",
    ]
    .iter()
    .any(|sensitive| lowered.contains(sensitive))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "panic-button-raw-{name}-{}.json",
            std::process::id()
        ))
    }

    #[test]
    fn extracts_id_from_topic() {
        let key = extract_device_key("msh/Panic/2/e/PKI/!a19450e7", b"\x08\x01");
        assert_eq!(key, "!a19450e7");
    }

    #[test]
    fn extracts_id_from_json_payload() {
        let payload = br#"{"device_id":"truck-01","lat":-6.2}"#;
        assert_eq!(extract_device_key("unknown/topic", payload), "truck-01");
    }

    #[test]
    fn converts_numeric_from_to_node_id() {
        let payload = br#"{"payload":{"from":3735928559}}"#;
        assert_eq!(extract_device_key("msh/x/2/json/0", payload), "!deadbeef");
    }

    #[test]
    fn redacts_sensitive_json_keys() {
        let payload =
            br#"{"device_id":"a","password":"rahasia","token":"abc","nested":{"api_key":"x"}}"#;
        let (text, bytes) = redact_payload(payload, 8192);
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["password"], "***");
        assert_eq!(value["token"], "***");
        assert_eq!(value["nested"]["api_key"], "***");
        assert_eq!(value["device_id"], "a");
        assert!(text.unwrap().contains("***"));
    }

    #[test]
    fn redacts_query_tokens() {
        let payload = b"lat=-6.2&token=abc123&password=rahasia";
        let (text, _) = redact_payload(payload, 8192);
        assert_eq!(text.as_deref(), Some("lat=-6.2&token=***&password=***"));
    }

    #[tokio::test]
    async fn enforces_limits_and_persists() {
        let path = temp_path("limits");
        let limits = RawCaptureLimits {
            max_entries: 40,
            max_per_device: 5,
            max_bytes: 8192,
            ttl: Duration::from_secs(3600),
        };
        let store = RawCaptureStore::load(&path, limits.clone()).await;
        for _ in 0..20 {
            store
                .record(
                    CaptureSource::Mqtt,
                    "msh/x/2/e/PKI/!abc",
                    "no_adapter",
                    b"payload",
                )
                .await;
        }
        assert_eq!(store.count().await, 5);
        assert_eq!(store.history("!abc").await.len(), 5);
        assert_eq!(store.devices().await.len(), 1);
        assert_eq!(store.devices().await[0].count, 5);

        let reloaded = RawCaptureStore::load(&path, limits).await;
        assert_eq!(reloaded.count().await, 5);
        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn clears_entries() {
        let path = temp_path("clear");
        let store = RawCaptureStore::load(&path, RawCaptureLimits::default()).await;
        store
            .record(
                CaptureSource::Http,
                "/api/ingest/auto",
                "http_no_adapter",
                b"{}",
            )
            .await;
        assert_eq!(store.count().await, 1);
        assert_eq!(store.clear("auto").await, 1);
        assert_eq!(store.count().await, 0);
        assert_eq!(store.clear_all().await, 0);
        tokio::fs::remove_file(&path).await.ok();
    }
}

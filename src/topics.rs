use crate::mqtt::{SubscriptionControl, effective_topics};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;
use tracing::warn;

pub const MAX_CUSTOM_TOPICS: usize = 32;
const MAX_TOPIC_LENGTH: usize = 256;
const MAX_LABEL_LENGTH: usize = 80;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopicEntry {
    pub topic: String,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CustomTopicView {
    pub topic: String,
    pub label: Option<String>,
    pub active: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicState {
    pub mqtt_enabled: bool,
    pub mqtt_connected: bool,
    pub baseline: Vec<String>,
    pub custom: Vec<CustomTopicView>,
    pub effective: Vec<String>,
}

#[derive(Debug)]
pub enum TopicError {
    Invalid(String),
    Duplicate(String),
    NotFound(String),
    Limit(String),
    Persist(anyhow::Error),
}

pub struct TopicStore {
    path: PathBuf,
    entries: Vec<TopicEntry>,
}

impl TopicStore {
    pub async fn load(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        let mut entries = match tokio::fs::read(&path).await {
            Ok(bytes) => match serde_json::from_slice::<Vec<TopicEntry>>(&bytes) {
                Ok(entries) => entries,
                Err(error) => {
                    warn!(%error, path = %path.display(), "ignoring invalid topic store file");
                    Vec::new()
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => {
                warn!(%error, path = %path.display(), "could not read topic store");
                Vec::new()
            }
        };

        entries.retain(|entry| validate_topic_filter(&entry.topic).is_ok());
        entries.sort_by(|a, b| a.topic.cmp(&b.topic));
        entries.dedup_by(|a, b| a.topic == b.topic);

        Self { path, entries }
    }

    pub fn entries(&self) -> Vec<TopicEntry> {
        self.entries.clone()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn contains(&self, topic: &str) -> bool {
        self.entries.iter().any(|entry| entry.topic == topic)
    }

    pub fn add(&mut self, entry: TopicEntry) -> bool {
        if self.contains(&entry.topic) {
            return false;
        }
        self.entries.push(entry);
        self.entries.sort_by(|a, b| a.topic.cmp(&b.topic));
        true
    }

    pub fn remove(&mut self, topic: &str) -> bool {
        let previous = self.entries.len();
        self.entries.retain(|entry| entry.topic != topic);
        self.entries.len() != previous
    }

    pub fn replace(&mut self, entries: Vec<TopicEntry>) {
        self.entries = entries;
        self.entries.sort_by(|a, b| a.topic.cmp(&b.topic));
    }

    pub async fn save(&self) -> anyhow::Result<()> {
        let json = serde_json::to_string_pretty(&self.entries)?;

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

pub struct TopicManager {
    baseline: Vec<String>,
    store: Mutex<TopicStore>,
    mutation: Mutex<()>,
    control: SubscriptionControl,
    mqtt_enabled: bool,
}

impl TopicManager {
    pub fn new(baseline: Vec<String>, store: TopicStore, mqtt_enabled: bool) -> Self {
        Self {
            baseline,
            store: Mutex::new(store),
            mutation: Mutex::new(()),
            control: SubscriptionControl::default(),
            mqtt_enabled,
        }
    }

    pub fn control(&self) -> &SubscriptionControl {
        &self.control
    }

    pub async fn effective_topics(&self) -> Vec<String> {
        let custom: Vec<String> = self
            .store
            .lock()
            .await
            .entries()
            .into_iter()
            .map(|entry| entry.topic)
            .collect();
        effective_topics(&self.baseline, &custom)
    }

    pub async fn state(&self) -> TopicState {
        let entries = self.store.lock().await.entries();
        let custom_topics: Vec<String> = entries.iter().map(|entry| entry.topic.clone()).collect();
        let effective = effective_topics(&self.baseline, &custom_topics);
        let custom = entries
            .into_iter()
            .map(|entry| {
                let active = effective.iter().any(|topic| topic == &entry.topic);
                CustomTopicView {
                    topic: entry.topic,
                    label: entry.label,
                    active,
                }
            })
            .collect();

        TopicState {
            mqtt_enabled: self.mqtt_enabled,
            mqtt_connected: self.control.is_connected().await,
            baseline: self.baseline.clone(),
            custom,
            effective,
        }
    }

    pub async fn add(
        &self,
        raw_topic: &str,
        raw_label: Option<String>,
    ) -> Result<TopicState, TopicError> {
        let topic = validate_topic_filter(raw_topic)?;
        let label = clean_label(raw_label)?;
        let _guard = self.mutation.lock().await;

        let (before, after) = {
            let mut store = self.store.lock().await;
            if store.contains(&topic) {
                return Err(TopicError::Duplicate(format!(
                    "topik {topic} sudah terdaftar"
                )));
            }
            if self.baseline.iter().any(|baseline| baseline == &topic) {
                return Err(TopicError::Duplicate(format!(
                    "topik {topic} sudah ada di topik environment"
                )));
            }
            if store.len() >= MAX_CUSTOM_TOPICS {
                return Err(TopicError::Limit(format!(
                    "maksimal {MAX_CUSTOM_TOPICS} topik tambahan"
                )));
            }

            let previous = store.entries();
            if !store.add(TopicEntry {
                topic: topic.clone(),
                label,
            }) {
                return Err(TopicError::Duplicate(format!(
                    "topik {topic} sudah terdaftar"
                )));
            }
            if let Err(error) = store.save().await {
                store.replace(previous);
                return Err(TopicError::Persist(error));
            }

            let before = effective_topics(
                &self.baseline,
                &previous
                    .into_iter()
                    .map(|entry| entry.topic)
                    .collect::<Vec<_>>(),
            );
            let after = effective_topics(
                &self.baseline,
                &store
                    .entries()
                    .into_iter()
                    .map(|entry| entry.topic)
                    .collect::<Vec<_>>(),
            );
            (before, after)
        };

        if let Err(error) = self.control.apply(&before, &after).await {
            warn!(%error, "failed to apply MQTT topic changes live");
        }

        Ok(self.state().await)
    }

    pub async fn remove(&self, raw_topic: &str) -> Result<TopicState, TopicError> {
        let topic = raw_topic.trim();
        if topic.is_empty() {
            return Err(TopicError::Invalid("topik wajib diisi".to_string()));
        }
        if self.baseline.iter().any(|baseline| baseline == topic) {
            return Err(TopicError::Invalid(
                "topik environment tidak bisa dihapus dari menu".to_string(),
            ));
        }

        let _guard = self.mutation.lock().await;

        let (before, after) = {
            let mut store = self.store.lock().await;
            let previous = store.entries();
            if !store.remove(topic) {
                return Err(TopicError::NotFound(format!(
                    "topik {topic} tidak ditemukan"
                )));
            }
            if let Err(error) = store.save().await {
                store.replace(previous);
                return Err(TopicError::Persist(error));
            }

            let before = effective_topics(
                &self.baseline,
                &previous
                    .into_iter()
                    .map(|entry| entry.topic)
                    .collect::<Vec<_>>(),
            );
            let after = effective_topics(
                &self.baseline,
                &store
                    .entries()
                    .into_iter()
                    .map(|entry| entry.topic)
                    .collect::<Vec<_>>(),
            );
            (before, after)
        };

        if let Err(error) = self.control.apply(&before, &after).await {
            warn!(%error, "failed to apply MQTT topic changes live");
        }

        Ok(self.state().await)
    }
}

pub fn validate_topic_filter(raw: &str) -> Result<String, TopicError> {
    let topic = raw.trim();
    if topic.is_empty() {
        return Err(TopicError::Invalid("topik wajib diisi".to_string()));
    }
    if topic.len() > MAX_TOPIC_LENGTH {
        return Err(TopicError::Invalid(format!(
            "topik maksimal {MAX_TOPIC_LENGTH} karakter"
        )));
    }
    if topic
        .chars()
        .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(TopicError::Invalid(
            "topik tidak boleh mengandung spasi atau karakter kontrol".to_string(),
        ));
    }

    let levels: Vec<&str> = topic.split('/').collect();
    for (index, level) in levels.iter().enumerate() {
        if level.contains('#') && (*level != "#" || index + 1 != levels.len()) {
            return Err(TopicError::Invalid(
                "wildcard # hanya boleh berada di akhir topik".to_string(),
            ));
        }
        if level.contains('+') && *level != "+" {
            return Err(TopicError::Invalid(
                "wildcard + harus mengisi satu level penuh".to_string(),
            ));
        }
    }

    Ok(topic.to_string())
}

fn clean_label(raw: Option<String>) -> Result<Option<String>, TopicError> {
    let Some(label) = raw
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    if label.len() > MAX_LABEL_LENGTH {
        return Err(TopicError::Invalid(format!(
            "label maksimal {MAX_LABEL_LENGTH} karakter"
        )));
    }
    if label.chars().any(char::is_control) {
        return Err(TopicError::Invalid(
            "label tidak boleh mengandung karakter kontrol".to_string(),
        ));
    }
    Ok(Some(label))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temporary_path() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("panic-button-topics-{unique}.json"))
    }

    #[test]
    fn accepts_valid_topic_filters() {
        for topic in [
            "#",
            "+",
            "gps/#",
            "gps/+/x",
            "owntracks/meshtastic/a32x",
            "a//b",
        ] {
            assert!(
                validate_topic_filter(topic).is_ok(),
                "{topic} should be valid"
            );
        }
    }

    #[test]
    fn rejects_invalid_topic_filters() {
        for topic in ["", " ", "a/#/b", "a#", "a/+b", "a/b+/c", "a b"] {
            assert!(
                validate_topic_filter(topic).is_err(),
                "{topic} should be invalid"
            );
        }
        assert!(validate_topic_filter(&"a".repeat(MAX_TOPIC_LENGTH + 1)).is_err());
    }

    #[tokio::test]
    async fn saves_and_loads_custom_topics() {
        let path = temporary_path();
        let mut store = TopicStore::load(&path).await;
        assert!(store.add(TopicEntry {
            topic: "owntracks/#".to_string(),
            label: Some("OwnTracks".to_string()),
        }));
        assert!(!store.add(TopicEntry {
            topic: "owntracks/#".to_string(),
            label: None,
        }));
        store.save().await.unwrap();

        let reloaded = TopicStore::load(&path).await;
        assert_eq!(reloaded.len(), 1);
        assert_eq!(reloaded.entries()[0].topic, "owntracks/#");
        assert_eq!(reloaded.entries()[0].label.as_deref(), Some("OwnTracks"));

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn manager_adds_and_removes_topics() {
        let path = temporary_path();
        let store = TopicStore::load(&path).await;
        let manager =
            TopicManager::new(vec!["msh/#".to_string(), "gps/#".to_string()], store, true);

        let state = manager
            .add("owntracks/#", Some("OwnTracks".to_string()))
            .await
            .unwrap();
        assert_eq!(state.effective, vec!["msh/#", "gps/#", "owntracks/#"]);
        assert_eq!(state.custom.len(), 1);
        assert!(state.custom[0].active);

        let state = manager.remove("owntracks/#").await.unwrap();
        assert_eq!(state.effective, vec!["msh/#", "gps/#"]);
        assert!(state.custom.is_empty());

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn manager_marks_topics_covered_by_baseline_inactive() {
        let path = temporary_path();
        let store = TopicStore::load(&path).await;
        let manager = TopicManager::new(vec!["#".to_string()], store, true);

        let state = manager.add("owntracks/#", None).await.unwrap();
        assert_eq!(state.effective, vec!["#"]);
        assert!(!state.custom[0].active);

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn manager_rejects_duplicates_and_baseline_removal() {
        let path = temporary_path();
        let store = TopicStore::load(&path).await;
        let manager = TopicManager::new(vec!["msh/#".to_string()], store, true);

        assert!(matches!(
            manager.add("msh/#", None).await,
            Err(TopicError::Duplicate(_))
        ));
        assert!(matches!(
            manager.remove("msh/#").await,
            Err(TopicError::Invalid(_))
        ));
        assert!(matches!(
            manager.remove("missing/#").await,
            Err(TopicError::NotFound(_))
        ));

        tokio::fs::remove_file(&path).await.ok();
    }
}

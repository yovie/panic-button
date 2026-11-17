use crate::DeviceConfig;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::warn;

pub struct DeviceStore {
    path: PathBuf,
    entries: HashMap<String, DeviceConfig>,
}

impl DeviceStore {
    pub async fn load(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        let entries = match tokio::fs::read(&path).await {
            Ok(bytes) => match serde_json::from_slice::<Vec<DeviceConfig>>(&bytes) {
                Ok(devices) => devices
                    .into_iter()
                    .map(|device| (device.id.clone(), device))
                    .collect(),
                Err(error) => {
                    warn!(%error, path = %path.display(), "ignoring invalid device store file");
                    HashMap::new()
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(error) => {
                warn!(%error, path = %path.display(), "could not read device store");
                HashMap::new()
            }
        };

        Self { path, entries }
    }

    pub fn configs(&self) -> Vec<DeviceConfig> {
        let mut devices: Vec<DeviceConfig> = self.entries.values().cloned().collect();
        devices.sort_by(|a, b| a.id.cmp(&b.id));
        devices
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn upsert(&mut self, config: DeviceConfig) {
        self.entries.insert(config.id.clone(), config);
    }

    pub fn remove(&mut self, id: &str) -> bool {
        self.entries.remove(id).is_some()
    }

    pub async fn save(&self) -> anyhow::Result<()> {
        let mut devices = self.configs();
        devices.shrink_to_fit();
        let json = serde_json::to_string_pretty(&devices)?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DeviceType, Protocol};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temporary_path() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("panic-button-devices-{unique}.json"))
    }

    fn config(id: &str) -> DeviceConfig {
        DeviceConfig {
            id: id.to_string(),
            label: Some("Tim Alpha".to_string()),
            device_type: DeviceType::PanicButton,
            protocol: Protocol::Meshtastic,
            serial: Some("SN-1".to_string()),
            owner: Some("Satgas".to_string()),
            tags: vec!["regu-1".to_string(), "malam".to_string()],
            notes: Some("catatan".to_string()),
            lat: Some(-6.2),
            lon: Some(106.8),
        }
    }

    #[tokio::test]
    async fn saves_and_loads_devices() {
        let path = temporary_path();
        let mut store = DeviceStore::load(&path).await;
        store.upsert(config("device-01"));
        store.upsert(config("device-02"));
        store.save().await.unwrap();

        let reloaded = DeviceStore::load(&path).await;
        assert_eq!(reloaded.len(), 2);
        assert_eq!(reloaded.configs()[0].id, "device-01");
        assert_eq!(reloaded.configs()[1].label.as_deref(), Some("Tim Alpha"));

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn removes_device_from_store() {
        let path = temporary_path();
        let mut store = DeviceStore::load(&path).await;
        store.upsert(config("device-01"));
        assert!(store.remove("device-01"));
        assert!(!store.remove("device-01"));
        store.save().await.unwrap();

        let reloaded = DeviceStore::load(&path).await;
        assert!(reloaded.is_empty());

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn missing_file_yields_empty_store() {
        let path = temporary_path();
        let store = DeviceStore::load(&path).await;
        assert!(store.is_empty());
    }
}

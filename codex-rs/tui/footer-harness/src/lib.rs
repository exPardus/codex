//! Resource-bounded harness compiling the exact footer worker source with
//! small config/event stand-ins. This does not compile the full Codex TUI.

extern crate self as codex_config;
extern crate self as codex_utils_absolute_path;

use std::path::PathBuf;

#[derive(Clone)]
pub struct AbsolutePathBuf(PathBuf);

impl TryFrom<PathBuf> for AbsolutePathBuf {
    type Error = std::io::Error;
    fn try_from(value: PathBuf) -> Result<Self, Self::Error> {
        if value.is_absolute() {
            Ok(Self(value))
        } else {
            Err(std::io::Error::other("not absolute"))
        }
    }
}

pub enum ConfigLayerSource {
    User {
        file: AbsolutePathBuf,
        profile: Option<String>,
    },
    Project {
        dot_codex_folder: AbsolutePathBuf,
    },
    SessionFlags,
    System,
}

pub struct ConfigLayerEntry {
    pub name: ConfigLayerSource,
    pub config: toml::Value,
}

impl ConfigLayerEntry {
    pub fn new(name: ConfigLayerSource, config: toml::Value) -> Self {
        Self { name, config }
    }
}

#[derive(Default)]
pub struct ConfigRequirements;
#[derive(Default)]
pub struct ConfigRequirementsToml;

pub struct ConfigLayerStack {
    layers: Vec<ConfigLayerEntry>,
}
impl ConfigLayerStack {
    pub fn new(
        layers: Vec<ConfigLayerEntry>,
        _: ConfigRequirements,
        _: ConfigRequirementsToml,
    ) -> std::io::Result<Self> {
        Ok(Self { layers })
    }
    pub fn layers_high_to_low(&self) -> impl DoubleEndedIterator<Item = &ConfigLayerEntry> {
        self.layers.iter().rev()
    }
}

pub mod types {
    use serde::Deserialize;
    #[derive(Clone, Deserialize)]
    pub struct StatusLineCommand {
        pub command: String,
        #[serde(default)]
        pub args: Vec<String>,
        #[serde(default = "refresh_default")]
        pub refresh_interval_ms: u64,
        #[serde(default = "timeout_default")]
        pub timeout_ms: u64,
        #[serde(default = "output_default")]
        pub max_output_bytes: usize,
    }
    fn refresh_default() -> u64 {
        5_000
    }
    fn timeout_default() -> u64 {
        500
    }
    fn output_default() -> usize {
        512
    }
}

pub mod app_event {
    #[derive(Debug)]
    pub enum AppEvent {
        StatusLineExternalUpdated { owner: uuid::Uuid, value: String },
    }
}

pub mod app_event_sender {
    use super::app_event::AppEvent;
    #[derive(Clone)]
    pub struct AppEventSender(tokio::sync::mpsc::UnboundedSender<AppEvent>);
    impl AppEventSender {
        pub fn new(tx: tokio::sync::mpsc::UnboundedSender<AppEvent>) -> Self {
            Self(tx)
        }
        pub fn send(&self, value: AppEvent) {
            let _ = self.0.send(value);
        }
    }
}

#[path = "../../src/external_status_line.rs"]
mod external_status_line;

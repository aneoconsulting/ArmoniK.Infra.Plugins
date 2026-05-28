use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// These crates are only used by the binary entry point in `main.rs`; reference
// them here so cargo's `unused-crate-dependencies` lint stays happy on the lib.
use clap as _;
use config as _;
use eyre as _;
use mimalloc as _;
use tonic_web as _;
use tracing_subscriber as _;

pub mod bag;
pub mod cluster;
pub mod service;
pub mod utils;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    #[default]
    Pretty,
    Json,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LbConfig {
    pub clusters: HashMap<String, cluster::ClusterConfig<armonik::client::ClientConfigArgs>>,
    #[serde(default)]
    pub listen_ip: String,
    #[serde(default)]
    pub listen_port: u16,
    #[serde(default)]
    pub refresh_delay: String,
    #[serde(default)]
    pub log_format: LogFormat,
    #[serde(flatten)]
    pub service_options: service::ServiceOptions,
}

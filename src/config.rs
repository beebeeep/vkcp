use anyhow::Result;
use serde::Deserialize;
use std::{collections::HashMap, fs};

#[derive(Deserialize, Clone)]
pub struct PeerConfig {
    pub id: u32,
    pub addr: String,
}

#[derive(Deserialize)]
pub struct Config {
    pub peer_id: u32,
    pub host: Option<String>,
    pub proxy_bind_addr: String,
    pub ctrl_bind_addr: String,

    pub heartbeat_period_ms: Option<u64>,
    pub heartbeat_timeout_ms: Option<u64>,
    pub election_timeout_ms_min: Option<u64>,
    pub election_timeout_ms_max: Option<u64>,
    pub vk_server_timeout_ms: Option<u64>,
    pub vk_server_check_period_ms: Option<u64>,

    pub peers: Vec<PeerConfig>,
    pub servers: Vec<String>,

    pub extra_tags: Option<HashMap<String, String>>,
}

impl Config {
    pub fn new(file: &str) -> Result<Self> {
        let cfg: Config = toml::from_str(&fs::read_to_string(file)?)?;
        Ok(cfg)
    }
}

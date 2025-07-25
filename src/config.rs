use anyhow::Result;
use serde::Deserialize;
use std::fs;

#[derive(Deserialize, Clone)]
pub struct PeerConfig {
    pub id: u32,
    pub addr: String,
}

#[derive(Deserialize)]
pub struct Config {
    pub peer_id: u32,
    pub host: Option<String>,
    pub proxy_port: u16,
    pub ctrl_port: u16,
    pub peers: Vec<PeerConfig>,
    pub servers: Vec<String>,
}

impl Config {
    pub fn new(file: &str) -> Result<Self> {
        let cfg: Config = toml::from_str(&fs::read_to_string(file)?)?;
        Ok(cfg)
    }
}

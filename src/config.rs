// src/config.rs
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub tcp_input: TcpInputConfig,
    pub routes: Vec<RouteConfig>,
    pub monitoring: MonitoringConfig,
    pub database: DatabaseConfig,
    pub query_server: QueryServerConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TcpInputConfig {
    pub listen_address: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RouteConfig {
    pub name: String,
    pub enabled: bool,
    pub pattern: String,
    pub output_type: String,
    pub output_address: String,
    pub output_format: OutputFormat,
    #[serde(default)]
    pub chain_hints: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OutputFormat {
    pub context: String,
    pub command: String,
    pub channel_field: String,
    pub text_field: String,
    pub timestamp_field: String,
    #[serde(default = "default_escape_newlines")]
    pub escape_newlines: bool,
}

fn default_escape_newlines() -> bool {
    false
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MonitoringConfig {
    pub log_messages: bool,
    pub log_errors: bool,
    pub log_connections: bool,
    pub log_forwarding: bool,
    #[serde(default)]
    pub log_forwarding_payload: bool,
    #[serde(default)]
    pub log_incoming_payload: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
    pub pool_size: usize,
    pub cache_size_mb: usize,
    pub cache_ttl_seconds: u64,
    pub compress_messages: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QueryServerConfig {
    pub enabled: bool,
    pub listen_address: String,
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = fs::read_to_string(path)?;
        let config: Config = toml::from_str(&contents)?;
        Ok(config)
    }

    pub fn get_enabled_routes(&self) -> Vec<&RouteConfig> {
        self.routes.iter().filter(|r| r.enabled).collect()
    }
}

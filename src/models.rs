use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IncomingMessage {
    pub chat_id: i64,
    pub chat_name: String,
    pub images: Vec<String>,
    pub mints: Vec<String>,
    pub text: String,
    pub tokens: Vec<String>,
    #[serde(default)]
    pub context: String,  // ADD THIS - preserve original context
    #[serde(default)]
    pub pool: Option<String>,  // ADD THIS - preserve original pool
    #[serde(default)]
    pub ethereum_mints: Vec<String>,
    #[serde(default)]
    pub evm_mints: Vec<String>,
    #[serde(default)]
    pub solana_mints: Vec<String>,
    #[serde(default)]
    pub bsc_mints: Vec<String>,
    #[serde(default)]
    pub addresses: Option<HashMap<String, Vec<String>>>,
    
    // New pool fields
    #[serde(default)]
    pub pools: Vec<String>,
    #[serde(default)]
    pub solana_pools: Vec<String>,
    #[serde(default)]
    pub ethereum_pools: Vec<String>,
    #[serde(default)]
    pub evm_pools: Vec<String>,
    #[serde(default)]
    pub bsc_pools: Vec<String>,
    #[serde(default)]
    pub pool_addresses: Option<HashMap<String, Vec<String>>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OutputMessage {
    pub context: String,  // Will preserve original empty string from incoming
    pub command: String,
    pub args: OutputArgs,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,  // Pool address field
}

#[derive(Debug, Clone, Serialize)]
pub struct OutputArgs {
    pub channel: String,
    pub ts: i64,
    pub text: String,
}
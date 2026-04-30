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
    
    // Additional fields that might be present
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
}

#[derive(Debug, Clone, Serialize)]
pub struct OutputMessage {
    pub context: String,  // This will contain the mint address
    pub command: String,
    pub args: OutputArgs,
}

#[derive(Debug, Clone, Serialize)]
pub struct OutputArgs {
    pub channel: String,
    pub ts: i64,
    pub text: String,
}
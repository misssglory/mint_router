use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IncomingMessage {
    pub chat_id: i64,
    pub chat_name: String,
    pub images: Vec<String>,
    pub mints: Vec<String>,
    pub text: String,
    pub tokens: Vec<String>,
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
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMessage {
    pub chat_id: i64,
    pub chat_name: String,
    pub text: String,
    pub timestamp_us: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub chat_id: i64,
    pub chat_name: String,
    pub text: String,
    pub timestamp_us: i64,
}

pub type ChatEntry = (i64, String);
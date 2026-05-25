use serde_json::{json, Value};

pub fn error(message: impl Into<String>) -> String {
    json!({ "error": message.into() }).to_string()
}

pub fn with_commands(message: impl Into<String>) -> String {
    json!({
        "error": message.into(),
        "commands": crate::query_server::protocol::help_commands()
    })
    .to_string()
}

pub fn json_string(value: Value) -> String {
    value.to_string()
}
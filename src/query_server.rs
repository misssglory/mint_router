use crate::config::QueryServerConfig;
use crate::database::DatabaseManager;
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use tokio::runtime::Runtime;

mod protocol {
    pub const CMD_GET: &str = "GET";
    pub const CMD_HELP: &str = "HELP";
    pub const CMD_CLEANUP: &str = "CLEANUP";

    pub const GET_MINT: &str = "mint";
    pub const GET_CHAT_ID: &str = "chat:id";
    pub const GET_CHAT_NAME: &str = "chat:name";
    pub const GET_CHAT_ID_FOR_NAME: &str = "chat:id-for-name";
    pub const GET_STATS: &str = "stats";
    pub const GET_CHANNELS_WITH_MINTS: &str = "channels-with-mints";
    pub const GET_CHANNEL_MINTS: &str = "channel-mints";
    pub const GET_CONTEXTS_BY_CHANNEL: &str = "contexts-by-channel";

    pub fn help_commands() -> Vec<&'static str> {
        vec![
            "GET mint <address> [--no-cache] - Get all messages for a mint address",
            "GET mint <address> --unique-chats - Get unique chats where mint was mentioned",
            "GET chat:id <id> - Get chat name by ID",
            "GET chat:name <name> - Get chat ID by name",
            "GET chat:id-for-name <name> - Get chat ID only",
            "GET channels-with-mints - Get all channels that have at least one stored context",
            "GET channel-mints chatid <id> - Get all contexts for a channel by ID",
            "GET channel-mints chatname <name> - Get all contexts for a channel by exact name",
            "GET contexts-by-channel <substring> - Get contexts where matched channel names contain substring (case-insensitive)",
            "GET stats - Get database statistics",
            "CLEANUP <max_age_seconds> - Delete messages older than specified seconds",
            "HELP - Show this help",
        ]
    }
}

mod parsing {
    pub fn split_parts(input: &str) -> Vec<&str> {
        input.split_whitespace().collect()
    }

    pub fn join_tail(parts: &[&str], start: usize) -> String {
        parts.get(start..).unwrap_or(&[]).join(" ")
    }

    pub fn normalize_quoted(value: &str) -> String {
        value
            .trim()
            .trim_start_matches('"')
            .trim_end_matches('"')
            .to_string()
    }
}

mod response {
    use super::*;

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
}

pub struct QueryServer {
    config: QueryServerConfig,
    db: Arc<DatabaseManager>,
}

impl QueryServer {
    pub fn new(config: QueryServerConfig, db: Arc<DatabaseManager>) -> Self {
        Self { config, db }
    }

    pub fn start(&self) -> std::io::Result<()> {
        if !self.config.enabled {
            println!("Query server disabled");
            return Ok(());
        }

        let listener = self.bind_listener()?;
        println!("Query server listening on {}", self.config.listen_address);

        for stream in listener.incoming() {
            self.handle_incoming_stream(stream);
        }

        Ok(())
    }

    fn bind_listener(&self) -> std::io::Result<TcpListener> {
        TcpListener::bind(&self.config.listen_address)
    }

    fn handle_incoming_stream(&self, stream: std::io::Result<TcpStream>) {
        match stream {
            Ok(stream) => {
                let db = Arc::clone(&self.db);
                thread::spawn(move || {
                    let rt = Runtime::new().unwrap();
                    rt.block_on(async move {
                        handle_query_connection(stream, db).await;
                    });
                });
            }
            Err(err) => {
                eprintln!("Query server accept error: {}", err);
            }
        }
    }
}

async fn handle_query_connection(mut stream: TcpStream, db: Arc<DatabaseManager>) {
    let addr = stream.peer_addr().ok();
    println!("[QUERY] Client connected: {:?}", addr);

    let reader = match stream.try_clone() {
        Ok(cloned) => BufReader::new(cloned),
        Err(err) => {
            eprintln!("[QUERY] Failed to clone stream: {}", err);
            return;
        }
    };

    for line in reader.lines() {
        match line {
            Ok(query_str) => {
                println!("[QUERY] Received: {}", query_str);
                let response = process_query(&query_str, db.as_ref()).await;

                if let Err(err) = writeln!(stream, "{}", response) {
                    eprintln!("[QUERY] Failed to send response: {}", err);
                    break;
                }
            }
            Err(err) => {
                eprintln!("[QUERY] Read error: {}", err);
                break;
            }
        }
    }

    println!("[QUERY] Client disconnected: {:?}", addr);
}

async fn process_query(query_str: &str, db: &DatabaseManager) -> String {
    let parts = parsing::split_parts(query_str);

    if parts.is_empty() {
        return response::with_commands("Empty query");
    }

    match parts[0] {
        protocol::CMD_GET => handle_get(&parts, db).await,
        protocol::CMD_CLEANUP => handle_cleanup(&parts, db).await,
        protocol::CMD_HELP => handle_help(),
        other => response::error(format!("Unknown command: {}", other)),
    }
}

async fn handle_get(parts: &[&str], db: &DatabaseManager) -> String {
    if parts.len() < 2 {
        return response::error("Invalid GET command. Usage: GET <type> [value]");
    }

    match parts[1] {
        protocol::GET_MINT => handle_get_mint(parts, db).await,
        protocol::GET_CHAT_ID => handle_get_chat_id(parts, db).await,
        protocol::GET_CHAT_NAME => handle_get_chat_name(parts, db).await,
        protocol::GET_CHAT_ID_FOR_NAME => handle_get_chat_id_for_name(parts, db).await,
        protocol::GET_STATS => handle_get_stats(db).await,
        protocol::GET_CHANNELS_WITH_MINTS => handle_get_channels_with_mints(db).await,
        protocol::GET_CHANNEL_MINTS => handle_get_channel_mints(parts, db).await,
        protocol::GET_CONTEXTS_BY_CHANNEL => handle_get_contexts_by_channel(parts, db).await,
        other => response::error(format!("Unknown GET type: {}", other)),
    }
}

async fn handle_get_mint(parts: &[&str], db: &DatabaseManager) -> String {
    if parts.len() < 3 {
        return response::error("Usage: GET mint <address> [--no-cache|--unique-chats]");
    }

    let mint = parts[2];
    let use_cache = !parts.contains(&"--no-cache");
    let unique_chats = parts.contains(&"--unique-chats");

    if unique_chats {
        return handle_get_mint_unique_chats(mint, db).await;
    }

    match db.get_messages_by_mint(mint, use_cache).await {
        Ok(messages) => {
            let messages_json: Vec<Value> = messages
                .into_iter()
                .map(|msg| {
                    json!({
                        "chat_id": msg.chat_id,
                        "chat_name": msg.chat_name,
                        "text": msg.text,
                        "timestamp_us": msg.timestamp_us
                    })
                })
                .collect();

            response::json_string(json!({
                "mint": mint,
                "messages": messages_json,
                "count": messages_json.len(),
                "cached": use_cache
            }))
        }
        Err(err) => response::error(format!("Failed to query messages: {}", err)),
    }
}

async fn handle_get_mint_unique_chats(mint: &str, db: &DatabaseManager) -> String {
    match db.get_unique_chats_for_mint(mint).await {
        Ok(chats) => {
            let chats_json: Vec<Value> = chats
                .into_iter()
                .map(|(chat_id, chat_name)| {
                    json!({
                        "chat_id": chat_id,
                        "chat_name": chat_name
                    })
                })
                .collect();

            response::json_string(json!({
                "mint": mint,
                "unique_chats": chats_json,
                "count": chats_json.len()
            }))
        }
        Err(err) => response::error(format!("Failed to query chats: {}", err)),
    }
}

async fn handle_get_chat_id(parts: &[&str], db: &DatabaseManager) -> String {
    if parts.len() < 3 {
        return response::error("Usage: GET chat:id <id>");
    }

    let chat_id = match parts[2].parse::<i64>() {
        Ok(id) => id,
        Err(_) => return response::error("Invalid chat ID"),
    };

    match db.get_chat_name(chat_id).await {
        Some(chat_name) => response::json_string(json!({
            "chat_id": chat_id,
            "chat_name": chat_name
        })),
        None => response::error("Chat not found"),
    }
}

async fn handle_get_chat_name(parts: &[&str], db: &DatabaseManager) -> String {
    if parts.len() < 3 {
        return response::error("Usage: GET chat:name <name>");
    }

    let raw_name = parsing::join_tail(parts, 2);
    let chat_name = parsing::normalize_quoted(&raw_name);

    if chat_name.is_empty() {
        return response::error("Chat name cannot be empty");
    }

    match db.get_chat_id(&chat_name).await {
        Some(chat_id) => response::json_string(json!({
            "chat_id": chat_id,
            "chat_name": chat_name
        })),
        None => response::error("Chat not found"),
    }
}

async fn handle_get_chat_id_for_name(parts: &[&str], db: &DatabaseManager) -> String {
    if parts.len() < 3 {
        return response::error("Usage: GET chat:id-for-name <name>");
    }

    let raw_name = parsing::join_tail(parts, 2);
    let chat_name = parsing::normalize_quoted(&raw_name);

    if chat_name.is_empty() {
        return response::error("Chat name cannot be empty");
    }

    match db.get_chat_id(&chat_name).await {
        Some(chat_id) => response::json_string(json!({ "chat_id": chat_id })),
        None => response::error("Chat not found"),
    }
}

async fn handle_get_stats(db: &DatabaseManager) -> String {
    match db.get_stats().await {
        Ok(stats) => stats.to_string(),
        Err(err) => response::error(format!("Failed to get stats: {}", err)),
    }
}

async fn handle_get_channels_with_mints(db: &DatabaseManager) -> String {
    match db.get_chats_with_any_mint().await {
        Ok(chats) => {
            let channels: Vec<Value> = chats
                .into_iter()
                .map(|(chat_id, chat_name)| {
                    json!({
                        "chat_id": chat_id,
                        "chat_name": chat_name
                    })
                })
                .collect();

            response::json_string(json!({
                "channels": channels,
                "count": channels.len()
            }))
        }
        Err(err) => response::error(format!("Failed to query channels with mints: {}", err)),
    }
}

async fn handle_get_channel_mints(parts: &[&str], db: &DatabaseManager) -> String {
    if parts.len() < 4 {
        return response::error("Usage: GET channel-mints chatid|chatname <value>");
    }

    match parts[2] {
        "chatid" => handle_get_channel_mints_by_id(parts, db).await,
        "chatname" => handle_get_channel_mints_by_name(parts, db).await,
        _ => response::error("Invalid mode for channel-mints. Use chatid or chatname"),
    }
}

async fn handle_get_channel_mints_by_id(parts: &[&str], db: &DatabaseManager) -> String {
    let chat_id = match parts[3].parse::<i64>() {
        Ok(id) => id,
        Err(_) => return response::error("Invalid chat ID"),
    };

    match db.get_mints_for_chat_id(chat_id).await {
        Ok(mints) => response::json_string(json!({
            "chat_id": chat_id,
            "mints": mints,
            "count": mints.len()
        })),
        Err(err) => response::error(format!("Failed to query mints for chat: {}", err)),
    }
}

async fn handle_get_channel_mints_by_name(parts: &[&str], db: &DatabaseManager) -> String {
    let raw_name = parsing::join_tail(parts, 3);
    let chat_name = parsing::normalize_quoted(&raw_name);

    if chat_name.is_empty() {
        return response::error("Chat name cannot be empty");
    }

    match db.get_mints_for_chat_name(chat_name.clone()).await {
        Ok(mints) => response::json_string(json!({
            "chat_name": chat_name,
            "mints": mints,
            "count": mints.len()
        })),
        Err(err) => response::error(format!("Failed to query mints for chat: {}", err)),
    }
}

async fn handle_get_contexts_by_channel(parts: &[&str], db: &DatabaseManager) -> String {
    if parts.len() < 3 {
        return response::error("Usage: GET contexts-by-channel <channel_name_substring>");
    }

    let raw = parsing::join_tail(parts, 2);
    let substring = parsing::normalize_quoted(&raw);

    if substring.is_empty() {
        return response::error("Channel name substring cannot be empty");
    }

    let matched_channels = match db.get_channels_by_name_substring(&substring).await {
        Ok(channels) => channels,
        Err(err) => return response::error(format!("Failed to query matched channels: {}", err)),
    };

    let contexts = match db.get_contexts_by_channel_substring(&substring).await {
        Ok(contexts) => contexts,
        Err(err) => return response::error(format!("Failed to query contexts by channel substring: {}", err)),
    };

    let channels_json: Vec<Value> = matched_channels
        .into_iter()
        .map(|(chat_id, chat_name)| {
            json!({
                "chat_id": chat_id,
                "chat_name": chat_name
            })
        })
        .collect();

    response::json_string(json!({
        "query": substring,
        "matched_channels": channels_json,
        "contexts": contexts,
        "count": contexts.len()
    }))
}

async fn handle_cleanup(parts: &[&str], db: &DatabaseManager) -> String {
    if parts.len() < 2 {
        return response::error("Usage: CLEANUP <max_age_seconds>");
    }

    let max_age_seconds = match parts[1].parse::<u64>() {
        Ok(v) => v,
        Err(_) => return response::error("Invalid max_age_seconds"),
    };

    match db.cleanup_old_entries(max_age_seconds).await {
        Ok(deleted_messages) => response::json_string(json!({
            "status": "success",
            "deleted_messages": deleted_messages,
            "max_age_seconds": max_age_seconds
        })),
        Err(err) => response::error(format!("Cleanup failed: {}", err)),
    }
}

fn handle_help() -> String {
    response::json_string(json!({
        "commands": protocol::help_commands()
    }))
}
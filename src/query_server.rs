// src/query_server.rs
use crate::config::QueryServerConfig;
use crate::database::DatabaseManager;
use serde_json::json;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use tokio::runtime::Runtime;

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

        let listener = TcpListener::bind(&self.config.listen_address)?;
        println!("Query server listening on {}", self.config.listen_address);

        let db = Arc::clone(&self.db);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let db_clone = Arc::clone(&db);
                    thread::spawn(move || {
                        let rt = Runtime::new().unwrap();
                        rt.block_on(async {
                            handle_query_connection(stream, db_clone).await;
                        });
                    });
                }
                Err(err) => {
                    eprintln!("Query server accept error: {}", err);
                }
            }
        }

        Ok(())
    }
}

async fn handle_query_connection(mut stream: TcpStream, db: Arc<DatabaseManager>) {
    let addr = stream.peer_addr().ok();
    println!("[QUERY] Client connected: {:?}", addr);

    let reader = BufReader::new(stream.try_clone().unwrap());

    for line in reader.lines() {
        match line {
            Ok(query_str) => {
                println!("[QUERY] Received: {}", query_str);

                let response = process_query(&query_str, &db).await;

                if let Err(e) = writeln!(stream, "{}", response) {
                    eprintln!("[QUERY] Failed to send response: {}", e);
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
    let parts: Vec<&str> = query_str.split_whitespace().collect();

    if parts.is_empty() {
        return json!({
            "error": "Empty query",
            "commands": [
                "GET mint <address> [--no-cache]",
                "GET mint <address> --unique-chats",
                "GET chat:id <id>",
                "GET chat:name <name>",
                "GET chat:id-for-name <name>",
                "GET stats",
                "CLEANUP <max_age_seconds>",
                "HELP"
            ]
        })
        .to_string();
    }

    match parts[0] {
        "GET" => {
            if parts.len() < 2 {
                return json!({"error": "Invalid GET command. Usage: GET <type> [value]"})
                    .to_string();
            }

            match parts[1] {
                "channels-with-mints" => match db.get_chats_with_any_mint().await {
                    Ok(chats) => {
                        let chats_json: Vec<serde_json::Value> = chats
                            .into_iter()
                            .map(|(id, name)| json!({ "chat_id": id, "chat_name": name }))
                            .collect();

                        json!({
                            "channels": chats_json,
                            "count": chats_json.len()
                        })
                        .to_string()
                    }
                    Err(err) => json!({
                        "error": format!("Failed to query channels with mints: {}", err)
                    })
                    .to_string(),
                },
                "channel-mints" => {
                    if parts.len() < 4 {
                        return json!({"error": "Usage: GET channel-mints chatid|chatname <value>"}).to_string();
                    }

                    let mode = parts[2];
                    let value = parts[3];

                    match mode {
                        "chatid" => {
                            if let Ok(chat_id) = value.parse::<i64>() {
                                match db.get_mints_for_chat_id(chat_id).await {
                                    Ok(mints) => json!({
                                        "chatid": chat_id,
                                        "mints": mints,
                                        "count": mints.len()
                                    })
                                    .to_string(),
                                    Err(err) => json!({
                                        "error": format!("Failed to query mints for chat: {}", err)
                                    })
                                    .to_string(),
                                }
                            } else {
                                json!({"error": "Invalid chat ID"}).to_string()
                            }
                        }
                        "chatname" => match db.get_mints_for_chat_name(value.to_string()).await {
                            Ok(mints) => json!({
                                "chatname": value,
                                "mints": mints,
                                "count": mints.len()
                            })
                            .to_string(),
                            Err(err) => json!({
                                "error": format!("Failed to query mints for chat: {}", err)
                            })
                            .to_string(),
                        },
                        _ => json!({
                            "error": "Invalid mode for channel-mints. Use chatid or chatname"
                        })
                        .to_string(),
                    }
                }

                "mint" => {
                    if parts.len() < 3 {
                        return json!({"error": "Usage: GET mint <address> [--no-cache|--unique-chats]"}).to_string();
                    }

                    let mint = parts[2];
                    let use_cache = !parts.contains(&"--no-cache");
                    let unique_chats = parts.contains(&"--unique-chats");

                    if unique_chats {
                        match db.get_unique_chats_for_mint(mint).await {
                            Ok(chats) => {
                                let chats_json: Vec<serde_json::Value> = chats
                                    .into_iter()
                                    .map(|(id, name)| {
                                        json!({
                                            "chat_id": id,
                                            "chat_name": name
                                        })
                                    })
                                    .collect();

                                json!({
                                    "mint": mint,
                                    "unique_chats": chats_json,
                                    "count": chats_json.len()
                                })
                                .to_string()
                            }
                            Err(err) => json!({
                                "error": format!("Failed to query chats: {}", err)
                            })
                            .to_string(),
                        }
                    } else {
                        match db.get_messages_by_mint(mint, use_cache).await {
                            Ok(messages) => {
                                let messages_json: Vec<serde_json::Value> = messages
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

                                json!({
                                    "mint": mint,
                                    "messages": messages_json,
                                    "count": messages_json.len(),
                                    "cached": use_cache
                                })
                                .to_string()
                            }
                            Err(err) => json!({
                                "error": format!("Failed to query messages: {}", err)
                            })
                            .to_string(),
                        }
                    }
                }

                "chat:id" => {
                    if parts.len() < 3 {
                        return json!({"error": "Usage: GET chat:id <id>"}).to_string();
                    }

                    if let Ok(chat_id) = parts[2].parse::<i64>() {
                        match db.get_chat_name(chat_id).await {
                            Some(name) => json!({
                                "chat_id": chat_id,
                                "chat_name": name
                            })
                            .to_string(),
                            None => json!({"error": "Chat not found"}).to_string(),
                        }
                    } else {
                        json!({"error": "Invalid chat ID"}).to_string()
                    }
                }

                "chat:name" => {
                    if parts.len() < 3 {
                        return json!({"error": "Usage: GET chat:name <name>"}).to_string();
                    }

                    let chat_name = parts[2];
                    match db.get_chat_id(chat_name).await {
                        Some(id) => json!({
                            "chat_id": id,
                            "chat_name": chat_name
                        })
                        .to_string(),
                        None => json!({"error": "Chat not found"}).to_string(),
                    }
                }

                "chat:id-for-name" => {
                    if parts.len() < 3 {
                        return json!({"error": "Usage: GET chat:id-for-name <name>"}).to_string();
                    }

                    let chat_name = parts[2];
                    match db.get_chat_id(chat_name).await {
                        Some(id) => json!({ "chat_id": id }).to_string(),
                        None => json!({"error": "Chat not found"}).to_string(),
                    }
                }

                "stats" => match db.get_stats().await {
                    Ok(stats) => stats.to_string(),
                    Err(err) => json!({
                        "error": format!("Failed to get stats: {}", err)
                    })
                    .to_string(),
                },

                _ => json!({"error": format!("Unknown GET type: {}", parts[1])}).to_string(),
            }
        }
        "CLEANUP" => {
            if parts.len() < 2 {
                return json!({"error": "Usage: CLEANUP <max_age_seconds>"}).to_string();
            }

            if let Ok(max_age) = parts[1].parse::<u64>() {
                match db.cleanup_old_entries(max_age).await {
                    Ok(deleted) => json!({
                        "status": "success",
                        "deleted_messages": deleted,
                        "max_age_seconds": max_age
                    })
                    .to_string(),
                    Err(err) => json!({"error": format!("Cleanup failed: {}", err)}).to_string(),
                }
            } else {
                json!({"error": "Invalid max_age_seconds"}).to_string()
            }
        }
        "HELP" => json!({
            "commands": [
                "GET mint <address> [--no-cache] - Get all messages for a mint address",
                "GET mint <address> --unique-chats - Get unique chats where mint was mentioned",
                "GET chat:id <id> - Get chat name by ID",
                "GET chat:name <name> - Get chat ID by name",
                "GET chat:id-for-name <name> - Get chat ID only",
                "GET stats - Get database statistics",
                "CLEANUP <max_age_seconds> - Delete messages older than specified seconds",
                "HELP - Show this help"
            ]
        })
        .to_string(),
        _ => json!({"error": format!("Unknown command: {}", parts[0])}).to_string(),
    }
}

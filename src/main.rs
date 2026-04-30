mod clients;
mod config;
mod models;
mod router;
mod database;
mod query_server;

use config::Config;
use router::Router;
use database::DatabaseManager;
use query_server::QueryServer;
use serde_json;
use std::io::{BufRead, BufReader};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use tokio::runtime::Runtime;

fn handle_client(stream: TcpStream, router: Arc<Router>, config: Arc<Config>, rt: Arc<Runtime>) {
    let addr = stream.peer_addr().ok();
    
    if config.monitoring.log_connections {
        println!("[INPUT] Client connected: {:?}", addr);
    }
    
    let reader = BufReader::new(stream);
    
    for line in reader.lines() {
        match line {
            Ok(msg_str) => {
                if config.monitoring.log_messages {
                    println!("[INPUT] Received: {}", msg_str);
                }
                
                match serde_json::from_str::<models::IncomingMessage>(&msg_str) {
                    Ok(msg) => {
                        if config.monitoring.log_messages {
                            println!("[INPUT] Processing message from chat: {}", msg.chat_name);
                            println!("[INPUT] Text length: {} chars", msg.text.len());
                            println!("[INPUT] Mints array: {:?}", msg.mints);
                        }
                        
                        // Clone the router and spawn the async task using the provided runtime
                        let router_clone = Arc::clone(&router);
                        let rt_clone = rt.clone();
                        
                        // Use the runtime to spawn the async task
                        rt_clone.spawn(async move {
                            router_clone.process_message(&msg).await;
                        });
                    }
                    Err(err) => {
                        if config.monitoring.log_errors {
                            eprintln!("[INPUT] Failed to parse message: {}", err);
                            eprintln!("[INPUT] Raw message: {}", msg_str);
                        }
                    }
                }
            }
            Err(err) => {
                if config.monitoring.log_errors {
                    eprintln!("[INPUT] Read error: {}", err);
                }
                break;
            }
        }
    }
    
    if config.monitoring.log_connections {
        println!("[INPUT] Client disconnected: {:?}", addr);
    }
}

fn main() -> std::io::Result<()> {
    // Create tokio runtime
    let rt = Arc::new(Runtime::new().unwrap());
    
    rt.block_on(async {
        // Load configuration
        let config = match Config::from_file("config.toml") {
            Ok(c) => Arc::new(c),
            Err(e) => {
                eprintln!("Failed to load config: {}", e);
                std::process::exit(1);
            }
        };
        
        // Initialize database
        let db = match DatabaseManager::new(config.database.clone()).await {
            Ok(d) => Arc::new(d),
            Err(e) => {
                eprintln!("Failed to initialize database: {}", e);
                std::process::exit(1);
            }
        };
        
        // Initialize router
        let router = match Router::new(Arc::clone(&config), Arc::clone(&db)).await {
            Ok(r) => Arc::new(r),
            Err(e) => {
                eprintln!("Failed to initialize router: {}", e);
                std::process::exit(1);
            }
        };
        
        // Start query server
        let query_server = QueryServer::new(config.query_server.clone(), Arc::clone(&db));
        let query_server_handle = thread::spawn(move || {
            if let Err(e) = query_server.start() {
                eprintln!("Query server error: {}", e);
            }
        });
        
        println!("TCP Router starting...");
        println!("================================");
        println!("Input listener: {}", config.tcp_input.listen_address);
        println!("PostgreSQL Database: {}:{}/{}", 
            config.database.host, 
            config.database.port, 
            config.database.dbname);
        println!("Cache size: {} MB", config.database.cache_size_mb);
        println!("Cache TTL: {} seconds", config.database.cache_ttl_seconds);
        println!("Compression: {}", config.database.compress_messages);
        println!("\nEnabled routes:");
        for route in config.get_enabled_routes() {
            println!("  • {} -> {} ({})", 
                     route.name, 
                     route.output_address,
                     route.output_type);
            println!("    Pattern: {}", route.pattern);
            println!("    Escape newlines: {}", route.output_format.escape_newlines);
        }
        println!("================================");
        
        let listener = TcpListener::bind(&config.tcp_input.listen_address)?;
        
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let router_clone = Arc::clone(&router);
                    let config_clone = Arc::clone(&config);
                    let rt_clone = Arc::clone(&rt);
                    
                    std::thread::spawn(move || {
                        handle_client(stream, router_clone, config_clone, rt_clone);
                    });
                }
                Err(err) => {
                    if config.monitoring.log_errors {
                        eprintln!("[INPUT] Accept error: {}", err);
                    }
                }
            }
        }
        
        // Wait for query server to finish (should not happen)
        let _ = query_server_handle.join();
        
        Ok(())
    })
}
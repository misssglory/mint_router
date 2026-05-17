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
use tracing::{info, error, debug, warn};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

fn handle_client(stream: TcpStream, router: Arc<Router>, config: Arc<Config>, rt: Arc<Runtime>) {
    let addr = stream.peer_addr().ok();
    
    if config.monitoring.log_connections {
        info!("[INPUT] Client connected: {:?}", addr);
    }
    
    let reader = BufReader::new(stream);
    
    for line in reader.lines() {
        match line {
            Ok(msg_str) => {
                if config.monitoring.log_messages {
                    debug!("[INPUT] Received: {}", msg_str);
                }
                
                match serde_json::from_str::<models::IncomingMessage>(&msg_str) {
                    Ok(msg) => {
                        if config.monitoring.log_messages {
                            debug!("[INPUT] Processing message from chat: {}", msg.chat_name);
                            debug!("[INPUT] Original context in message: '{}'", msg.context);
                            debug!("[INPUT] Original pool field: {:?}", msg.pool);
                            debug!("[INPUT] Text length: {} chars", msg.text.len());
                            debug!("[INPUT] Mints array: {:?}", msg.mints);
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
                            error!("[INPUT] Failed to parse message: {}", err);
                            error!("[INPUT] Raw message: {}", msg_str);
                        }
                    }
                }
            }
            Err(err) => {
                if config.monitoring.log_errors {
                    error!("[INPUT] Read error: {}", err);
                }
                break;
            }
        }
    }
    
    if config.monitoring.log_connections {
        info!("[INPUT] Client disconnected: {:?}", addr);
    }
}

fn main() -> std::io::Result<()> {
    // Initialize tracing subscriber with JSON formatting for production
    // Or use simple formatting for development
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .init();
    
    info!("Starting TCP Router with tracing enabled");
    
    // Create tokio runtime
    let rt = Arc::new(Runtime::new().unwrap());
    
    rt.block_on(async {
        // Load configuration
        let config = match Config::from_file("config.toml") {
            Ok(c) => Arc::new(c),
            Err(e) => {
                error!("Failed to load config: {}", e);
                std::process::exit(1);
            }
        };
        
        // Initialize database
        let db = match DatabaseManager::new(config.database.clone()).await {
            Ok(d) => Arc::new(d),
            Err(e) => {
                error!("Failed to initialize database: {}", e);
                std::process::exit(1);
            }
        };
        
        // Initialize router
        let router = match Router::new(Arc::clone(&config), Arc::clone(&db)).await {
            Ok(r) => Arc::new(r),
            Err(e) => {
                error!("Failed to initialize router: {}", e);
                std::process::exit(1);
            }
        };
        
        // Start query server
        let query_server = QueryServer::new(config.query_server.clone(), Arc::clone(&db));
        let query_server_handle = thread::spawn(move || {
            if let Err(e) = query_server.start() {
                error!("Query server error: {}", e);
            }
        });
        
        info!("TCP Router starting...");
        info!("================================");
        info!("Input listener: {}", config.tcp_input.listen_address);
        info!("PostgreSQL Database: {}:{}/{}", 
            config.database.host, 
            config.database.port, 
            config.database.dbname);
        info!("Cache size: {} MB", config.database.cache_size_mb);
        info!("Cache TTL: {} seconds", config.database.cache_ttl_seconds);
        info!("Compression: {}", config.database.compress_messages);
        info!("Enabled routes:");
        for route in config.get_enabled_routes() {
            info!("  • {} -> {} ({})", 
                     route.name, 
                     route.output_address,
                     route.output_type);
            info!("    Pattern: {}", route.pattern);
            info!("    Escape newlines: {}", route.output_format.escape_newlines);
        }
        info!("================================");
        
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
                        error!("[INPUT] Accept error: {}", err);
                    }
                }
            }
        }
        
        // Wait for query server to finish (should not happen)
        let _ = query_server_handle.join();
        
        Ok(())
    })
}
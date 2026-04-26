mod clients;
mod config;
mod models;
mod router;

use config::Config;
use router::Router;
use serde_json;
use std::io::{BufRead, BufReader};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

fn handle_client(stream: TcpStream, router: Arc<Router>, config: Arc<Config>) {
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
                        // Debug: show what we're processing
                        if config.monitoring.log_messages {
                            println!("[INPUT] Processing message from chat: {}", msg.chat_name);
                            println!("[INPUT] Text length: {} chars", msg.text.len());
                            println!("[INPUT] Mints array: {:?}", msg.mints);
                        }
                        router.process_message(&msg);
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
    // Load configuration
    let config = match Config::from_file("config.toml") {
        Ok(c) => Arc::new(c),
        Err(e) => {
            eprintln!("Failed to load config: {}", e);
            std::process::exit(1);
        }
    };
    
    // Initialize router
    let router = match Router::new(Arc::clone(&config)) {
        Ok(r) => Arc::new(r),
        Err(e) => {
            eprintln!("Failed to initialize router: {}", e);
            std::process::exit(1);
        }
    };
    
    println!("TCP Router starting...");
    println!("================================");
    println!("Input listener: {}", config.tcp_input.listen_address);
    println!("\nEnabled routes:");
    for route in config.get_enabled_routes() {
        println!("  • {} -> {} ({})", 
                 route.name, 
                 route.output_address,
                 route.output_type);
        println!("    Pattern: {}", route.pattern);
    }
    println!("================================");
    
    let listener = TcpListener::bind(&config.tcp_input.listen_address)?;
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let router_clone = Arc::clone(&router);
                let config_clone = Arc::clone(&config);
                
                thread::spawn(move || {
                    handle_client(stream, router_clone, config_clone);
                });
            }
            Err(err) => {
                if config.monitoring.log_errors {
                    eprintln!("[INPUT] Accept error: {}", err);
                }
            }
        }
    }
    
    Ok(())
}
use crate::config::NetworkConfig;
use crate::handlers::NetworkHandler;
use crate::models::IncomingMessage;
use crate::Config;
use serde_json;
use std::io::{BufRead, BufReader};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

pub fn start_server(network_config: NetworkConfig, global_config: Arc<Config>) -> std::io::Result<()> {
    let network_name = network_config.name.clone();
    let listen_address = network_config.listen_address.clone();
    
    // Create handler for this network
    let handler = match NetworkHandler::new(network_config) {
        Ok(h) => Arc::new(h),
        Err(e) => {
            eprintln!("Failed to create handler for {}: {}", network_name, e);
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid regex pattern: {}", e),
            ));
        }
    };
    
    let listener = TcpListener::bind(&listen_address)?;
    
    if global_config.monitoring.log_connections {
        println!("✓ {} listening on tcp://{}", network_name.to_uppercase(), listen_address);
    }
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let handler_clone = Arc::clone(&handler);
                let global_config_clone = Arc::clone(&global_config);
                let network_name_clone = network_name.clone();
                
                thread::spawn(move || {
                    handle_connection(stream, handler_clone, global_config_clone, network_name_clone);
                });
            }
            Err(err) => {
                if global_config.monitoring.log_errors {
                    eprintln!("{} accept error: {}", network_name, err);
                }
            }
        }
    }
    
    Ok(())
}

fn handle_connection(
    stream: TcpStream,
    handler: Arc<NetworkHandler>,
    global_config: Arc<Config>,
    network_name: String,
) {
    let addr = stream.peer_addr().ok();
    
    if global_config.monitoring.log_connections {
        println!("[{}] Client connected: {:?}", network_name, addr);
    }
    
    let reader = BufReader::new(stream);
    
    for line in reader.lines() {
        match line {
            Ok(msg_str) => {
                if global_config.monitoring.log_messages {
                    println!("[{}] MESSAGE: {}", network_name, msg_str);
                }
                
                // Parse the incoming JSON message
                match serde_json::from_str::<IncomingMessage>(&msg_str) {
                    Ok(msg) => {
                        let output_messages = handler.process_message(&msg);
                        
                        for output_msg in output_messages {
                            send_output(&output_msg, &global_config);
                        }
                    }
                    Err(err) => {
                        if global_config.monitoring.log_errors {
                            eprintln!("[{}] Failed to parse message: {}", network_name, err);
                        }
                    }
                }
            }
            Err(err) => {
                if global_config.monitoring.log_errors {
                    eprintln!("[{}] Read error: {}", network_name, err);
                }
                break;
            }
        }
    }
    
    if global_config.monitoring.log_connections {
        println!("[{}] Client disconnected: {:?}", network_name, addr);
    }
}

fn send_output(message: &crate::models::OutputMessage, config: &Config) {
    match config.output.output_type.as_str() {
        "stdout" => {
            let json_output = serde_json::to_string(message).unwrap();
            println!("{}", json_output);
        }
        "tcp" => {
            // TODO: Implement TCP output for future use
            if config.monitoring.log_errors {
                eprintln!("TCP output not yet implemented");
            }
        }
        "both" => {
            let json_output = serde_json::to_string(message).unwrap();
            println!("{}", json_output);
            // TODO: Also send via TCP
        }
        _ => {}
    }
}
use crate::config::RouteConfig;
use crate::models::OutputMessage;
use std::io::Write;
use std::net::TcpStream;
use std::time::Duration;

pub mod solana_client;
pub mod ethereum_client;
pub mod bnb_client;

pub trait OutputClient: Send + Sync {
    fn send_message(&self, message: &OutputMessage, route: &RouteConfig) -> Result<(), String>;
}

pub struct TcpOutputClient;

impl TcpOutputClient {
    pub fn new() -> Self {
        Self
    }
    
    fn send_tcp(&self, address: &str, message: &OutputMessage) -> Result<(), String> {
        let json_message = serde_json::to_string(message)
            .map_err(|e| format!("Failed to serialize message: {}", e))?;
        
        let mut stream = TcpStream::connect_timeout(
            &address.parse().map_err(|e| format!("Invalid address: {}", e))?,
            Duration::from_secs(5)
        ).map_err(|e| format!("Failed to connect to {}: {}", address, e))?;
        
        stream.set_write_timeout(Some(Duration::from_secs(5)))
            .map_err(|e| format!("Failed to set timeout: {}", e))?;
        
        stream.write_all(json_message.as_bytes())
            .map_err(|e| format!("Failed to send message: {}", e))?;
        
        stream.write_all(b"\n")
            .map_err(|e| format!("Failed to send newline: {}", e))?;
        
        stream.flush()
            .map_err(|e| format!("Failed to flush stream: {}", e))?;
        
        Ok(())
    }
}

impl OutputClient for TcpOutputClient {
    fn send_message(&self, message: &OutputMessage, route: &RouteConfig) -> Result<(), String> {
        match route.output_type.as_str() {
            "tcp" => self.send_tcp(&route.output_address, message),
            "stdout" => {
                let json_output = serde_json::to_string(message)
                    .map_err(|e| format!("Failed to serialize: {}", e))?;
                println!("[OUTPUT:{}] {}", route.name, json_output);
                Ok(())
            }
            _ => Err(format!("Unknown output type: {}", route.output_type)),
        }
    }
}
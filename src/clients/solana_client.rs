use super::{OutputClient, TcpOutputClient};
use crate::config::RouteConfig;
use crate::models::OutputMessage;

pub struct SolanaClient {
    tcp_client: TcpOutputClient,
}

impl SolanaClient {
    pub fn new() -> Self {
        Self {
            tcp_client: TcpOutputClient::new(),
        }
    }
    
    pub fn forward_message(&self, message: &OutputMessage, route: &RouteConfig) -> Result<(), String> {
        println!("[SOLANA_CLIENT] Forwarding with route name: '{}'", route.name);
        self.tcp_client.send_message(message, route)
    }
}
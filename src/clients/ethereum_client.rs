use super::{OutputClient, TcpOutputClient};
use crate::config::RouteConfig;
use crate::models::OutputMessage;

pub struct EthereumClient {
    tcp_client: TcpOutputClient,
}

impl EthereumClient {
    pub fn new() -> Self {
        Self {
            tcp_client: TcpOutputClient::new(),
        }
    }
    
    pub fn forward_message(&self, message: &OutputMessage, route: &RouteConfig) -> Result<(), String> {
        self.tcp_client.send_message(message, route)
    }
}
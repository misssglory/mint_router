use crate::clients::{
    bnb_client::BnbClient,
    ethereum_client::EthereumClient,
    solana_client::SolanaClient,
};
use crate::config::{Config, RouteConfig};
use crate::models::{IncomingMessage, OutputArgs, OutputMessage};
use regex::Regex;
use std::sync::Arc;
use std::time::SystemTime;

pub struct Router {
    config: Arc<Config>,
    solana_client: SolanaClient,
    ethereum_client: EthereumClient,
    bnb_client: BnbClient,
    route_patterns: Vec<(RouteConfig, Regex)>,
    solana_mint_pattern: Regex,
}

impl Router {
    pub fn new(config: Arc<Config>) -> Result<Self, regex::Error> {
        let mut route_patterns = Vec::new();
        
        for route in &config.routes {
            if route.enabled {
                let pattern = Regex::new(&route.pattern)?;
                route_patterns.push((route.clone(), pattern));
            }
        }
        
        let solana_mint_pattern = Regex::new(r"[1-9A-HJ-NP-Za-km-z]{32,44}")?;
        
        Ok(Self {
            config,
            solana_client: SolanaClient::new(),
            ethereum_client: EthereumClient::new(),
            bnb_client: BnbClient::new(),
            route_patterns,
            solana_mint_pattern,
        })
    }
    
    pub fn process_message(&self, msg: &IncomingMessage) {
        let timestamp = get_timestamp_micros();
        
        // Collect all potential mint addresses from different sources
        let mut all_mints = Vec::new();
        
        // 1. Add mints from the mints array
        all_mints.extend(msg.mints.clone());
        
        // 2. Extract base58 strings from text (for Solana)
        all_mints.extend(self.extract_base58_mints(&msg.text));
        
        // 3. Extract Ethereum addresses from text
        all_mints.extend(self.extract_eth_addresses(&msg.text));
        
        // Remove duplicates while preserving order
        let mut unique_mints = Vec::new();
        for mint in all_mints {
            if !unique_mints.contains(&mint) {
                unique_mints.push(mint);
            }
        }
        
        if unique_mints.is_empty() && self.config.monitoring.log_messages {
            println!("[ROUTER] No mint addresses found in message");
            return;
        }
        
        for mint_address in &unique_mints {
            // Find which route matches this mint address
            if let Some((route, _)) = self.route_patterns
                .iter()
                .find(|(_, pattern)| pattern.is_match(mint_address))
            {
                let output_msg = self.create_output_message(msg, route, mint_address, timestamp);
                
                if self.config.monitoring.log_forwarding {
                    println!("[ROUTER] Forwarding mint {} to {} ({})", 
                             mint_address, route.name, route.output_address);
                }
                
                // Forward to appropriate client based on route name
                let result = match route.name.as_str() {
                    "solana" => self.solana_client.forward_message(&output_msg, route),
                    "ethereum" => self.ethereum_client.forward_message(&output_msg, route),
                    "bnb" => self.bnb_client.forward_message(&output_msg, route),
                    _ => {
                        if self.config.monitoring.log_errors {
                            eprintln!("[ROUTER] Unknown route name: '{}'", route.name);
                        }
                        Err(format!("Unknown route: {}", route.name))
                    },
                };
                
                if let Err(err) = result {
                    if self.config.monitoring.log_errors {
                        eprintln!("[ROUTER] Failed to forward to {}: {}", route.name, err);
                    }
                } else if self.config.monitoring.log_forwarding {
                    println!("[ROUTER] Successfully forwarded to {}", route.name);
                }
            } else if self.config.monitoring.log_messages {
                println!("[ROUTER] No matching route for mint: {}", mint_address);
            }
        }
    }
    
    fn extract_base58_mints(&self, text: &str) -> Vec<String> {
        let mut mints = Vec::new();
        
        for capture in self.solana_mint_pattern.find_iter(text) {
            let candidate = capture.as_str();
            
            if candidate.len() >= 32 && candidate.len() <= 44 {
                if !candidate.starts_with("http") && 
                   !candidate.contains("://") &&
                   !candidate.contains(".com") &&
                   !candidate.contains(".org") {
                    mints.push(candidate.to_string());
                }
            }
        }
        
        mints
    }
    
    fn extract_eth_addresses(&self, text: &str) -> Vec<String> {
        let mut addresses = Vec::new();
        let eth_pattern = Regex::new(r"0x[a-fA-F0-9]{40}").unwrap();
        
        for capture in eth_pattern.find_iter(text) {
            addresses.push(capture.as_str().to_string());
        }
        
        addresses
    }
    
    fn create_output_message(
        &self,
        msg: &IncomingMessage,
        route: &RouteConfig,
        mint_address: &str,
        timestamp: i64,
    ) -> OutputMessage {
        OutputMessage {
            context: mint_address.to_string(),
            command: route.output_format.command.clone(),
            args: OutputArgs {
                channel: msg.chat_name.clone(),
                ts: timestamp,
                text: msg.text.clone(),
            },
        }
    }
}

fn get_timestamp_micros() -> i64 {
    let now = SystemTime::now();
    let duration = now.duration_since(SystemTime::UNIX_EPOCH).unwrap();
    (duration.as_secs() as i64) * 1_000_000 + (duration.subsec_micros() as i64)
}
use crate::clients::{
    bnb_client::BnbClient, ethereum_client::EthereumClient, solana_client::SolanaClient,
};
use crate::config::{Config, RouteConfig};
use crate::database::DatabaseManager;
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
    db: Arc<DatabaseManager>,
}
impl Router {
    pub async fn new(
        config: Arc<Config>,
        db: Arc<DatabaseManager>,
    ) -> Result<Self, regex::Error> {
        let mut route_patterns = Vec::new();
        for route in &config.routes {
            if route.enabled {
                let pattern = Regex::new(&route.pattern)?;
                route_patterns.push((route.clone(), pattern));
            }
        }
        Ok(Self {
            config,
            solana_client: SolanaClient::new(),
            ethereum_client: EthereumClient::new(),
            bnb_client: BnbClient::new(),
            route_patterns,
            db,
        })
    }
    pub async fn process_message(&self, msg: &IncomingMessage) {
        let timestamp = get_timestamp_micros();
        let escape_newlines = self.get_escape_newlines_setting();
        let all_mints = self.collect_all_mints(msg);
        for mint in all_mints {
            if let Err(err) = self
                .db
                .store_message(
                    &mint,
                    msg.chat_id,
                    &msg.chat_name,
                    &msg.text,
                    timestamp,
                    escape_newlines,
                )
                .await
            {
                if self.config.monitoring.log_errors {
                    eprintln!(
                        "[ROUTER] Failed to store message for mint {}: {}", mint, err
                    );
                }
            } else if self.config.monitoring.log_messages {
                println!("[ROUTER] Stored message for mint: {}", mint);
            }
        }
        let solana_mints = self.collect_solana_mints(msg);
        for mint_address in solana_mints {
            if let Some(route) = self.find_route_by_name("solana") {
                if self.config.monitoring.log_forwarding {
                    println!("[ROUTER] Processing Solana mint: {}", mint_address);
                }
                let output_msg = self
                    .create_output_message(msg, route, &mint_address, timestamp);
                self.forward_to_client(&output_msg, route, &mint_address);
            }
        }
        let eth_mints = self.collect_ethereum_mints(msg);
        for mint_address in eth_mints {
            if let Some(route) = self.find_route_by_name("ethereum") {
                if self.config.monitoring.log_forwarding {
                    println!("[ROUTER] Processing Ethereum mint: {}", mint_address);
                }
                let output_msg = self
                    .create_output_message(msg, route, &mint_address, timestamp);
                self.forward_to_client(&output_msg, route, &mint_address);
            }
        }
        let bnb_mints = self.collect_bnb_mints(msg);
        for mint_address in bnb_mints {
            if let Some(route) = self.find_route_by_name("bnb") {
                if self.config.monitoring.log_forwarding {
                    println!("[ROUTER] Processing BNB mint: {}", mint_address);
                }
                let output_msg = self
                    .create_output_message(msg, route, &mint_address, timestamp);
                self.forward_to_client(&output_msg, route, &mint_address);
            }
        }
    }
    fn collect_solana_mints(&self, msg: &IncomingMessage) -> Vec<String> {
        let mut mints = Vec::new();
        mints.extend(msg.solana_mints.clone());
        for mint in &msg.mints {
            if !mint.starts_with("0x") && mint.len() >= 32 && mint.len() <= 44 {
                mints.push(mint.clone());
            }
        }
        if let Some(addresses) = &msg.addresses {
            if let Some(solana_addrs) = addresses.get("solana") {
                mints.extend(solana_addrs.clone());
            }
        }
        mints.sort();
        mints.dedup();
        mints
    }
    fn collect_ethereum_mints(&self, msg: &IncomingMessage) -> Vec<String> {
        let mut mints = Vec::new();
        mints.extend(msg.ethereum_mints.clone());
        mints.extend(msg.evm_mints.clone());
        for mint in &msg.mints {
            if mint.starts_with("0x") && mint.len() == 42 {
                if !msg.bsc_mints.contains(mint) {
                    mints.push(mint.clone());
                }
            }
        }
        if let Some(addresses) = &msg.addresses {
            if let Some(eth_addrs) = addresses.get("ethereum") {
                mints.extend(eth_addrs.clone());
            }
        }
        mints.sort();
        mints.dedup();
        mints
    }
    fn collect_bnb_mints(&self, msg: &IncomingMessage) -> Vec<String> {
        let mut mints = Vec::new();
        mints.extend(msg.bsc_mints.clone());
        for mint in &msg.mints {
            if msg.bsc_mints.contains(mint) {
                mints.push(mint.clone());
            }
        }
        if let Some(addresses) = &msg.addresses {
            if let Some(bsc_addrs) = addresses.get("bsc") {
                mints.extend(bsc_addrs.clone());
            }
            if let Some(unknown_addrs) = addresses.get("unknown") {
                for addr in unknown_addrs {
                    if self.is_bnb_address(addr, &msg.text) {
                        mints.push(addr.clone());
                    }
                }
            }
        }
        mints.sort();
        mints.dedup();
        mints
    }
    fn is_bnb_address(&self, _address: &str, context: &str) -> bool {
        let context_lower = context.to_lowercase();
        let bnb_keywords = ["bsc", "bnb", "binance", "bep20", "pancake", "bakery"];
        for keyword in bnb_keywords {
            if context_lower.contains(keyword) {
                return true;
            }
        }
        false
    }
    fn find_route_by_name(&self, name: &str) -> Option<&RouteConfig> {
        self.route_patterns
            .iter()
            .find(|(route, _)| route.name == name)
            .map(|(route, _)| route)
    }
    fn forward_to_client(
        &self,
        output_msg: &OutputMessage,
        route: &RouteConfig,
        mint_address: &str,
    ) {
        if self.config.monitoring.log_forwarding {
            println!(
                "[ROUTER] Forwarding mint {} to {} ({})", mint_address, route.name, route
                .output_address
            );
        }
        let result = match route.name.as_str() {
            "solana" => self.solana_client.forward_message(output_msg, route),
            "ethereum" => self.ethereum_client.forward_message(output_msg, route),
            "bnb" => self.bnb_client.forward_message(output_msg, route),
            _ => {
                if self.config.monitoring.log_errors {
                    eprintln!("[ROUTER] Unknown route name: '{}'", route.name);
                }
                Err(format!("Unknown route: {}", route.name))
            }
        };
        if let Err(err) = result {
            if self.config.monitoring.log_errors {
                eprintln!("[ROUTER] Failed to forward to {}: {}", route.name, err);
            }
        } else if self.config.monitoring.log_forwarding {
            println!("[ROUTER] Successfully forwarded to {}", route.name);
        }
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
    fn collect_all_mints(&self, msg: &IncomingMessage) -> Vec<String> {
        let mut all_mints = Vec::new();
        all_mints.extend(msg.mints.clone());
        all_mints.extend(msg.solana_mints.clone());
        all_mints.extend(msg.ethereum_mints.clone());
        all_mints.extend(msg.evm_mints.clone());
        all_mints.extend(msg.bsc_mints.clone());
        if let Some(addresses) = &msg.addresses {
            for (_, addrs) in addresses {
                all_mints.extend(addrs.clone());
            }
        }
        all_mints.sort();
        all_mints.dedup();
        all_mints
    }
    fn get_escape_newlines_setting(&self) -> bool {
        self.config
            .routes
            .first()
            .map(|r| r.output_format.escape_newlines)
            .unwrap_or(false)
    }
}
fn get_timestamp_micros() -> i64 {
    let now = SystemTime::now();
    let duration = now.duration_since(SystemTime::UNIX_EPOCH).unwrap();
    (duration.as_secs() as i64) * 1_000_000 + (duration.subsec_micros() as i64)
}

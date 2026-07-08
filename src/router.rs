use crate::clients::{
    bnb_client::BnbClient, ethereum_client::EthereumClient, solana_client::SolanaClient,
};
use crate::config::{Config, RouteConfig};
use crate::database::DatabaseManager;
use crate::models::{IncomingMessage, OutputArgs, OutputMessage};
use regex::Regex;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{debug, error, info, warn};

pub struct Router {
    config: Arc<Config>,
    solana_client: SolanaClient,
    ethereum_client: EthereumClient,
    bnb_client: BnbClient,
    route_patterns: Vec<(RouteConfig, Regex)>,
    db: Arc<DatabaseManager>,
}

impl Router {
    pub async fn new(config: Arc<Config>, db: Arc<DatabaseManager>) -> Result<Self, regex::Error> {
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
        if !self.should_process_message(msg) {
            return;
        }

        let timestamp = get_timestamp_micros();
        let escape_newlines = self.get_escape_newlines_setting();

        debug!(
            "[ROUTER] Processing message - Original context: '{}', pool: {:?}",
            msg.context, msg.pool
        );

        // Store messages for mints
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
                    error!(
                        "[ROUTER] Failed to store message for mint {}: {}",
                        mint, err
                    );
                }
            } else if self.config.monitoring.log_messages {
                info!("[ROUTER] Stored message for mint: {}", mint);
            }
        }

        // Store messages for pools
        let all_pools = self.collect_all_pools(msg);
        for pool in all_pools {
            if let Err(err) = self
                .db
                .store_message(
                    &format!("pool:{}", pool),
                    msg.chat_id,
                    &msg.chat_name,
                    &msg.text,
                    timestamp,
                    escape_newlines,
                )
                .await
            {
                if self.config.monitoring.log_errors {
                    error!(
                        "[ROUTER] Failed to store message for pool {}: {}",
                        pool, err
                    );
                }
            } else if self.config.monitoring.log_messages {
                info!("[ROUTER] Stored message for pool: {}", pool);
            }
        }

        // Process Solana mints and pools
        let solana_mints = self.collect_solana_mints(msg);
        let solana_pools = self.collect_solana_pools(msg);

        for mint_address in solana_mints {
            if let Some(route) = self.find_route_by_name("solana") {
                if self.config.monitoring.log_forwarding {
                    debug!("[ROUTER] Processing Solana mint: {}", mint_address);
                }
                let output_msg = self.create_output_message(
                    msg,
                    route,
                    Some(mint_address.clone()),
                    None,
                    timestamp,
                );
                self.forward_to_client(&output_msg, route, &mint_address);
            }
        }

        for pool_address in solana_pools {
            if let Some(route) = self.find_route_by_name("solana") {
                if self.config.monitoring.log_forwarding {
                    debug!("[ROUTER] Processing Solana pool: {}", pool_address);
                }
                let output_msg = self.create_output_message(
                    msg,
                    route,
                    None,
                    Some(pool_address.clone()),
                    timestamp,
                );
                self.forward_to_client(&output_msg, route, &pool_address);
            }
        }

        // Process Ethereum mints and pools
        let eth_mints = self.collect_ethereum_mints(msg);
        let eth_pools = self.collect_ethereum_pools(msg);

        for mint_address in eth_mints {
            if let Some(route) = self.find_route_by_name("ethereum") {
                if self.config.monitoring.log_forwarding {
                    debug!("[ROUTER] Processing Ethereum mint: {}", mint_address);
                }
                let output_msg = self.create_output_message(
                    msg,
                    route,
                    Some(mint_address.clone()),
                    None,
                    timestamp,
                );
                self.forward_to_client(&output_msg, route, &mint_address);
            }
        }

        for pool_address in eth_pools {
            if let Some(route) = self.find_route_by_name("ethereum") {
                if self.config.monitoring.log_forwarding {
                    debug!("[ROUTER] Processing Ethereum pool: {}", pool_address);
                }
                let output_msg = self.create_output_message(
                    msg,
                    route,
                    None,
                    Some(pool_address.clone()),
                    timestamp,
                );
                self.forward_to_client(&output_msg, route, &pool_address);
            }
        }

        // Process BNB mints and pools
        let bnb_mints = self.collect_bnb_mints(msg);
        let bnb_pools = self.collect_bnb_pools(msg);

        for mint_address in bnb_mints {
            if let Some(route) = self.find_route_by_name("bnb") {
                if self.config.monitoring.log_forwarding {
                    debug!("[ROUTER] Processing BNB mint: {}", mint_address);
                }
                let output_msg = self.create_output_message(
                    msg,
                    route,
                    Some(mint_address.clone()),
                    None,
                    timestamp,
                );
                self.forward_to_client(&output_msg, route, &mint_address);
            }
        }

        for pool_address in bnb_pools {
            if let Some(route) = self.find_route_by_name("bnb") {
                if self.config.monitoring.log_forwarding {
                    debug!("[ROUTER] Processing BNB pool: {}", pool_address);
                }
                let output_msg = self.create_output_message(
                    msg,
                    route,
                    None,
                    Some(pool_address.clone()),
                    timestamp,
                );
                self.forward_to_client(&output_msg, route, &pool_address);
            }
        }

        // Process Base chain mints and pools - route to Ethereum endpoint
        let base_mints = self.collect_base_mints(msg);
        let base_pools = self.collect_base_pools(msg);

        for mint_address in base_mints {
            if let Some(route) = self.find_route_by_name("ethereum") {
                if self.config.monitoring.log_forwarding {
                    debug!(
                        "[ROUTER] Processing Base mint: {} -> routing to Ethereum endpoint",
                        mint_address
                    );
                }
                let output_msg = self.create_output_message(
                    msg,
                    route,
                    Some(mint_address.clone()),
                    None,
                    timestamp,
                );
                self.forward_to_client(&output_msg, route, &mint_address);
            }
        }

        for pool_address in base_pools {
            if let Some(route) = self.find_route_by_name("ethereum") {
                if self.config.monitoring.log_forwarding {
                    debug!(
                        "[ROUTER] Processing Base pool: {} -> routing to Ethereum endpoint",
                        pool_address
                    );
                }
                let output_msg = self.create_output_message(
                    msg,
                    route,
                    None,
                    Some(pool_address.clone()),
                    timestamp,
                );
                self.forward_to_client(&output_msg, route, &pool_address);
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

    fn collect_solana_pools(&self, msg: &IncomingMessage) -> Vec<String> {
        let mut pools = Vec::new();
        pools.extend(msg.solana_pools.clone());
        for pool in &msg.pools {
            if !pool.starts_with("0x") && pool.len() >= 32 && pool.len() <= 44 {
                pools.push(pool.clone());
            }
        }
        if let Some(pool_addresses) = &msg.pool_addresses {
            if let Some(solana_pools) = pool_addresses.get("solana") {
                pools.extend(solana_pools.clone());
            }
        }
        pools.sort();
        pools.dedup();
        pools
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

    fn collect_ethereum_pools(&self, msg: &IncomingMessage) -> Vec<String> {
        let mut pools = Vec::new();
        pools.extend(msg.ethereum_pools.clone());
        pools.extend(msg.evm_pools.clone());
        if let Some(pool_addresses) = &msg.pool_addresses {
            if let Some(eth_pools) = pool_addresses.get("ethereum") {
                pools.extend(eth_pools.clone());
            }
            if let Some(evm_pools) = pool_addresses.get("evm") {
                pools.extend(evm_pools.clone());
            }
        }
        pools.sort();
        pools.dedup();
        pools
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
        }
        mints.sort();
        mints.dedup();
        mints
    }

    fn collect_bnb_pools(&self, msg: &IncomingMessage) -> Vec<String> {
        let mut pools = Vec::new();
        pools.extend(msg.bsc_pools.clone());
        if let Some(pool_addresses) = &msg.pool_addresses {
            if let Some(bsc_pools) = pool_addresses.get("bsc") {
                pools.extend(bsc_pools.clone());
            }
        }
        pools.sort();
        pools.dedup();
        pools
    }

    // ADD Base chain collection methods
    fn collect_base_mints(&self, msg: &IncomingMessage) -> Vec<String> {
        let mut mints = Vec::new();
        mints.extend(msg.base_mints.clone());
        if let Some(addresses) = &msg.addresses {
            if let Some(base_addrs) = addresses.get("base") {
                mints.extend(base_addrs.clone());
            }
        }
        mints.sort();
        mints.dedup();
        mints
    }

    fn collect_base_pools(&self, msg: &IncomingMessage) -> Vec<String> {
        let mut pools = Vec::new();
        pools.extend(msg.base_pools.clone());
        if let Some(pool_addresses) = &msg.pool_addresses {
            if let Some(base_pools) = pool_addresses.get("base") {
                pools.extend(base_pools.clone());
            }
        }
        pools.sort();
        pools.dedup();
        pools
    }

    fn collect_all_mints(&self, msg: &IncomingMessage) -> Vec<String> {
        let mut all_mints = Vec::new();
        all_mints.extend(msg.mints.clone());
        all_mints.extend(msg.solana_mints.clone());
        all_mints.extend(msg.ethereum_mints.clone());
        all_mints.extend(msg.evm_mints.clone());
        all_mints.extend(msg.bsc_mints.clone());
        all_mints.extend(msg.base_mints.clone()); // ADD Base mints
        if let Some(addresses) = &msg.addresses {
            for (_, addrs) in addresses {
                all_mints.extend(addrs.clone());
            }
        }
        all_mints.sort();
        all_mints.dedup();
        all_mints
    }

    fn collect_all_pools(&self, msg: &IncomingMessage) -> Vec<String> {
        let mut all_pools = Vec::new();
        all_pools.extend(msg.pools.clone());
        all_pools.extend(msg.solana_pools.clone());
        all_pools.extend(msg.ethereum_pools.clone());
        all_pools.extend(msg.evm_pools.clone());
        all_pools.extend(msg.bsc_pools.clone());
        all_pools.extend(msg.base_pools.clone()); // ADD Base pools
        if let Some(pool_addresses) = &msg.pool_addresses {
            for (_, pools) in pool_addresses {
                all_pools.extend(pools.clone());
            }
        }
        all_pools.sort();
        all_pools.dedup();
        all_pools
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

    fn log_forwarding_details(
        &self,
        route: &RouteConfig,
        address: &str,
        output_msg: &OutputMessage,
    ) {
        if !self.config.monitoring.log_forwarding {
            return;
        }

        info!(
            target: "forwarding",
            route = %route.name,
            output_address = %route.output_address,
            address = %address,
            command = %output_msg.command,
            channel = %output_msg.args.channel,
            "Forwarding to route"
        );

        if self.config.monitoring.log_forwarding_payload {
            match serde_json::to_string_pretty(output_msg) {
                Ok(payload) => {
                    debug!(
                        target: "forwarding_payload",
                        route = %route.name,
                        payload = %payload,
                        "Forwarding payload details"
                    );
                }
                Err(e) => {
                    error!("Failed to serialize output message for logging: {}", e);
                }
            }
        }
    }

    fn forward_to_client(&self, output_msg: &OutputMessage, route: &RouteConfig, address: &str) {
        self.log_forwarding_details(route, address, output_msg);
        if self.config.monitoring.log_forwarding {
            info!(
                "[ROUTER] Forwarding address {} to {} ({})",
                address, route.name, route.output_address
            );
        }
        let result = match route.name.as_str() {
            "solana" => self.solana_client.forward_message(output_msg, route),
            "ethereum" => self.ethereum_client.forward_message(output_msg, route),
            "bnb" => self.bnb_client.forward_message(output_msg, route),
            _ => {
                if self.config.monitoring.log_errors {
                    error!("[ROUTER] Unknown route name: '{}'", route.name);
                }
                Err(format!("Unknown route: {}", route.name))
            }
        };
        if let Err(err) = result {
            if self.config.monitoring.log_errors {
                error!(
                    target: "forwarding_errors",
                    route = %route.name,
                    address = %address,
                    error = %err,
                    "Failed to forward to route"
                );
            }
        } else if self.config.monitoring.log_forwarding {
            info!("[ROUTER] Successfully forwarded to {}", route.name);
        }
    }

    fn create_output_message(
        &self,
        msg: &IncomingMessage,
        route: &RouteConfig,
        mint_address: Option<String>,
        pool_address: Option<String>,
        timestamp: i64,
    ) -> OutputMessage {
        let context = if let Some(ref mint) = mint_address {
            mint.clone()
        } else {
            msg.context.clone()
        };
        let pool = pool_address;
        debug!(
            "[ROUTER] Creating output message - Context: '{}', mint: {:?}, pool: {:?}, route: {}",
            context, mint_address, pool, route.name
        );
        OutputMessage {
            context,
            command: route.output_format.command.clone(),
            args: OutputArgs {
                channel: self.get_output_channel(msg, route),
                ts: timestamp,
                text: msg.text.clone(),
            },
            pool,
        }
    }

    fn get_output_channel(&self, msg: &IncomingMessage, route: &RouteConfig) -> String {
        match route.output_format.channel_field.as_str() {
            "chat_id" | "channel_id" | "id" => msg.chat_id.to_string(),
            "chat_name" | "channel_name" | "name" => msg.chat_name.clone(),
            unknown => {
                warn!(
                    target: "config",
                    route = %route.name,
                    channel_field = %unknown,
                    "Unknown channel_field value; defaulting to chat_name"
                );
                msg.chat_name.clone()
            }
        }
    }

    fn get_escape_newlines_setting(&self) -> bool {
        self.config
            .routes
            .first()
            .map(|r| r.output_format.escape_newlines)
            .unwrap_or(false)
    }

    fn should_process_message(&self, msg: &IncomingMessage) -> bool {
        let should_ignore = self
            .config
            .should_ignore_channel(msg.chat_id, &msg.chat_name);

        if should_ignore && self.config.monitoring.log_ignored_messages {
            info!(
                target: "ignored_channels",
                chat_id = msg.chat_id,
                chat_name = %msg.chat_name,
                "Message from ignored channel, skipping processing"
            );
        }

        !should_ignore
    }
}

fn get_timestamp_micros() -> i64 {
    let now = SystemTime::now();
    let duration = now.duration_since(SystemTime::UNIX_EPOCH).unwrap();
    (duration.as_secs() as i64) * 1_000_000 + (duration.subsec_micros() as i64)
}

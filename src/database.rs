use crate::config::DatabaseConfig;
use deadpool_postgres::{Config as PoolConfig, Pool, Runtime};
use lru::LruCache;
use lz4_flex::block::{compress, decompress};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_postgres::{Client, NoTls, Row};

mod models {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct StoredMessage {
        pub chat_id: i64,
        pub chat_name: String,
        pub text: String,
        pub timestamp_us: i64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct QueryResult {
        pub chat_id: i64,
        pub chat_name: String,
        pub text: String,
        pub timestamp_us: i64,
    }

    pub type ChatEntry = (i64, String);
}

mod cache {
    use super::*;
    use crate::database::models::QueryResult;

    pub struct ChatCache {
        id_to_name: LruCache<i64, String>,
        name_to_id: LruCache<String, i64>,
    }

    impl ChatCache {
        pub fn new(capacity: usize) -> Self {
            let cap = NonZeroUsize::new(capacity.max(1)).unwrap();
            Self {
                id_to_name: LruCache::new(cap),
                name_to_id: LruCache::new(cap),
            }
        }

        pub fn get_name(&mut self, chat_id: i64) -> Option<String> {
            self.id_to_name.get(&chat_id).cloned()
        }

        pub fn get_id(&mut self, chat_name: &str) -> Option<i64> {
            self.name_to_id.get(chat_name).cloned()
        }

        pub fn insert(&mut self, chat_id: i64, chat_name: String) {
            self.id_to_name.put(chat_id, chat_name.clone());
            self.name_to_id.put(chat_name, chat_id);
        }
    }

    #[derive(Debug, Clone)]
    struct MintCacheEntry {
        messages: Vec<QueryResult>,
        last_update: SystemTime,
    }

    pub struct MintCache {
        inner: LruCache<String, MintCacheEntry>,
        ttl: Duration,
    }

    impl MintCache {
        pub fn new(capacity: usize, ttl_seconds: u64) -> Self {
            let cap = NonZeroUsize::new(capacity.max(1)).unwrap();
            Self {
                inner: LruCache::new(cap),
                ttl: Duration::from_secs(ttl_seconds),
            }
        }

        pub fn get(&mut self, mint: &str) -> Option<Vec<QueryResult>> {
            let entry = self.inner.get(mint)?;
            let age = entry.last_update.elapsed().ok()?;
            if age < self.ttl {
                Some(entry.messages.clone())
            } else {
                None
            }
        }

        pub fn put(&mut self, mint: String, messages: Vec<QueryResult>) {
            self.inner.put(
                mint,
                MintCacheEntry {
                    messages,
                    last_update: SystemTime::now(),
                },
            );
        }

        pub fn invalidate(&mut self, mint: &str) {
            self.inner.pop(mint);
        }

        pub fn cleanup_expired(&mut self) {
            let now = SystemTime::now();
            let ttl = self.ttl;

            let expired: Vec<String> = self
                .inner
                .iter()
                .filter(|(_, entry)| {
                    now.duration_since(entry.last_update)
                        .unwrap_or(Duration::ZERO)
                        >= ttl
                })
                .map(|(key, _)| key.clone())
                .collect();

            for key in expired {
                self.inner.pop(&key);
            }
        }

        pub fn clear(&mut self) {
            self.inner.clear();
        }
    }

    pub use ChatCache as ChatCacheImpl;
    pub use MintCache as MintCacheImpl;
}

mod sql {
    pub const CREATE_SCHEMA: &str = r#"
        CREATE TABLE IF NOT EXISTS messages (
            id BIGSERIAL PRIMARY KEY,
            mint TEXT NOT NULL,
            chat_id BIGINT NOT NULL,
            chat_name TEXT NOT NULL,
            text BYTEA NOT NULL,
            timestamp_us BIGINT NOT NULL,
            compressed BOOLEAN NOT NULL DEFAULT false,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_mint ON messages(mint);
        CREATE INDEX IF NOT EXISTS idx_timestamp ON messages(timestamp_us);
        CREATE INDEX IF NOT EXISTS idx_chat_id ON messages(chat_id);
        CREATE INDEX IF NOT EXISTS idx_mint_timestamp ON messages(mint, timestamp_us DESC);

        CREATE TABLE IF NOT EXISTS chats (
            chat_id BIGINT PRIMARY KEY,
            chat_name TEXT NOT NULL,
            last_seen_us BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_chat_name ON chats(chat_name);
        CREATE INDEX IF NOT EXISTS idx_chat_last_seen ON chats(last_seen_us DESC);
    "#;

    pub const INSERT_MESSAGE: &str = r#"
        INSERT INTO messages (mint, chat_id, chat_name, text, timestamp_us, compressed)
        VALUES ($1, $2, $3, $4, $5, $6)
    "#;

    pub const UPSERT_CHAT: &str = r#"
        INSERT INTO chats (chat_id, chat_name, last_seen_us)
        VALUES ($1, $2, $3)
        ON CONFLICT (chat_id)
        DO UPDATE SET
            chat_name = EXCLUDED.chat_name,
            last_seen_us = EXCLUDED.last_seen_us,
            updated_at = NOW()
    "#;

    pub const SELECT_MESSAGES_BY_MINT: &str = r#"
        SELECT chat_id, chat_name, text, timestamp_us, compressed
        FROM messages
        WHERE mint = $1
        ORDER BY timestamp_us DESC
    "#;

    pub const SELECT_CHAT_NAME_BY_ID: &str = r#"
        SELECT chat_name
        FROM chats
        WHERE chat_id = $1
    "#;

    pub const SELECT_CHAT_ID_BY_NAME: &str = r#"
        SELECT chat_id
        FROM chats
        WHERE chat_name = $1
    "#;

    pub const SELECT_UNIQUE_CHATS_FOR_MINT: &str = r#"
        SELECT DISTINCT chat_id, chat_name
        FROM messages
        WHERE mint = $1
        ORDER BY chat_name
    "#;

    pub const DELETE_OLD_MESSAGES: &str = r#"
        DELETE FROM messages
        WHERE timestamp_us < $1
    "#;

    pub const DELETE_ORPHAN_CHATS: &str = r#"
        DELETE FROM chats
        WHERE NOT EXISTS (
            SELECT 1
            FROM messages
            WHERE messages.chat_id = chats.chat_id
        )
    "#;

    pub const SELECT_STATS: &str = r#"
        SELECT
            COUNT(*) as total_messages,
            COUNT(DISTINCT mint) as unique_mints,
            COUNT(DISTINCT chat_id) as unique_chats,
            MIN(timestamp_us) as oldest_message,
            MAX(timestamp_us) as newest_message
        FROM messages
    "#;

    pub const SELECT_MINTS_FOR_CHAT_ID: &str = r#"
        SELECT DISTINCT mint
        FROM messages
        WHERE chat_id = $1
        ORDER BY mint
    "#;

    pub const SELECT_MINTS_FOR_CHAT_NAME: &str = r#"
        SELECT DISTINCT mint
        FROM messages
        WHERE chat_name = $1
        ORDER BY mint
    "#;

    pub const SELECT_CHATS_WITH_ANY_MINT: &str = r#"
        SELECT DISTINCT chat_id, chat_name
        FROM messages
        WHERE mint IS NOT NULL AND mint <> ''
        ORDER BY chat_name
    "#;

    pub const SELECT_CHANNELS_BY_SUBSTRING: &str = r#"
        SELECT chat_id, chat_name
        FROM chats
        WHERE chat_name ILIKE '%' || $1 || '%'
        ORDER BY chat_name
    "#;

    pub const SELECT_CONTEXTS_BY_CHANNEL_SUBSTRING: &str = r#"
        SELECT DISTINCT m.mint
        FROM messages m
        JOIN chats c ON c.chat_id = m.chat_id
        WHERE c.chat_name ILIKE '%' || $1 || '%'
        ORDER BY m.mint
    "#;
}

pub use models::{ChatEntry, QueryResult, StoredMessage};

pub struct DatabaseManager {
    pool: Pool,
    chat_cache: Arc<Mutex<cache::ChatCacheImpl>>,
    mint_cache: Arc<Mutex<cache::MintCacheImpl>>,
    config: DatabaseConfig,
}

impl DatabaseManager {
    pub async fn new(config: DatabaseConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let pool = Self::build_pool(&config)?;
        Self::init_schema(&pool).await?;

        Ok(Self {
            pool,
            chat_cache: Arc::new(Mutex::new(cache::ChatCacheImpl::new(10_000))),
            mint_cache: Arc::new(Mutex::new(cache::MintCacheImpl::new(
                Self::cache_capacity_entries(config.cache_size_mb),
                config.cache_ttl_seconds,
            ))),
            config,
        })
    }

    fn build_pool(config: &DatabaseConfig) -> Result<Pool, Box<dyn std::error::Error>> {
        let mut pg = PoolConfig::new();
        pg.host = Some(config.host.clone());
        pg.port = Some(config.port);
        pg.user = Some(config.user.clone());
        pg.password = Some(config.password.clone());
        pg.dbname = Some(config.dbname.clone());
        pg.pool = Some(deadpool_postgres::PoolConfig {
            max_size: config.pool_size,
            timeouts: deadpool_postgres::Timeouts::default(),
            ..Default::default()
        });

        let pool = pg.create_pool(Some(Runtime::Tokio1), NoTls)?;
        Ok(pool)
    }

    async fn init_schema(pool: &Pool) -> Result<(), Box<dyn std::error::Error>> {
        let client = pool.get().await?;
        client.batch_execute(sql::CREATE_SCHEMA).await?;
        Ok(())
    }

    fn cache_capacity_entries(cache_size_mb: usize) -> usize {
        let bytes = cache_size_mb.max(1) * 1024 * 1024;
        let approx_entry_size = 1024usize;
        (bytes / approx_entry_size).max(1)
    }

    async fn client(&self) -> Result<deadpool_postgres::Client, String> {
        self.pool
            .get()
            .await
            .map_err(|e| format!("Failed to get database connection: {}", e))
    }

    fn normalize_text(&self, text: &str, escape_newlines: bool) -> String {
        if escape_newlines {
            text.replace('\n', "\\n")
        } else {
            text.to_string()
        }
    }

    fn encode_text(&self, text: &str) -> Result<(Vec<u8>, bool), String> {
        if self.config.compress_messages {
            Ok((compress(text.as_bytes()), true))
        } else {
            Ok((text.as_bytes().to_vec(), false))
        }
    }

    fn decode_text(&self, data: &[u8], compressed: bool) -> Result<String, String> {
        if !compressed {
            return String::from_utf8(data.to_vec()).map_err(|e| format!("Invalid UTF-8: {}", e));
        }

        let decompressed =
            decompress(data, 10 * 1024 * 1024).map_err(|e| format!("Decompression failed: {:?}", e))?;

        String::from_utf8(decompressed).map_err(|e| format!("Invalid UTF-8: {}", e))
    }

    fn map_message_row(&self, row: Row) -> Result<QueryResult, String> {
        let chat_id: i64 = row.get(0);
        let chat_name: String = row.get(1);
        let text_data: Vec<u8> = row.get(2);
        let timestamp_us: i64 = row.get(3);
        let compressed: bool = row.get(4);
        let text = self.decode_text(&text_data, compressed)?;

        Ok(QueryResult {
            chat_id,
            chat_name,
            text,
            timestamp_us,
        })
    }

    fn map_chat_row(row: Row) -> ChatEntry {
        let chat_id: i64 = row.get(0);
        let chat_name: String = row.get(1);
        (chat_id, chat_name)
    }

    fn rows_to_strings(rows: Vec<Row>, column: &str) -> Vec<String> {
        rows.into_iter()
            .filter_map(|row| row.try_get::<_, String>(column).ok())
            .collect()
    }

    fn insert_chat_cache(&self, chat_id: i64, chat_name: String) {
        if let Ok(mut cache) = self.chat_cache.lock() {
            cache.insert(chat_id, chat_name);
        }
    }

    fn invalidate_mint_cache(&self, mint: &str) {
        if let Ok(mut cache) = self.mint_cache.lock() {
            cache.invalidate(mint);
        }
    }

    fn try_get_cached_messages(&self, mint: &str) -> Option<Vec<QueryResult>> {
        self.mint_cache.lock().ok()?.get(mint)
    }

    fn cache_messages(&self, mint: &str, messages: &[QueryResult]) {
        if let Ok(mut cache) = self.mint_cache.lock() {
            cache.put(mint.to_string(), messages.to_vec());
        }
    }

    pub async fn store_message(
        &self,
        mint: &str,
        chat_id: i64,
        chat_name: &str,
        text: &str,
        timestamp_us: i64,
        escape_newlines: bool,
    ) -> Result<(), String> {
        let normalized = self.normalize_text(text, escape_newlines);
        let (encoded_text, compressed) = self.encode_text(&normalized)?;
        let client = self.client().await?;

        self.insert_message_row(
            &client,
            mint,
            chat_id,
            chat_name,
            &encoded_text,
            timestamp_us,
            compressed,
        )
        .await?;

        self.upsert_chat_row(&client, chat_id, chat_name, timestamp_us)
            .await?;

        self.insert_chat_cache(chat_id, chat_name.to_string());
        self.invalidate_mint_cache(mint);

        Ok(())
    }

    async fn insert_message_row(
        &self,
        client: &Client,
        mint: &str,
        chat_id: i64,
        chat_name: &str,
        encoded_text: &[u8],
        timestamp_us: i64,
        compressed: bool,
    ) -> Result<(), String> {
        client
            .execute(
                sql::INSERT_MESSAGE,
                &[&mint, &chat_id, &chat_name, &encoded_text, &timestamp_us, &compressed],
            )
            .await
            .map_err(|e| format!("Failed to insert message: {}", e))?;

        Ok(())
    }

    async fn upsert_chat_row(
        &self,
        client: &Client,
        chat_id: i64,
        chat_name: &str,
        timestamp_us: i64,
    ) -> Result<(), String> {
        client
            .execute(sql::UPSERT_CHAT, &[&chat_id, &chat_name, &timestamp_us])
            .await
            .map_err(|e| format!("Failed to update chat: {}", e))?;

        Ok(())
    }

    pub async fn get_messages_by_mint(
        &self,
        mint: &str,
        use_cache: bool,
    ) -> Result<Vec<QueryResult>, String> {
        if use_cache {
            if let Some(cached) = self.try_get_cached_messages(mint) {
                return Ok(cached);
            }
        }

        let client = self.client().await?;
        let rows = client
            .query(sql::SELECT_MESSAGES_BY_MINT, &[&mint])
            .await
            .map_err(|e| format!("Failed to query messages: {}", e))?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            messages.push(self.map_message_row(row)?);
        }

        if use_cache && !messages.is_empty() {
            self.cache_messages(mint, &messages);
        }

        Ok(messages)
    }

    pub async fn get_chat_name(&self, chat_id: i64) -> Option<String> {
        if let Ok(mut cache) = self.chat_cache.lock() {
            if let Some(name) = cache.get_name(chat_id) {
                return Some(name);
            }
        }

        let client = self.pool.get().await.ok()?;
        let row = client
            .query_opt(sql::SELECT_CHAT_NAME_BY_ID, &[&chat_id])
            .await
            .ok()??;

        let chat_name: String = row.get(0);
        self.insert_chat_cache(chat_id, chat_name.clone());
        Some(chat_name)
    }

    pub async fn get_chat_id(&self, chat_name: &str) -> Option<i64> {
        if let Ok(mut cache) = self.chat_cache.lock() {
            if let Some(id) = cache.get_id(chat_name) {
                return Some(id);
            }
        }

        let client = self.pool.get().await.ok()?;
        let row = client
            .query_opt(sql::SELECT_CHAT_ID_BY_NAME, &[&chat_name])
            .await
            .ok()??;

        let chat_id: i64 = row.get(0);
        self.insert_chat_cache(chat_id, chat_name.to_string());
        Some(chat_id)
    }

    pub async fn get_unique_chats_for_mint(&self, mint: &str) -> Result<Vec<ChatEntry>, String> {
        let client = self.client().await?;
        let rows = client
            .query(sql::SELECT_UNIQUE_CHATS_FOR_MINT, &[&mint])
            .await
            .map_err(|e| format!("Failed to query unique chats: {}", e))?;

        Ok(rows.into_iter().map(Self::map_chat_row).collect())
    }

    pub async fn cleanup_old_entries(&self, max_age_seconds: u64) -> Result<usize, String> {
        let cutoff = Self::cutoff_timestamp_us(max_age_seconds);
        let client = self.client().await?;

        let deleted = client
            .execute(sql::DELETE_OLD_MESSAGES, &[&cutoff])
            .await
            .map_err(|e| format!("Failed to delete old messages: {}", e))?;

        client
            .execute(sql::DELETE_ORPHAN_CHATS, &[])
            .await
            .map_err(|e| format!("Failed to cleanup chats: {}", e))?;

        if let Ok(mut mint_cache) = self.mint_cache.lock() {
            mint_cache.clear();
            mint_cache.cleanup_expired();
        }

        Ok(deleted as usize)
    }

    fn cutoff_timestamp_us(max_age_seconds: u64) -> i64 {
        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_micros() as i64;

        now_us - (max_age_seconds as i64 * 1_000_000)
    }

    pub async fn get_stats(&self) -> Result<serde_json::Value, String> {
        let client = self.client().await?;
        let row = client
            .query_one(sql::SELECT_STATS, &[])
            .await
            .map_err(|e| format!("Failed to get stats: {}", e))?;

        Ok(json!({
            "total_messages": row.get::<_, i64>(0),
            "unique_mints": row.get::<_, i64>(1),
            "unique_chats": row.get::<_, i64>(2),
            "oldest_message_us": row.get::<_, Option<i64>>(3),
            "newest_message_us": row.get::<_, Option<i64>>(4),
        }))
    }

    pub async fn get_mints_for_chat_id(
        &self,
        chat_id: i64,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let client = self.pool.get().await?;
        let rows = client.query(sql::SELECT_MINTS_FOR_CHAT_ID, &[&chat_id]).await?;
        Ok(Self::rows_to_strings(rows, "mint"))
    }

    pub async fn get_mints_for_chat_name(
        &self,
        chat_name: String,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let client = self.pool.get().await?;
        let rows = client
            .query(sql::SELECT_MINTS_FOR_CHAT_NAME, &[&chat_name])
            .await?;
        Ok(Self::rows_to_strings(rows, "mint"))
    }

    pub async fn get_chats_with_any_mint(&self) -> Result<Vec<ChatEntry>, String> {
        let client = self.client().await?;
        let rows = client
            .query(sql::SELECT_CHATS_WITH_ANY_MINT, &[])
            .await
            .map_err(|e| format!("Failed to query chats with mints: {}", e))?;

        Ok(rows.into_iter().map(Self::map_chat_row).collect())
    }

    pub async fn get_channels_by_name_substring(
        &self,
        substring: &str,
    ) -> Result<Vec<ChatEntry>, String> {
        let client = self.client().await?;
        let rows = client
            .query(sql::SELECT_CHANNELS_BY_SUBSTRING, &[&substring])
            .await
            .map_err(|e| format!("Failed to query channels by substring: {}", e))?;

        Ok(rows.into_iter().map(Self::map_chat_row).collect())
    }

    pub async fn get_contexts_by_channel_substring(
        &self,
        substring: &str,
    ) -> Result<Vec<String>, String> {
        let client = self.client().await?;
        let rows = client
            .query(sql::SELECT_CONTEXTS_BY_CHANNEL_SUBSTRING, &[&substring])
            .await
            .map_err(|e| format!("Failed to query contexts by channel substring: {}", e))?;

        Ok(Self::rows_to_strings(rows, "mint"))
    }
}
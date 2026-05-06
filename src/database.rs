use crate::config::DatabaseConfig;
use async_trait::async_trait;
use deadpool_postgres::{
    Config as PoolConfig, Manager, ManagerConfig, Pool, RecyclingMethod, Runtime,
};
use lru::LruCache;
use lz4_flex::block::{compress, decompress};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_postgres::{NoTls, Row};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMessage {
    pub chat_id: i64,
    pub chat_name: String,
    pub text: String,
    pub timestamp_us: i64,
}
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub chat_id: i64,
    pub chat_name: String,
    pub text: String,
    pub timestamp_us: i64,
}
struct ChatCache {
    id_to_name: LruCache<i64, String>,
    name_to_id: LruCache<String, i64>,
}
impl ChatCache {
    fn new(capacity: usize) -> Self {
        Self {
            id_to_name: LruCache::new(NonZeroUsize::new(capacity).unwrap()),
            name_to_id: LruCache::new(NonZeroUsize::new(capacity).unwrap()),
        }
    }
    fn get_name(&mut self, chat_id: i64) -> Option<String> {
        self.id_to_name.get(&chat_id).cloned()
    }
    fn get_id(&mut self, chat_name: &str) -> Option<i64> {
        self.name_to_id.get(chat_name).cloned()
    }
    fn insert(&mut self, chat_id: i64, chat_name: String) {
        self.id_to_name.put(chat_id, chat_name.clone());
        self.name_to_id.put(chat_name, chat_id);
    }
}
struct MintCacheEntry {
    messages: Vec<QueryResult>,
    last_update: SystemTime,
}
struct MintCache {
    inner: LruCache<String, MintCacheEntry>,
    ttl: Duration,
}
impl MintCache {
    fn new(capacity: usize, ttl_seconds: u64) -> Self {
        Self {
            inner: LruCache::new(NonZeroUsize::new(capacity).unwrap()),
            ttl: Duration::from_secs(ttl_seconds),
        }
    }
    fn get(&mut self, mint: &str) -> Option<Vec<QueryResult>> {
        if let Some(entry) = self.inner.get(mint) {
            if entry.last_update.elapsed().ok()? < self.ttl {
                return Some(entry.messages.clone());
            }
        }
        None
    }
    fn put(&mut self, mint: String, messages: Vec<QueryResult>) {
        self.inner.put(
            mint,
            MintCacheEntry {
                messages,
                last_update: SystemTime::now(),
            },
        );
    }
    fn cleanup_expired(&mut self) {
        let now = SystemTime::now();
        let ttl = self.ttl;
        let to_remove: Vec<String> = self
            .inner
            .iter()
            .filter(|(_, entry)| {
                now.duration_since(entry.last_update)
                    .unwrap_or(Duration::ZERO)
                    >= ttl
            })
            .map(|(k, _)| k.clone())
            .collect();
        for key in to_remove {
            self.inner.pop(&key);
        }
    }
}
pub struct DatabaseManager {
    pool: Pool,
    chat_cache: Arc<Mutex<ChatCache>>,
    mint_cache: Arc<Mutex<MintCache>>,
    config: DatabaseConfig,
}
impl DatabaseManager {
    pub async fn new(config: DatabaseConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let mut pg_config = PoolConfig::new();
        pg_config.host = Some(config.host.clone());
        pg_config.port = Some(config.port);
        pg_config.user = Some(config.user.clone());
        pg_config.password = Some(config.password.clone());
        pg_config.dbname = Some(config.dbname.clone());
        pg_config.pool = Some(deadpool_postgres::PoolConfig {
            max_size: config.pool_size,
            timeouts: deadpool_postgres::Timeouts::default(),
            ..Default::default()
        });
        let pool = pg_config.create_pool(Some(Runtime::Tokio1), NoTls)?;
        let client = pool.get().await?;
        client
            .batch_execute(
                r#"
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
            "#,
            )
            .await?;
        let cache_capacity = config.cache_size_mb * 1024 * 1024 / 1024;
        Ok(Self {
            pool,
            chat_cache: Arc::new(Mutex::new(ChatCache::new(10000))),
            mint_cache: Arc::new(Mutex::new(MintCache::new(
                cache_capacity,
                config.cache_ttl_seconds,
            ))),
            config,
        })
    }
    fn compress_text(&self, text: &str) -> Result<Vec<u8>, String> {
        if !self.config.compress_messages {
            return Ok(text.as_bytes().to_vec());
        }
        let compressed = compress(text.as_bytes());
        Ok(compressed)
    }
    fn decompress_text(&self, data: &[u8], compressed: bool) -> Result<String, String> {
        if !compressed {
            return String::from_utf8(data.to_vec()).map_err(|e| format!("Invalid UTF-8: {}", e));
        }
        let decompressed = decompress(data, 10 * 1024 * 1024)
            .map_err(|e| format!("Decompression failed: {:?}", e))?;
        String::from_utf8(decompressed).map_err(|e| format!("Invalid UTF-8: {}", e))
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
        let mut text_processed = text.to_string();
        if escape_newlines {
            text_processed = text_processed.replace('\n', "\\n");
        }
        let compressed_data = self.compress_text(&text_processed)?;
        let compressed_flag = self.config.compress_messages;
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| format!("Failed to get database connection: {}", e))?;
        client
            .execute(
                "INSERT INTO messages (mint, chat_id, chat_name, text, timestamp_us, compressed) 
             VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &mint,
                    &chat_id,
                    &chat_name,
                    &compressed_data,
                    &timestamp_us,
                    &compressed_flag,
                ],
            )
            .await
            .map_err(|e| format!("Failed to insert message: {}", e))?;
        client
            .execute(
                "INSERT INTO chats (chat_id, chat_name, last_seen_us) 
             VALUES ($1, $2, $3)
             ON CONFLICT (chat_id) 
             DO UPDATE SET chat_name = EXCLUDED.chat_name, last_seen_us = EXCLUDED.last_seen_us",
                &[&chat_id, &chat_name, &timestamp_us],
            )
            .await
            .map_err(|e| format!("Failed to update chat: {}", e))?;
        let mut chat_cache = self.chat_cache.lock().unwrap();
        chat_cache.insert(chat_id, chat_name.to_string());
        let mut mint_cache = self.mint_cache.lock().unwrap();
        mint_cache.inner.pop(mint);
        Ok(())
    }
    pub async fn get_messages_by_mint(
        &self,
        mint: &str,
        use_cache: bool,
    ) -> Result<Vec<QueryResult>, String> {
        if use_cache {
            let mut mint_cache = self.mint_cache.lock().unwrap();
            if let Some(cached) = mint_cache.get(mint) {
                return Ok(cached);
            }
        }
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| format!("Failed to get database connection: {}", e))?;
        let rows = client
            .query(
                "SELECT chat_id, chat_name, text, timestamp_us, compressed 
             FROM messages 
             WHERE mint = $1 
             ORDER BY timestamp_us DESC",
                &[&mint],
            )
            .await
            .map_err(|e| format!("Failed to query messages: {}", e))?;
        let mut messages = Vec::new();
        for row in rows {
            let chat_id: i64 = row.get(0);
            let chat_name: String = row.get(1);
            let text_data: Vec<u8> = row.get(2);
            let timestamp_us: i64 = row.get(3);
            let compressed: bool = row.get(4);
            let text = self.decompress_text(&text_data, compressed)?;
            messages.push(QueryResult {
                chat_id,
                chat_name,
                text,
                timestamp_us,
            });
        }
        if use_cache && !messages.is_empty() {
            let mut mint_cache = self.mint_cache.lock().unwrap();
            mint_cache.put(mint.to_string(), messages.clone());
        }
        Ok(messages)
    }
    pub async fn get_chat_name(&self, chat_id: i64) -> Option<String> {
        {
            let mut cache = self.chat_cache.lock().unwrap();
            if let Some(name) = cache.get_name(chat_id) {
                return Some(name);
            }
        }
        let client = self.pool.get().await.ok()?;
        let rows = client
            .query(
                "SELECT chat_name FROM chats WHERE chat_id = $1",
                &[&chat_id],
            )
            .await
            .ok()?;
        if let Some(row) = rows.first() {
            let name: String = row.get(0);
            let mut cache = self.chat_cache.lock().unwrap();
            cache.insert(chat_id, name.clone());
            Some(name)
        } else {
            None
        }
    }
    pub async fn get_chat_id(&self, chat_name: &str) -> Option<i64> {
        {
            let mut cache = self.chat_cache.lock().unwrap();
            if let Some(id) = cache.get_id(chat_name) {
                return Some(id);
            }
        }
        let client = self.pool.get().await.ok()?;
        let rows = client
            .query(
                "SELECT chat_id FROM chats WHERE chat_name = $1",
                &[&chat_name],
            )
            .await
            .ok()?;
        if let Some(row) = rows.first() {
            let id: i64 = row.get(0);
            let mut cache = self.chat_cache.lock().unwrap();
            cache.insert(id, chat_name.to_string());
            Some(id)
        } else {
            None
        }
    }
    pub async fn get_unique_chats_for_mint(
        &self,
        mint: &str,
    ) -> Result<Vec<(i64, String)>, String> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| format!("Failed to get database connection: {}", e))?;
        let rows = client
            .query(
                "SELECT DISTINCT chat_id, chat_name 
             FROM messages 
             WHERE mint = $1 
             ORDER BY chat_name",
                &[&mint],
            )
            .await
            .map_err(|e| format!("Failed to query unique chats: {}", e))?;
        let mut chats = Vec::new();
        for row in rows {
            let chat_id: i64 = row.get(0);
            let chat_name: String = row.get(1);
            chats.push((chat_id, chat_name));
        }
        Ok(chats)
    }
    pub async fn cleanup_old_entries(&self, max_age_seconds: u64) -> Result<usize, String> {
        let cutoff = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64
            - (max_age_seconds as i64 * 1_000_000);
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| format!("Failed to get database connection: {}", e))?;
        let deleted = client
            .execute("DELETE FROM messages WHERE timestamp_us < $1", &[&cutoff])
            .await
            .map_err(|e| format!("Failed to delete old messages: {}", e))?;
        client
            .execute(
                "DELETE FROM chats WHERE NOT EXISTS (SELECT 1 FROM messages WHERE messages.chat_id = chats.chat_id)",
                &[],
            )
            .await
            .map_err(|e| format!("Failed to cleanup chats: {}", e))?;
        let mut mint_cache = self.mint_cache.lock().unwrap();
        mint_cache.cleanup_expired();
        Ok(deleted as usize)
    }
    pub async fn get_stats(&self) -> Result<serde_json::Value, String> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| format!("Failed to get database connection: {}", e))?;
        let row = client
            .query_one(
                "SELECT 
                COUNT(*) as total_messages,
                COUNT(DISTINCT mint) as unique_mints,
                COUNT(DISTINCT chat_id) as unique_chats,
                MIN(timestamp_us) as oldest_message,
                MAX(timestamp_us) as newest_message
             FROM messages",
                &[],
            )
            .await
            .map_err(|e| format!("Failed to get stats: {}", e))?;
        Ok(serde_json::json!(
            { "total_messages" : row.get::< _, i64 > (0), "unique_mints" : row.get::<
            _, i64 > (1), "unique_chats" : row.get::< _, i64 > (2),
            "oldest_message_us" : row.get::< _, Option < i64 >> (3),
            "newest_message_us" : row.get::< _, Option < i64 >> (4), }
        ))
    }
    pub async fn get_mints_for_chat_id(
        &self,
        chat_id: i64,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT DISTINCT mint FROM messages WHERE chat_id = $1",
                &[&chat_id],
            )
            .await?;
        let mints = rows
            .into_iter()
            .filter_map(|row| row.try_get::<_, String>("mint").ok())
            .collect();
        Ok(mints)
    }
    pub async fn get_mints_for_chat_name(
        &self,
        chat_name: String,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT DISTINCT mint FROM messages WHERE chat_name = $1",
                &[&chat_name],
            )
            .await?;
        let mints = rows
            .into_iter()
            .filter_map(|row| row.try_get::<_, String>("mint").ok())
            .collect();
        Ok(mints)
    }
    pub async fn get_chats_with_any_mint(&self) -> Result<Vec<(i64, String)>, String> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| format!("Failed to get database connection: {}", e))?;

        let rows = client
            .query(
                "SELECT DISTINCT chat_id, chat_name
                 FROM messages
                 WHERE mint IS NOT NULL AND mint <> ''
                 ORDER BY chat_name",
                &[],
            )
            .await
            .map_err(|e| format!("Failed to query chats with mints: {}", e))?;

        let mut chats = Vec::new();
        for row in rows {
            let chat_id: i64 = row.get(0);
            let chat_name: String = row.get(1);
            chats.push((chat_id, chat_name));
        }

        Ok(chats)
    }
}

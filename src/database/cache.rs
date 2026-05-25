use crate::database::models::QueryResult;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::time::{Duration, SystemTime};

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
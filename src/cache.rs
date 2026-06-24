use std::any::TypeId;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{OnceLock, RwLock};
use std::time::Duration;

use crate::entry::Entry;
use crate::hasher::tick;
use crate::traits::{CacheStrategy, ImcCacheable};

// ---------------------------------------------------------------------------
// Per-type cache
// ---------------------------------------------------------------------------

pub(crate) struct PerTypeCache {
    strategy: CacheStrategy,
    max_size: usize,
    data: HashMap<u64, Entry>,
    index: HashMap<u64, u64>,
}

impl PerTypeCache {
    pub(crate) fn from_trait<T: ImcCacheable>() -> Self {
        #[cfg(feature = "invalidation-redis")]
        crate::invalidation::register::<T>();
        Self {
            strategy: T::cache_strategy(),
            max_size: T::cache_max_size(),
            data: HashMap::new(),
            index: HashMap::new(),
        }
    }

    /// Try to fetch a cached value.  Returns `None` on miss, type-mismatch
    /// or TTL expiry.
    pub(crate) fn get<V: Clone + Send + Sync + 'static>(&self, args_hash: u64) -> Option<V> {
        let id_hash = self.index.get(&args_hash)?;
        let entry = self.data.get(id_hash)?;

        if let Some(ttl) = entry.ttl {
            if entry.created_at.elapsed() > ttl {
                return None;
            }
        }

        let v = entry.value.downcast_ref::<V>()?.clone();
        entry.access_count.fetch_add(1, Ordering::Relaxed);
        entry.last_accessed.store(tick(), Ordering::Relaxed);
        Some(v)
    }

    /// Store a value, deduplicating by `id_hash`.
    pub(crate) fn set<V: Send + Sync + 'static>(
        &mut self,
        args_hash: u64,
        id_hash: u64,
        value: V,
        ttl: Option<Duration>,
    ) {
        // 1. Remove expired entry (if any)
        let expired = self.data.get(&id_hash).map_or(false, |e| {
            e.ttl.map_or(false, |ttl| e.created_at.elapsed() > ttl)
        });
        if expired {
            self.data.remove(&id_hash);
        }

        // 2. Dedup — only when the existing copy is still fresh
        if self.data.contains_key(&id_hash) {
            self.index.insert(args_hash, id_hash);
            return;
        }

        // 3. Evict if full (skip when background worker is active)
        if self.data.len() >= self.max_size
            && crate::worker::WORKER_TX.lock().unwrap_or_else(|e| e.into_inner()).is_none()
        {
            if let Some(evict_id) = self.evict_candidate() {
                self.data.remove(&evict_id);
            }
        }

        // 4. Insert
        self.data.insert(id_hash, Entry::new(value, ttl));
        self.index.insert(args_hash, id_hash);
    }

    /// Select the entry to evict according to the configured strategy.
    fn evict_candidate(&self) -> Option<u64> {
        if self.data.is_empty() {
            return None;
        }

        match self.strategy {
            CacheStrategy::Lru => self
                .data
                .iter()
                .min_by_key(|(_, e)| e.last_accessed.load(Ordering::Relaxed))
                .map(|(k, _)| *k),
            CacheStrategy::Mru => self
                .data
                .iter()
                .max_by_key(|(_, e)| e.last_accessed.load(Ordering::Relaxed))
                .map(|(k, _)| *k),
            CacheStrategy::Lfu => self
                .data
                .iter()
                .min_by_key(|(_, e)| e.access_count.load(Ordering::Relaxed))
                .map(|(k, _)| *k),
            CacheStrategy::Mfu => self
                .data
                .iter()
                .max_by_key(|(_, e)| e.access_count.load(Ordering::Relaxed))
                .map(|(k, _)| *k),
            CacheStrategy::Fifo => self
                .data
                .iter()
                .min_by_key(|(_, e)| e.inserted_at)
                .map(|(k, _)| *k),
        }
    }

    pub(crate) fn remove_data(&mut self, id_hash: u64) {
        self.data.remove(&id_hash);
    }

    pub(crate) fn clear(&mut self) {
        self.data.clear();
        self.index.clear();
    }

    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    /// Remove all TTL-expired entries and their stale index references.
    pub(crate) fn remove_expired(&mut self) {
        self.data.retain(|_, e| {
            e.ttl.map_or(true, |ttl| e.created_at.elapsed() <= ttl)
        });
        self.index.retain(|_, id_hash| self.data.contains_key(id_hash));
    }

    /// Evict entries until `data.len() <= max_size` and clean up orphaned
    /// index entries.
    pub(crate) fn evict_to_max_size(&mut self) {
        while self.data.len() > self.max_size {
            if let Some(evict_id) = self.evict_candidate() {
                self.data.remove(&evict_id);
            }
        }
        self.index.retain(|_, id_hash| self.data.contains_key(id_hash));
    }
}

// ---------------------------------------------------------------------------
// Global cache registry
// ---------------------------------------------------------------------------

pub(crate) struct GlobalCache {
    pub(crate) stores: RwLock<HashMap<TypeId, PerTypeCache>>,
}

impl GlobalCache {
    fn new() -> Self {
        Self {
            stores: RwLock::new(HashMap::new()),
        }
    }
}

pub(crate) fn global() -> &'static GlobalCache {
    static G: OnceLock<GlobalCache> = OnceLock::new();
    G.get_or_init(GlobalCache::new)
}

/// Ensure the global cache store is initialised (no-op if already done).
pub(crate) fn global_init() {
    global();
}

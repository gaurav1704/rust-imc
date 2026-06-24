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

    pub(crate) fn get<V: Clone + Send + Sync + 'static>(&self, args_hash: u64) -> Option<V> {
        let id_hash = self.index.get(&args_hash)?;
        let entry = self.data.get(id_hash)?;

        if let Some(ttl) = entry.ttl {
            if entry.created_at.elapsed() > ttl {
                crate::metrics::record_miss();
                return None;
            }
        }

        let v = entry.value.downcast_ref::<V>()?.clone();
        entry.access_count.fetch_add(1, Ordering::Relaxed);
        entry.last_accessed.store(tick(), Ordering::Relaxed);
        crate::metrics::record_hit();
        Some(v)
    }

    pub(crate) fn set<V: Send + Sync + 'static>(
        &mut self,
        args_hash: u64,
        id_hash: u64,
        value: V,
        ttl: Option<Duration>,
    ) {
        let expired = self.data.get(&id_hash).map_or(false, |e| {
            e.ttl.map_or(false, |ttl| e.created_at.elapsed() > ttl)
        });
        if expired {
            self.data.remove(&id_hash);
        }

        if self.data.contains_key(&id_hash) {
            self.index.insert(args_hash, id_hash);
            crate::metrics::record_set();
            return;
        }

        if self.data.len() >= self.max_size
            && crate::worker::WORKER_TX.lock().unwrap_or_else(|e| e.into_inner()).is_none()
        {
            if let Some(evict_id) = self.evict_candidate() {
                self.data.remove(&evict_id);
                crate::metrics::record_eviction();
                crate::log_event!(DEBUG, crate::log::CACHE, crate::log::EVICT, id_hash = evict_id);
            }
        }

        self.data.insert(id_hash, Entry::new(value, ttl));
        self.index.insert(args_hash, id_hash);
        crate::metrics::record_set();
    }

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
        crate::log_event!(DEBUG, crate::log::CACHE, crate::log::REMOVE, id_hash = id_hash);
    }

    pub(crate) fn clear(&mut self) {
        let count = self.data.len();
        self.data.clear();
        self.index.clear();
        crate::metrics::record_expired(count as u64);
        crate::log_event!(INFO, crate::log::CACHE, crate::log::CLEAR, evicted = count);
    }

    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn remove_expired(&mut self) {
        let before = self.data.len();
        self.data.retain(|_, e| {
            e.ttl.map_or(true, |ttl| e.created_at.elapsed() <= ttl)
        });
        let removed = before - self.data.len();
        self.index.retain(|_, id_hash| self.data.contains_key(id_hash));
        if removed > 0 {
            crate::metrics::record_expired(removed as u64);
            crate::log_event!(DEBUG, crate::log::CACHE, crate::log::EXPIRY, count = removed);
        }
    }

    pub(crate) fn evict_to_max_size(&mut self) {
        let mut evicted = 0u64;
        while self.data.len() > self.max_size {
            if let Some(evict_id) = self.evict_candidate() {
                self.data.remove(&evict_id);
                evicted += 1;
            }
        }
        self.index.retain(|_, id_hash| self.data.contains_key(id_hash));
        if evicted > 0 {
            crate::metrics::record_eviction();
            crate::log_event!(DEBUG, crate::log::CACHE, crate::log::EVICT, count = evicted);
        }
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

pub(crate) fn global_init() {
    global();
}

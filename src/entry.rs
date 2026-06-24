use std::any::Any;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

use crate::hasher::tick;

/// A single cached value together with its access metadata.
pub(crate) struct Entry {
    pub(crate) value: Box<dyn Any + Send + Sync>,
    pub(crate) access_count: AtomicU64,
    pub(crate) last_accessed: AtomicU64,
    pub(crate) inserted_at: u64,
    pub(crate) created_at: Instant,
    pub(crate) ttl: Option<Duration>,
}

impl Entry {
    pub(crate) fn new<V: Send + Sync + 'static>(value: V, ttl: Option<Duration>) -> Self {
        let now = tick();
        Self {
            value: Box::new(value),
            access_count: AtomicU64::new(1),
            last_accessed: AtomicU64::new(now),
            inserted_at: now,
            created_at: Instant::now(),
            ttl,
        }
    }
}

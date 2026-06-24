use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

/// Deterministic FNV-1a hasher so the same value produces the same hash
/// across processes (required for cross-process invalidation).
pub(crate) struct StableHasher(u64);

impl StableHasher {
    pub(crate) fn new() -> Self {
        Self(14695981039346656037)
    }
}

impl Hasher for StableHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 ^= b as u64;
            self.0 = self.0.wrapping_mul(1099511628211);
        }
    }
}

/// Compute a deterministic stable hash for the given value.
/// Uses FNV-1a — consistent across processes on the same platform.
pub(crate) fn hash_value(v: impl Hash) -> u64 {
    let mut h = StableHasher::new();
    v.hash(&mut h);
    h.finish()
}

/// Monotonically increasing clock (for ordering metadata).
pub(crate) fn tick() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

use std::hash::Hash;
use std::time::Duration;

use crate::hasher::hash_value;

/// Eviction strategy for a per-type cache namespace.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CacheStrategy {
    /// Evict the entry accessed **least recently**.
    Lru,
    /// Evict the entry accessed **most recently**.
    Mru,
    /// Evict the entry accessed **least frequently**.
    Lfu,
    /// Evict the entry accessed **most frequently**.
    Mfu,
    /// Evict the entry that was inserted **earliest** (FIFO).
    Fifo,
}

/// Any type that should be cacheable **must** implement this trait.
///
/// Implementors define:
/// - a unique identifier (`Id`) extracted via [`cache_id`](ImcCacheable::cache_id)
/// - the eviction strategy, TTL and max-size for the type
///
/// # Example
///
/// ```ignore
/// #[derive(Clone)]
/// struct User { id: u32, name: String }
///
/// impl ImcCacheable for User {
///     type Id = u32;
///     fn cache_id(&self) -> u32 { self.id }
///     fn cache_ttl() -> Option<Duration> { Some(Duration::from_secs(300)) }
/// }
/// ```
pub trait ImcCacheable: Clone + Send + Sync + 'static {
    /// The unique-identifier type for this data (e.g. a primary key).
    type Id: Hash + Eq + Clone + Send + 'static;

    /// Extract the unique identifier from a value *after* it has been
    /// fetched.  This is used internally to deduplicate entries.
    fn cache_id(&self) -> Self::Id;

    // ── Optional defaults ──────────────────────────────────────────────

    /// Eviction strategy for this type's cache namespace.
    fn cache_strategy() -> CacheStrategy { CacheStrategy::Lru }

    /// Time-to-live for cached values.  `None` means they never expire.
    fn cache_ttl() -> Option<Duration> { None }

    /// Maximum number of entries allowed in this type's namespace.
    fn cache_max_size() -> usize { 10_000 }

    /// Optional: return the approximate byte-size of `self` for the
    /// per-value size limit check.  When `Some(s)` is returned and
    /// `s > cache_max_value_size()` the value is returned directly
    /// without caching.  `None` means "unknown — always cache".
    ///
    /// ```ignore
    /// fn cache_value_size(&self) -> Option<usize> {
    ///     Some(std::mem::size_of_val(self) + self.name.len())
    /// }
    /// ```
    fn cache_value_size(&self) -> Option<usize> { None }

    /// Maximum byte-size of a single cached value.  Values whose
    /// [`cache_value_size()`](ImcCacheable::cache_value_size) exceeds
    /// this are not stored (they bypass the cache).
    /// Default: 1 MiB.
    fn cache_max_value_size() -> usize { 1_048_576 }

    /// Optional: return a pub/sub channel name to enable cross-process
    /// cache invalidation for this type. Requires the `invalidation-redis`
    /// feature and a [`CacheWorker`](crate::worker::CacheWorker) with
    /// [`WorkerConfig::redis_connection_string`](crate::worker::WorkerConfig) configured.
    fn cache_invalidation_channel() -> Option<&'static str> { None }
}

// ---------------------------------------------------------------------------
// Blanket impl for Vec<T> — cache entire result sets
// ---------------------------------------------------------------------------

impl<T: ImcCacheable> ImcCacheable for Vec<T> {
    type Id = String;

    fn cache_id(&self) -> String {
        let ids: Vec<T::Id> = self.iter().map(|e| e.cache_id()).collect();
        hash_value(&ids).to_string()
    }

    fn cache_strategy() -> CacheStrategy { T::cache_strategy() }
    fn cache_ttl() -> Option<Duration> { T::cache_ttl() }
    fn cache_max_size() -> usize { T::cache_max_size() }
    fn cache_invalidation_channel() -> Option<&'static str> { None }
    fn cache_value_size(&self) -> Option<usize> {
        let elem: Option<usize> = self.iter().try_fold(0usize, |acc, e| {
            e.cache_value_size().map(|s| acc + s)
        });
        elem.map(|s| s + self.capacity() * std::mem::size_of::<T>())
    }
    fn cache_max_value_size() -> usize { T::cache_max_value_size() }
}

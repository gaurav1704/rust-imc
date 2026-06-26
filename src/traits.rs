use std::hash::Hash;
use std::time::Duration;

use crate::hasher::hash_value;

/// Marker trait for cache keys whose updates should be broadcast
/// to other pods via Redis pub/sub.
///
/// Derive this on your key enum with [`#[derive(CriticalKey)]`](crate::CriticalKey)
/// to automatically invalidate the corresponding cache entry across all
/// pods whenever [`through_imc_keyed`](crate::through_imc_keyed) stores a
/// new value for that key.
///
/// Requires the `critical` feature.
///
/// # Example
///
/// ```rust,ignore
/// use imc::CriticalKey;
///
/// #[derive(Hash, Clone, CriticalKey)]
/// enum UserKey { ById(i32), ByEmail(String) }
/// ```
#[cfg(feature = "critical")]
#[cfg_attr(docsrs, doc(cfg(feature = "critical")))]
pub trait CriticalKey: Hash + Clone + Send + 'static {
    /// Pub/sub channel name for this key type.
    ///
    /// The derive macro generates this automatically from
    /// `module_path!() + "::" + type_name`.
    fn channel() -> &'static str;
}

/// Eviction strategy for a per-type cache namespace.
///
/// Each type that implements [`ImcCacheable`] chooses one strategy.
/// The strategy determines which entry is evicted when the cache
/// exceeds [`ImcCacheable::cache_max_size`].
///
/// # Example
///
/// ```rust
/// use imc::CacheStrategy;
///
/// match CacheStrategy::Lru {
///     CacheStrategy::Lru => println!("evict least recently used"),
///     _ => unreachable!(),
/// }
/// ```
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
/// # Required methods
///
/// | Method | Returns | Purpose |
/// |--------|---------|---------|
/// | [`cache_id`](ImcCacheable::cache_id) | `Self::Id` | Extract unique identity from a value |
///
/// # Associated types
///
/// | Type | Bound | Purpose |
/// |------|-------|---------|
/// | `Id` | `Hash + Eq + Clone + Send + 'static` | Unique identity (e.g. primary key) |
/// | `Key` | `Hash + Clone + Send + 'static` | Cache-key type for [`through_imc_keyed`](crate::through_imc_keyed) |
///
/// # Optional methods
///
/// | Method | Default | Purpose |
/// |--------|---------|---------|
/// | [`cache_strategy`](ImcCacheable::cache_strategy) | `Lru` | Eviction strategy |
/// | [`cache_ttl`](ImcCacheable::cache_ttl) | `None` | Time-to-live per entry |
/// | [`cache_max_size`](ImcCacheable::cache_max_size) | `10_000` | Max entries per type |
/// | [`cache_value_size`](ImcCacheable::cache_value_size) | `None` | Byte-size for per-value limit |
/// | [`cache_max_value_size`](ImcCacheable::cache_max_value_size) | `1_048_576` | Max bytes per value (1 MiB) |
/// | [`cache_invalidation_channel`](ImcCacheable::cache_invalidation_channel) | `None` | Redis pub/sub channel for cross-process invalidation |
///
/// # Example
///
/// ```rust
/// use std::time::Duration;
/// use imc::{ImcCacheable, CacheStrategy};
///
/// #[derive(Clone, Debug, PartialEq)]
/// struct Product { id: u64, name: String, price: f64 }
///
/// impl ImcCacheable for Product {
///     type Id = u64;
///     type Key = String;
///
///     fn cache_id(&self) -> u64 { self.id }
///
///     fn cache_strategy() -> CacheStrategy { CacheStrategy::Lfu }
///     fn cache_ttl() -> Option<Duration> { Some(Duration::from_secs(600)) }
///     fn cache_max_size() -> usize { 5_000 }
/// }
/// ```
pub trait ImcCacheable: Clone + Send + Sync + 'static {
    /// The unique-identifier type for this data (e.g. a primary key).
    type Id: Hash + Eq + Clone + Send + 'static;

    /// Typed cache key for use with [`through_imc_keyed`](crate::through_imc_keyed).
    ///
    /// All types must specify this (typically `type Key = String;`).  Override
    /// with a closed enum to make invalid cache keys a compile-time error.
    ///
    /// # Example
    ///
    /// ```rust
    /// use imc::ImcCacheable;
    ///
    /// #[derive(Hash, Clone)]
    /// enum UserKey { ById(i32), ByEmail(String) }
    ///
    /// # #[derive(Clone)]
    /// # struct User { id: i32 }
    /// impl ImcCacheable for User {
    ///     type Id = i32;
    ///     type Key = UserKey;
    ///     fn cache_id(&self) -> i32 { self.id }
    /// }
    /// ```
    type Key: Hash + Clone + Send + 'static;

    /// Extract the unique identifier from a value *after* it has been
    /// fetched.  This is what the cache uses for deduplication — two
    /// queries that return values with the same `cache_id()` share a
    /// single backing entry.
    fn cache_id(&self) -> Self::Id;

    // ── Optional defaults ──────────────────────────────────────────────

    /// Eviction strategy for this type's cache namespace.
    ///
    /// See [`CacheStrategy`] for the available strategies.
    ///
    /// Default: [`CacheStrategy::Lru`].
    fn cache_strategy() -> CacheStrategy { CacheStrategy::Lru }

    /// Time-to-live for cached values.
    ///
    /// When `Some(duration)` is returned, entries older than `duration`
    /// are treated as expired on read and removed by the background sweep.
    /// `None` means entries never expire (eviction-only).
    fn cache_ttl() -> Option<Duration> { None }

    /// Maximum number of entries allowed in this type's cache namespace.
    ///
    /// When the number of entries exceeds this limit, the eviction
    /// strategy (see [`cache_strategy`](ImcCacheable::cache_strategy))
    /// determines which entry is removed.
    fn cache_max_size() -> usize { 10_000 }

    /// Optional: return the approximate byte-size of `self`.
    ///
    /// When `Some(s)` is returned and `s > cache_max_value_size()` the
    /// value bypasses the cache entirely — the closure executes every time.
    /// `None` means "unknown — always cache".
    ///
    /// # Example
    ///
    /// ```rust
    /// # use std::time::Duration;
    /// # use imc::{ImcCacheable, CacheStrategy};
    /// # #[derive(Clone)]
    /// # struct User { id: u32, name: String }
    /// impl ImcCacheable for User {
    /// #     type Id = u32;
    /// #     type Key = String;
    /// #     fn cache_id(&self) -> u32 { self.id }
    ///     fn cache_value_size(&self) -> Option<usize> {
    ///         Some(std::mem::size_of::<u32>() + self.name.len())
    ///     }
    /// #     fn cache_strategy() -> CacheStrategy { CacheStrategy::Lru }
    /// #     fn cache_ttl() -> Option<Duration> { Some(Duration::from_secs(300)) }
    /// #     fn cache_max_size() -> usize { 10_000 }
    /// }
    /// ```
    fn cache_value_size(&self) -> Option<usize> { None }

    /// Maximum byte-size of a single cached value.
    ///
    /// Values whose [`cache_value_size()`](ImcCacheable::cache_value_size)
    /// exceeds this threshold are not stored (they bypass the cache).
    ///
    /// Default: **1 MiB** (1_048_576 bytes).
    fn cache_max_value_size() -> usize { 1_048_576 }

    /// Optional: return a pub/sub channel name for cross-process
    /// cache invalidation.
    ///
    /// When a channel is configured and the `invalidation-redis` feature
    /// is enabled, publishing the stable hash of an `Id` on that channel
    /// removes the entry from every subscribing pod.
    ///
    /// See [`imc_invalidation_id`](crate::imc_invalidation_id) and
    /// [`WorkerConfig::redis_connection_string`](crate::WorkerConfig).
    fn cache_invalidation_channel() -> Option<&'static str> { None }
}

// ---------------------------------------------------------------------------
// Blanket impl for Vec<T> — cache entire result sets
// ---------------------------------------------------------------------------

/// [`Vec<T>`] implements [`ImcCacheable`] when `T` does.
///
/// This makes it possible to cache entire filtered result sets obtained
/// through tuple-based multi-condition keys.
///
/// # Dedup
///
/// The [`cache_id`](ImcCacheable::cache_id) hashes all element IDs
/// together.  Two queries returning the same logical set of entities
/// share one cached `Vec`, even if the query arguments differ.
impl<T: ImcCacheable> ImcCacheable for Vec<T> {
    type Id = String;
    type Key = String;

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

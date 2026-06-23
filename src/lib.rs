use std::any::{Any, TypeId};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{OnceLock, RwLock};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Trait – the only way to use the cache
// ---------------------------------------------------------------------------

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

    /// Eviction strategy for this type’s cache namespace.
    fn cache_strategy() -> CacheStrategy { CacheStrategy::Lru }

    /// Time-to-live for cached values.  `None` means they never expire.
    fn cache_ttl() -> Option<Duration> { None }

    /// Maximum number of entries allowed in this type’s namespace.
    fn cache_max_size() -> usize { 10_000 }
}

// ---------------------------------------------------------------------------
// Cache configuration (per-type, sourced from the trait)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Internal entry
// ---------------------------------------------------------------------------

struct Entry {
    value: Box<dyn Any + Send + Sync>,
    access_count: AtomicU64,
    last_accessed: AtomicU64,
    inserted_at: u64,
    created_at: Instant,
    ttl: Option<Duration>,
}

impl Entry {
    fn new<V: Send + Sync + 'static>(value: V, ttl: Option<Duration>) -> Self {
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

// ---------------------------------------------------------------------------
// Per-type cache
// ---------------------------------------------------------------------------

struct PerTypeCache {
    strategy: CacheStrategy,
    max_size: usize,
    data: HashMap<u64, Entry>,
    index: HashMap<u64, u64>,
}

impl PerTypeCache {
    fn from_trait<T: ImcCacheable>() -> Self {
        Self {
            strategy: T::cache_strategy(),
            max_size: T::cache_max_size(),
            data: HashMap::new(),
            index: HashMap::new(),
        }
    }

    /// Try to fetch a cached value.  Returns `None` on miss, eviction,
    /// type-mismatch or TTL expiry.
    fn get<V: Clone + Send + Sync + 'static>(&self, args_hash: u64) -> Option<V> {
        eprintln!("  get({}): index.len={}, data.len={}", args_hash, self.index.len(), self.data.len());
        let id_hash = self.index.get(&args_hash)?;
        eprintln!("  get: id_hash={}", id_hash);
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
    fn set<V: Send + Sync + 'static>(
        &mut self,
        args_hash: u64,
        id_hash: u64,
        value: V,
        ttl: Option<Duration>,
    ) {
        // ── 1.  Remove expired entry (if any) ──────────────────────
        let expired = self.data.get(&id_hash).map_or(false, |e| {
            e.ttl.map_or(false, |ttl| e.created_at.elapsed() > ttl)
        });
        if expired {
            self.data.remove(&id_hash);
        }

        // ── 2.  Dedup — only when the existing copy is still fresh ─
        if self.data.contains_key(&id_hash) {
            self.index.insert(args_hash, id_hash);
            return;
        }

        // ── 3.  Evict if full ──────────────────────────────────────
        if self.data.len() >= self.max_size {
            if let Some(evict_id) = self.evict_candidate() {
                self.data.remove(&evict_id);
            }
        }

        // ── 4.  Insert ─────────────────────────────────────────────
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

    fn remove_data(&mut self, id_hash: u64) {
        self.data.remove(&id_hash);
    }

    fn clear(&mut self) {
        self.data.clear();
        self.index.clear();
    }

    fn len(&self) -> usize {
        self.data.len()
    }
}

// ---------------------------------------------------------------------------
// Global cache registry
// ---------------------------------------------------------------------------

struct GlobalCache {
    stores: RwLock<HashMap<TypeId, PerTypeCache>>,
}

impl GlobalCache {
    fn new() -> Self {
        Self {
            stores: RwLock::new(HashMap::new()),
        }
    }
}

fn global() -> &'static GlobalCache {
    static G: OnceLock<GlobalCache> = OnceLock::new();
    G.get_or_init(GlobalCache::new)
}

/// Monotonically increasing clock (for ordering metadata).
fn tick() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

fn hash_value(v: impl Hash) -> u64 {
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

// ---------------------------------------------------------------------------
// Public API – the only entry points
// ---------------------------------------------------------------------------

/// Run a (usually expensive) closure `f`, caching its result.
///
/// `T` must implement [`ImcCacheable`].  The cache is keyed by the
/// `(type, args)` tuple; on subsequent calls with the **same args**
/// the cached value is returned without re-executing `f`.
///
/// When the returned value is stored its [`cache_id`](ImcCacheable::cache_id)
/// is extracted.  If an entry with that identity already exists **no
/// duplicate is created** – the existing copy is kept and the freshly
/// computed value is discarded.
///
/// # Example
///
/// ```
/// use std::time::Duration;
/// use imc::{ImcCacheable, CacheStrategy, through_imc};
///
/// #[derive(Clone, Debug, PartialEq)]
/// struct User { id: u32, name: String }
///
/// impl ImcCacheable for User {
///     type Id = u32;
///     fn cache_id(&self) -> u32 { self.id }
///     fn cache_strategy() -> CacheStrategy { CacheStrategy::Lru }
///     fn cache_ttl() -> Option<Duration> { Some(Duration::from_secs(300)) }
/// }
///
/// // Simulate fetching by user-id …
/// let u = through_imc::<User, _>(42u32, || User { id: 42, name: "Alice".into() });
/// assert_eq!(u.name, "Alice");
///
/// // … and by email.  Both queries return the *same* backing `User` object.
/// let u2 = through_imc::<User, _>("alice@example.com", || User { id: 42, name: "Alice".into() });
/// assert_eq!(u2.name, "Alice");
/// ```
pub fn through_imc<T, A, F>(args: A, f: F) -> T
where
    T: ImcCacheable,
    A: Hash + Clone + Send + 'static,
    F: FnOnce() -> T,
{
    let type_id = TypeId::of::<T>();
    let args_hash = hash_value(&args);
        eprintln!("through_imc args_hash={}", args_hash);

    // Fast path: read-lock check
    {
        let stores = global().stores.read().unwrap();
        if let Some(cache) = stores.get(&type_id) {
            if let Some(value) = cache.get::<T>(args_hash) {
                eprintln!("through_imc FAST path hit");
                return value;
            }
        }
    }

    // Miss – compute the value
    let value = f();
    let id = value.cache_id();
    let id_hash = hash_value(&id);
    eprintln!("through_imc id_hash={}", id_hash);
    let ttl = T::cache_ttl();

    // Write-lock to store (deduplicates internally)
    let mut stores = global().stores.write().unwrap();
    let cache = stores
        .entry(type_id)
        .or_insert_with(|| PerTypeCache::from_trait::<T>());
    cache.set::<T>(args_hash, id_hash, value, ttl);

    // Re-read to return the canonical copy (in case dedup kicked in)
    eprintln!("through_imc SLOW path, trying to read back");
    let result = cache.get::<T>(args_hash);
    eprintln!("through_imc read back: {:?}", result.is_some());
    result.expect("value was just stored")
}

/// Async version of [`through_imc`].
pub async fn through_imc_async<T, A, F, Fut>(args: A, f: F) -> T
where
    T: ImcCacheable,
    A: Hash + Clone + Send + 'static,
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    let type_id = TypeId::of::<T>();
    let args_hash = hash_value(&args);

    {
        let stores = global().stores.read().unwrap();
        if let Some(cache) = stores.get(&type_id) {
            if let Some(value) = cache.get::<T>(args_hash) {
                return value;
            }
        }
    }

    let value = f().await;
    let id = value.cache_id();
    let id_hash = hash_value(&id);
    let ttl = T::cache_ttl();

    let mut stores = global().stores.write().unwrap();
    let cache = stores
        .entry(type_id)
        .or_insert_with(|| PerTypeCache::from_trait::<T>());
    cache.set::<T>(args_hash, id_hash, value, ttl);

    cache
        .get::<T>(args_hash)
        .expect("value was just stored")
}

// ---------------------------------------------------------------------------
// Cache inspection / maintenance
// ---------------------------------------------------------------------------

/// Remove a single entry (by its unique [`Id`](ImcCacheable::Id)).
///
/// Stale index entries are *not* eagerly cleaned up; they will resolve to a
/// miss on the next access and be overwritten.
pub fn imc_remove<T: ImcCacheable>(id: &T::Id) {
    let type_id = TypeId::of::<T>();
    let id_hash = hash_value(id);

    let mut stores = global().stores.write().unwrap();
    if let Some(cache) = stores.get_mut(&type_id) {
        cache.remove_data(id_hash);
    }
}

/// Evict every cached entry for `T`.
pub fn imc_clear<T: ImcCacheable>() {
    let type_id = TypeId::of::<T>();

    let mut stores = global().stores.write().unwrap();
    if let Some(cache) = stores.get_mut(&type_id) {
        cache.clear();
    }
}

/// Number of unique entries currently cached for `T`.
pub fn imc_len<T: ImcCacheable>() -> usize {
    let type_id = TypeId::of::<T>();

    let stores = global().stores.read().unwrap();
    stores.get(&type_id).map_or(0, |c| c.len())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

    // ── Test helpers ─────────────────────────────────────────────────



    /// A simple cacheable type used throughout the core tests.
    #[derive(Clone, Debug, PartialEq)]
    struct Widget {
        id: u32,
        label: String,
    }

    impl ImcCacheable for Widget {
        type Id = u32;
        fn cache_id(&self) -> u32 { self.id }
        fn cache_strategy() -> CacheStrategy { CacheStrategy::Lru }
        fn cache_max_size() -> usize { 10_000 }
        fn cache_ttl() -> Option<Duration> { None }
    }

    /// Helper to call `through_imc` without turbofish.
    fn fetch<T: ImcCacheable, A: Hash + Clone + Send + 'static>(args: A, f: impl FnOnce() -> T) -> T {
        through_imc(args, f)
    }

    // ── Strategy tests ───────────────────────────────────────────────
    //
    // Each strategy uses its own struct (unique TypeId → no cross-test
    // interference).  `#[serial]` is not needed because they never touch
    // each other's namespace.

    macro_rules! strat_def {
        ($name:ident, $strategy:expr) => {
            #[derive(Clone, Debug, PartialEq)]
            struct $name { id: u32, _val: u32 }
            impl ImcCacheable for $name {
                type Id = u32;
                fn cache_id(&self) -> u32 { self.id }
                fn cache_strategy() -> CacheStrategy { $strategy }
                fn cache_max_size() -> usize { 3 }
                fn cache_ttl() -> Option<Duration> { None }
            }
        };
    }
    strat_def!(StratLru, CacheStrategy::Lru);
    strat_def!(StratMru, CacheStrategy::Mru);
    strat_def!(StratLfu, CacheStrategy::Lfu);
    strat_def!(StratMfu, CacheStrategy::Mfu);
    strat_def!(StratFifo, CacheStrategy::Fifo);

    #[test]
    fn test_lru_eviction() {
        fetch::<StratLru, _>(1u32, || StratLru { id: 1, _val: 10 });
        fetch::<StratLru, _>(2u32, || StratLru { id: 2, _val: 20 });
        fetch::<StratLru, _>(3u32, || StratLru { id: 3, _val: 30 });
        assert_eq!(imc_len::<StratLru>(), 3);

        // Touch id=1, making id=2 the least-recent → 4 evicts 2
        fetch::<StratLru, _>(1u32, || StratLru { id: 1, _val: 999 });
        fetch::<StratLru, _>(4u32, || StratLru { id: 4, _val: 40 });
        assert_eq!(imc_len::<StratLru>(), 3);

        let miss: StratLru = fetch(2u32, || StratLru { id: 2, _val: 999 });
        assert_eq!(miss._val, 999);
    }

    #[test]
    fn test_mru_eviction() {
        fetch::<StratMru, _>(1u32, || StratMru { id: 1, _val: 10 });
        fetch::<StratMru, _>(2u32, || StratMru { id: 2, _val: 20 });
        fetch::<StratMru, _>(3u32, || StratMru { id: 3, _val: 30 });
        assert_eq!(imc_len::<StratMru>(), 3);

        // Touch id=1, making it most-recent → 4 evicts 1
        fetch::<StratMru, _>(1u32, || StratMru { id: 1, _val: 999 });
        fetch::<StratMru, _>(4u32, || StratMru { id: 4, _val: 40 });
        assert_eq!(imc_len::<StratMru>(), 3);

        let miss: StratMru = fetch(1u32, || StratMru { id: 1, _val: 999 });
        assert_eq!(miss._val, 999);
    }

    #[test]
    fn test_lfu_eviction() {
        fetch::<StratLfu, _>(1u32, || StratLfu { id: 1, _val: 10 });
        fetch::<StratLfu, _>(2u32, || StratLfu { id: 2, _val: 20 });
        fetch::<StratLfu, _>(3u32, || StratLfu { id: 3, _val: 30 });
        assert_eq!(imc_len::<StratLfu>(), 3);

        // Bump frequency of 1 (×2) and 2 (×1) → id=3 is least-frequent → 4 evicts 3
        fetch::<StratLfu, _>(1u32, || StratLfu { id: 1, _val: 999 });
        fetch::<StratLfu, _>(1u32, || StratLfu { id: 1, _val: 999 });
        fetch::<StratLfu, _>(2u32, || StratLfu { id: 2, _val: 888 });
        fetch::<StratLfu, _>(4u32, || StratLfu { id: 4, _val: 40 });
        assert_eq!(imc_len::<StratLfu>(), 3);

        let miss: StratLfu = fetch(3u32, || StratLfu { id: 3, _val: 999 });
        assert_eq!(miss._val, 999);
    }

    #[test]
    fn test_mfu_eviction() {
        fetch::<StratMfu, _>(1u32, || StratMfu { id: 1, _val: 10 });
        fetch::<StratMfu, _>(2u32, || StratMfu { id: 2, _val: 20 });
        fetch::<StratMfu, _>(3u32, || StratMfu { id: 3, _val: 30 });
        assert_eq!(imc_len::<StratMfu>(), 3);

        // Bump frequency of 1 (×2) → id=1 is most-frequent → 4 evicts 1
        fetch::<StratMfu, _>(1u32, || StratMfu { id: 1, _val: 999 });
        fetch::<StratMfu, _>(1u32, || StratMfu { id: 1, _val: 999 });
        fetch::<StratMfu, _>(4u32, || StratMfu { id: 4, _val: 40 });
        assert_eq!(imc_len::<StratMfu>(), 3);

        let miss: StratMfu = fetch(1u32, || StratMfu { id: 1, _val: 999 });
        assert_eq!(miss._val, 999);
    }

    #[test]
    fn test_fifo_eviction() {
        fetch::<StratFifo, _>(1u32, || StratFifo { id: 1, _val: 10 });
        fetch::<StratFifo, _>(2u32, || StratFifo { id: 2, _val: 20 });
        fetch::<StratFifo, _>(3u32, || StratFifo { id: 3, _val: 30 });
        assert_eq!(imc_len::<StratFifo>(), 3);

        // Touch id=1 doesn't help FIFO → 4 evicts 1 (oldest insertion)
        fetch::<StratFifo, _>(1u32, || StratFifo { id: 1, _val: 999 });
        fetch::<StratFifo, _>(4u32, || StratFifo { id: 4, _val: 40 });
        assert_eq!(imc_len::<StratFifo>(), 3);

        let miss: StratFifo = fetch(1u32, || StratFifo { id: 1, _val: 999 });
        assert_eq!(miss._val, 999);
    }

    // ── Core behaviour ───────────────────────────────────────────────

    #[test]
    #[serial]
    fn test_basic_caching() {
        imc_clear::<Widget>();
        let call_count = AtomicU32::new(0);

        let r1: Widget = fetch("key1", || {
            call_count.fetch_add(1, AtomicOrdering::SeqCst);
            Widget { id: 1, label: "first".into() }
        });
        let r2: Widget = fetch("key1", || {
            call_count.fetch_add(1, AtomicOrdering::SeqCst);
            Widget { id: 1, label: "first".into() }
        });

        assert_eq!(r1.label, "first");
        assert_eq!(r2.label, "first");
        assert_eq!(call_count.load(AtomicOrdering::SeqCst), 1);
    }

    #[test]
    #[serial]
    fn test_dedup_same_id_different_args() {
        imc_clear::<Widget>();

        let r1: Widget = fetch(42u32, || Widget { id: 42, label: "Alice".into() });
        assert_eq!(r1.label, "Alice");

        let r2: Widget = fetch("alice@example.com", || Widget {
            id: 42,
            label: "WRONG".into(),
        });
        assert_eq!(r2.label, "Alice");
        assert_eq!(imc_len::<Widget>(), 1);
    }

    #[test]
    #[serial]
    fn test_separate_ids_do_not_dedup() {
        imc_clear::<Widget>();

        let r1: Widget = fetch(1u32, || Widget { id: 1, label: "one".into() });
        let r2: Widget = fetch(2u32, || Widget { id: 2, label: "two".into() });

        assert_eq!(r1.label, "one");
        assert_eq!(r2.label, "two");
        assert_eq!(imc_len::<Widget>(), 2);
    }

    #[test]
    #[serial]
    fn test_imc_len_and_clear() {
        imc_clear::<Widget>();

        assert_eq!(imc_len::<Widget>(), 0);

        fetch::<Widget, _>("a", || Widget { id: 1, label: "A".into() });
        fetch::<Widget, _>("b", || Widget { id: 2, label: "B".into() });
        assert_eq!(imc_len::<Widget>(), 2);

        imc_remove::<Widget>(&1);
        assert_eq!(imc_len::<Widget>(), 1);

        imc_clear::<Widget>();
        assert_eq!(imc_len::<Widget>(), 0);
    }

    #[test]
    #[serial]
    fn test_max_size_enforced() {
        #[derive(Clone, Debug, PartialEq)]
        struct SmallWidget { id: u32, _val: u32 }
        impl ImcCacheable for SmallWidget {
            type Id = u32;
            fn cache_id(&self) -> u32 { self.id }
            fn cache_strategy() -> CacheStrategy { CacheStrategy::Fifo }
            fn cache_max_size() -> usize { 2 }
            fn cache_ttl() -> Option<Duration> { None }
        }
        imc_clear::<SmallWidget>();

        fetch::<SmallWidget, _>(1u32, || SmallWidget { id: 1, _val: 10 });
        fetch::<SmallWidget, _>(2u32, || SmallWidget { id: 2, _val: 20 });
        assert_eq!(imc_len::<SmallWidget>(), 2);

        fetch::<SmallWidget, _>(3u32, || SmallWidget { id: 3, _val: 30 });
        assert_eq!(imc_len::<SmallWidget>(), 2);

        let miss: SmallWidget = fetch(1u32, || SmallWidget { id: 1, _val: 99 });
        assert_eq!(miss._val, 99);
    }

    // ── TTL tests ────────────────────────────────────────────────────

    #[derive(Clone, Debug, PartialEq)]
    struct TtlWidget { id: u32, val: u32 }
    impl ImcCacheable for TtlWidget {
        type Id = u32;
        fn cache_id(&self) -> u32 { self.id }
        fn cache_strategy() -> CacheStrategy { CacheStrategy::Lru }
        fn cache_max_size() -> usize { 100 }
        fn cache_ttl() -> Option<Duration> { Some(Duration::from_millis(50)) }
    }

    #[test]
    #[serial]
    fn test_ttl_expiry() {
        imc_clear::<TtlWidget>();
        let call_count = AtomicU32::new(0);

        let r1: TtlWidget = fetch(1u32, || {
            call_count.fetch_add(1, AtomicOrdering::SeqCst);
            TtlWidget { id: 1, val: 42 }
        });
        assert_eq!(r1.val, 42);
        assert_eq!(call_count.load(AtomicOrdering::SeqCst), 1);

        let r2: TtlWidget = fetch(1u32, || {
            call_count.fetch_add(1, AtomicOrdering::SeqCst);
            TtlWidget { id: 1, val: 99 }
        });
        assert_eq!(r2.val, 42);
        assert_eq!(call_count.load(AtomicOrdering::SeqCst), 1);

        std::thread::sleep(Duration::from_millis(60));

        let r3: TtlWidget = fetch(1u32, || {
            call_count.fetch_add(1, AtomicOrdering::SeqCst);
            TtlWidget { id: 1, val: 200 }
        });
        assert_eq!(r3.val, 200);
        assert_eq!(call_count.load(AtomicOrdering::SeqCst), 2);
    }

    #[derive(Clone, Debug, PartialEq)]
    struct NoTtlWidget { id: u32, val: u32 }
    impl ImcCacheable for NoTtlWidget {
        type Id = u32;
        fn cache_id(&self) -> u32 { self.id }
        fn cache_strategy() -> CacheStrategy { CacheStrategy::Lru }
        fn cache_max_size() -> usize { 100 }
        fn cache_ttl() -> Option<Duration> { None }
    }

    #[test]
    #[serial]
    fn test_ttl_no_expiry_when_none() {
        imc_clear::<NoTtlWidget>();
        let call_count = AtomicU32::new(0);

        fetch::<NoTtlWidget, _>(1u32, || {
            call_count.fetch_add(1, AtomicOrdering::SeqCst);
            NoTtlWidget { id: 1, val: 42 }
        });

        std::thread::sleep(Duration::from_millis(20));

        let r2: NoTtlWidget = fetch(1u32, || {
            call_count.fetch_add(1, AtomicOrdering::SeqCst);
            NoTtlWidget { id: 1, val: 99 }
        });
        assert_eq!(r2.val, 42);
        assert_eq!(call_count.load(AtomicOrdering::SeqCst), 1);
    }

    // ── Verify the doc examples compile ──────────────────────────────

    #[allow(dead_code)]
    fn doc_example() {
        #[derive(Clone, Debug, PartialEq)]
        struct User { id: u32, name: String }

        impl ImcCacheable for User {
            type Id = u32;
            fn cache_id(&self) -> u32 { self.id }
            fn cache_strategy() -> CacheStrategy { CacheStrategy::Lru }
            fn cache_ttl() -> Option<Duration> { Some(Duration::from_secs(300)) }
        }

        let _u: User = fetch(42u32, || User { id: 42, name: "Alice".into() });
        let _u2: User = fetch("alice@example.com", || User { id: 42, name: "Alice".into() });
    }
}

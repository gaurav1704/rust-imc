use std::any::{Any, TypeId};
use std::collections::HashMap;
#[cfg(any(feature = "async", feature = "tokio"))]
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Mutex, OnceLock, RwLock};
#[cfg(not(feature = "tokio"))]
use std::thread::JoinHandle;
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

    /// Optional: return a pub/sub channel name to enable cross-process
    /// cache invalidation for this type. Requires the `invalidation-redis`
    /// feature and a [`CacheWorker`] with
    /// [`WorkerConfig::redis_connection_string`] configured.
    fn cache_invalidation_channel() -> Option<&'static str> { None }
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
// Background worker command
// ---------------------------------------------------------------------------

/// Commands that can be sent to the background maintenance worker.
enum CacheCmd {
    Remove(TypeId, u64),
    Clear(TypeId),
    ClearAll,
    Shutdown,
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
        #[cfg(feature = "invalidation-redis")]
        invalidation::register::<T>();
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

        // ── 3.  Evict if full (skip when background worker is active) ─
        if self.data.len() >= self.max_size
            && WORKER_TX.lock().unwrap_or_else(|e| e.into_inner()).is_none()
        {
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

    /// Remove all TTL-expired entries and their stale index references.
    fn remove_expired(&mut self) {
        self.data.retain(|_, e| {
            e.ttl.map_or(true, |ttl| e.created_at.elapsed() <= ttl)
        });
        self.index.retain(|_, id_hash| self.data.contains_key(id_hash));
    }

    /// Evict entries until `data.len() <= max_size` and clean up orphaned
    /// index entries.
    fn evict_to_max_size(&mut self) {
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

/// Deterministic FNV-1a hasher so the same value produces the same hash
/// across processes (required for cross-process invalidation).
struct StableHasher(u64);

impl StableHasher {
    fn new() -> Self {
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

fn hash_value(v: impl Hash) -> u64 {
    let mut h = StableHasher::new();
    v.hash(&mut h);
    h.finish()
}

// ---------------------------------------------------------------------------
// Background worker — offloads eviction, expiry, and invalidation
// ---------------------------------------------------------------------------

/// Global sender that signals whether a background worker is active.
static WORKER_TX: Mutex<Option<Sender<CacheCmd>>> = Mutex::new(None);

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
/// let u: User = through_imc(42u32, || User { id: 42, name: "Alice".into() });
/// assert_eq!(u.name, "Alice");
///
/// // … and by email.  Both queries return the *same* backing `User` object.
/// let u2: User = through_imc("alice@example.com", || User { id: 42, name: "Alice".into() });
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

    // Fast path: read-lock check
    {
        let stores = global().stores.read().unwrap();
        if let Some(cache) = stores.get(&type_id) {
            if let Some(value) = cache.get::<T>(args_hash) {
                return value;
            }
        }
    }

    // Miss – compute the value
    let value = f();
    let id = value.cache_id();
    let id_hash = hash_value(&id);
    let ttl = T::cache_ttl();

    // Write-lock to store (deduplicates internally)
    let mut stores = global().stores.write().unwrap();
    let cache = stores
        .entry(type_id)
        .or_insert_with(|| PerTypeCache::from_trait::<T>());
    cache.set::<T>(args_hash, id_hash, value, ttl);

    // Re-read to return the canonical copy (in case dedup kicked in)
    cache
        .get::<T>(args_hash)
        .expect("value was just stored")
}

/// Async version of [`through_imc`].
///
/// Requires the `async` or `tokio` feature.
#[cfg(any(feature = "async", feature = "tokio"))]
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
// Background worker — public API
// ---------------------------------------------------------------------------

/// Platform-agnostic join handle: `std::thread::JoinHandle` or
/// `tokio::task::JoinHandle` depending on the `tokio` feature.
#[cfg(not(feature = "tokio"))]
type WorkerJoinHandle = JoinHandle<()>;
#[cfg(feature = "tokio")]
type WorkerJoinHandle = tokio::task::JoinHandle<()>;

/// Spawn a long-lived background thread (or tokio blocking-task when the
/// `tokio` feature is active).
#[cfg(not(feature = "tokio"))]
fn spawn_worker_thread(name: &'static str, f: impl FnOnce() + Send + 'static) -> WorkerJoinHandle {
    std::thread::Builder::new()
        .name(name.into())
        .spawn(f)
        .expect("failed to spawn imc worker thread")
}
#[cfg(feature = "tokio")]
fn spawn_worker_thread(_name: &'static str, f: impl FnOnce() + Send + 'static) -> WorkerJoinHandle {
    tokio::task::spawn_blocking(f)
}

/// Configuration for the background maintenance worker.
#[derive(Clone, Debug)]
pub struct WorkerConfig {
    /// How often the background thread sweeps for expired & excess entries.
    pub sweep_interval: Duration,
    /// Optional Redis connection string for cross-process cache invalidation
    /// via pub/sub. Requires the `invalidation-redis` feature.
    #[cfg(feature = "invalidation-redis")]
    pub redis_connection_string: Option<String>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            sweep_interval: Duration::from_secs(10),
            #[cfg(feature = "invalidation-redis")]
            redis_connection_string: None,
        }
    }
}

/// A background worker that periodically sweeps the cache for TTL-expired
/// entries and evicts over-capacity namespaces, and processes
/// remove/clear/shutdown commands.
///
/// When a worker is active the hot-path `through_imc` / `through_imc_async`
/// skip inline eviction so that the main thread is never blocked by an O(n)
/// eviction scan.
pub struct CacheWorker {
    tx: Sender<CacheCmd>,
    _handle: WorkerJoinHandle,
    #[cfg(feature = "invalidation-redis")]
    _redis_handle: Option<WorkerJoinHandle>,
}

impl CacheWorker {
    /// Spawn a worker with default [`WorkerConfig`].
    pub fn spawn() -> Self {
        Self::spawn_with_config(WorkerConfig::default())
    }

    /// Spawn a worker with a custom configuration.
    ///
    /// # Panics
    ///
    /// Panics if a worker is already running (only one instance is allowed).
    pub fn spawn_with_config(config: WorkerConfig) -> Self {
        let (tx, rx) = mpsc::channel();

        let mut guard = WORKER_TX.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            guard.is_none(),
            "an imc background worker is already running"
        );
        *guard = Some(tx.clone());

        let cfg_for_loop = WorkerConfig {
            sweep_interval: config.sweep_interval,
            #[cfg(feature = "invalidation-redis")]
            redis_connection_string: config.redis_connection_string.clone(),
        };

        let handle = spawn_worker_thread("imc-worker", move || worker_loop(rx, cfg_for_loop));

        #[cfg(feature = "invalidation-redis")]
        let redis_handle = if let Some(ref redis_url) = config.redis_connection_string {
            let channels = invalidation::snapshot_channels();
            if !channels.is_empty() {
                let url = redis_url.clone();
                Some(spawn_worker_thread("imc-redis", move || {
                    invalidation::redis_subscriber_loop(&url, channels)
                }))
            } else {
                None
            }
        } else {
            None
        };

        Self {
            tx,
            _handle: handle,
            #[cfg(feature = "invalidation-redis")]
            _redis_handle: redis_handle,
        }
    }

    /// Enqueue a remove-command for the given type and id.
    pub fn remove<T: ImcCacheable>(&self, id: &T::Id) {
        let type_id = TypeId::of::<T>();
        let id_hash = hash_value(id);
        let _ = self.tx.send(CacheCmd::Remove(type_id, id_hash));
    }

    /// Enqueue a clear-command for the given type.
    pub fn clear<T: ImcCacheable>(&self) {
        let type_id = TypeId::of::<T>();
        let _ = self.tx.send(CacheCmd::Clear(type_id));
    }

    /// Enqueue a clear-all command (evicts every namespace).
    pub fn clear_all(&self) {
        let _ = self.tx.send(CacheCmd::ClearAll);
    }
}

impl Drop for CacheWorker {
    fn drop(&mut self) {
        // Clear the global marker so inline eviction resumes.
        *WORKER_TX.lock().unwrap_or_else(|e| e.into_inner()) = None;
        // Signal shutdown so the background thread joins promptly.
        let _ = self.tx.send(CacheCmd::Shutdown);
    }
}

fn worker_loop(rx: Receiver<CacheCmd>, config: WorkerConfig) {
    let sweep_interval = config.sweep_interval;

    loop {
        match rx.recv_timeout(sweep_interval) {
            Ok(cmd) => match cmd {
                CacheCmd::Remove(type_id, id_hash) => {
                    let mut stores = global().stores.write().unwrap();
                    if let Some(cache) = stores.get_mut(&type_id) {
                        cache.remove_data(id_hash);
                    }
                }
                CacheCmd::Clear(type_id) => {
                    let mut stores = global().stores.write().unwrap();
                    if let Some(cache) = stores.get_mut(&type_id) {
                        cache.clear();
                    }
                }
                CacheCmd::ClearAll => {
                    let mut stores = global().stores.write().unwrap();
                    stores.clear();
                }
                CacheCmd::Shutdown => return,
            },
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
        }

        sweep_all();
    }
}

fn sweep_all() {
    let type_ids: Vec<TypeId> = {
        let stores = global().stores.read().unwrap();
        stores.keys().copied().collect()
    };

    for type_id in type_ids {
        let mut stores = global().stores.write().unwrap();
        if let Some(cache) = stores.get_mut(&type_id) {
            cache.remove_expired();
            cache.evict_to_max_size();
        }
    }
}

// ---------------------------------------------------------------------------
// Cross-process invalidation (behind optional feature)
// ---------------------------------------------------------------------------

/// Compute the stable hash string that other pods must publish to
/// invalidate this cache entry via Redis pub/sub.
///
/// Only needed when the `invalidation-redis` feature is enabled **and**
/// [`ImcCacheable::cache_invalidation_channel`] returns a channel name.
///
/// # Example
///
/// ```ignore
/// let inval_id = imc_invalidation_id::<User>(&42);
/// // publish `inval_id` on User's channel when User 42 is mutated
/// redis_publish("users", inval_id);
/// ```
pub fn imc_invalidation_id<T: ImcCacheable>(id: &T::Id) -> String {
    let id_hash = hash_value(id);
    id_hash.to_string()
}

#[cfg(feature = "invalidation-redis")]
pub(crate) mod invalidation {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct Registry {
        channels: HashMap<String, TypeId>,
    }

    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();

    fn registry() -> &'static Mutex<Registry> {
        REGISTRY.get_or_init(|| Mutex::new(Registry { channels: HashMap::new() }))
    }

    pub(crate) fn register<T: ImcCacheable>() {
        let channel = match T::cache_invalidation_channel() {
            Some(c) => c,
            None => return,
        };
        let mut reg = registry().lock().unwrap();
        reg.channels.entry(channel.to_string()).or_insert(TypeId::of::<T>());
    }

    pub(crate) fn snapshot_channels() -> Vec<(String, TypeId)> {
        let reg = registry().lock().unwrap();
        reg.channels.iter().map(|(c, t)| (c.clone(), *t)).collect()
    }

    pub(crate) fn redis_subscriber_loop(redis_url: &str, channels: Vec<(String, TypeId)>) {
        let chan_map: HashMap<String, TypeId> = channels.into_iter().collect();
        let client = match redis::Client::open(redis_url) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("imc: failed to connect to Redis: {e}");
                return;
            }
        };

        loop {
            if WORKER_TX.lock().unwrap_or_else(|e| e.into_inner()).is_none() {
                break;
            }
            match run_subscriber(&client, &chan_map) {
                Ok(()) => break,
                Err(e) => {
                    eprintln!("imc: Redis subscriber error: {e}, reconnecting in 5s");
                    std::thread::sleep(Duration::from_secs(5));
                }
            }
        }
    }

    fn run_subscriber(
        client: &redis::Client,
        chan_map: &HashMap<String, TypeId>,
    ) -> redis::RedisResult<()> {
        let mut conn = client.get_connection()?;
        conn.set_read_timeout(Some(Duration::from_secs(10)))?;
        let mut pubsub = conn.as_pubsub();

        for channel in chan_map.keys() {
            pubsub.subscribe(channel.as_str())?;
        }

        loop {
            if WORKER_TX.lock().unwrap_or_else(|e| e.into_inner()).is_none() {
                break;
            }

            match pubsub.get_message() {
                Ok(msg) => {
                    let channel = msg.get_channel_name();
                    let payload: String = msg.get_payload()?;

                    if let Ok(id_hash) = payload.parse::<u64>() {
                        if let Some(&type_id) = chan_map.get(channel) {
                            let mut stores = global().stores.write().unwrap();
                            if let Some(cache) = stores.get_mut(&type_id) {
                                cache.remove_data(id_hash);
                            }
                        }
                    }
                }
                Err(e) => {
                    if e.is_timeout() {
                        continue;
                    }
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

    /// Enter a tokio runtime context when the `tokio` feature is active.
    /// Declared at the start of any test that calls `CacheWorker::spawn`.
    macro_rules! enter_tokio {
        () => {
            #[cfg(feature = "tokio")]
            let _tokio_rt = tokio::runtime::Runtime::new().unwrap();
            #[cfg(feature = "tokio")]
            let _tokio_guard = _tokio_rt.enter();
        };
    }

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

    // ── Worker tests ────────────────────────────────────────────────
    //
    // Each test uses its own struct + a short sweep interval.

    macro_rules! worker_strat_def {
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
    worker_strat_def!(WkrLru, CacheStrategy::Lru);
    worker_strat_def!(WkrFifo, CacheStrategy::Fifo);

    #[test]
    #[serial]
    fn test_worker_skips_inline_eviction() {
        enter_tokio!();
        imc_clear::<WkrLru>();

        let _worker = CacheWorker::spawn_with_config(WorkerConfig {
            sweep_interval: Duration::from_secs(1),
            #[cfg(feature = "invalidation-redis")]
            redis_connection_string: None,
        });

        // max_size = 3 → 4 entries exceed the limit
        fetch::<WkrLru, _>(1u32, || WkrLru { id: 1, _val: 10 });
        fetch::<WkrLru, _>(2u32, || WkrLru { id: 2, _val: 20 });
        fetch::<WkrLru, _>(3u32, || WkrLru { id: 3, _val: 30 });
        fetch::<WkrLru, _>(4u32, || WkrLru { id: 4, _val: 40 });

        // Inline eviction was skipped → 4 entries
        assert_eq!(imc_len::<WkrLru>(), 4);
    }

    #[test]
    #[serial]
    fn test_worker_eviction_sweep() {
        enter_tokio!();
        imc_clear::<WkrFifo>();

        let worker = CacheWorker::spawn_with_config(WorkerConfig {
            sweep_interval: Duration::from_secs(1),
            #[cfg(feature = "invalidation-redis")]
            redis_connection_string: None,
        });

        fetch::<WkrFifo, _>(1u32, || WkrFifo { id: 1, _val: 10 });
        fetch::<WkrFifo, _>(2u32, || WkrFifo { id: 2, _val: 20 });
        fetch::<WkrFifo, _>(3u32, || WkrFifo { id: 3, _val: 30 });
        fetch::<WkrFifo, _>(4u32, || WkrFifo { id: 4, _val: 40 });
        assert_eq!(imc_len::<WkrFifo>(), 4);

        // Send a dummy command to trigger an immediate sweep
        worker.remove::<WkrFifo>(&999);

        // Give the worker a moment to process (sweep will evict to 3)
        for _ in 0..50 {
            if imc_len::<WkrFifo>() <= 3 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(imc_len::<WkrFifo>(), 3);
    }

    #[test]
    #[serial]
    fn test_worker_inline_eviction_resumes_after_drop() {
        enter_tokio!();
        imc_clear::<WkrFifo>();

        let worker = CacheWorker::spawn_with_config(WorkerConfig {
            sweep_interval: Duration::from_secs(1),
            #[cfg(feature = "invalidation-redis")]
            redis_connection_string: None,
        });

        // Fill past max_size — inline eviction is skipped
        fetch::<WkrFifo, _>(10u32, || WkrFifo { id: 10, _val: 100 });
        fetch::<WkrFifo, _>(20u32, || WkrFifo { id: 20, _val: 200 });
        fetch::<WkrFifo, _>(30u32, || WkrFifo { id: 30, _val: 300 });
        fetch::<WkrFifo, _>(40u32, || WkrFifo { id: 40, _val: 400 });
        assert_eq!(imc_len::<WkrFifo>(), 4);

        // Drop the worker → global marker cleared
        drop(worker);

        // Inline eviction fires on each insert (4 → evict 1 → 3 → insert → 4)
        fetch::<WkrFifo, _>(50u32, || WkrFifo { id: 50, _val: 500 });
        assert_eq!(imc_len::<WkrFifo>(), 4);

        // A second insert evicts another, confirming eviction is active
        fetch::<WkrFifo, _>(60u32, || WkrFifo { id: 60, _val: 600 });
        assert_eq!(imc_len::<WkrFifo>(), 4);
    }

    #[test]
    #[serial]
    fn test_worker_remove_via_command() {
        enter_tokio!();
        imc_clear::<WkrLru>();

        let worker = CacheWorker::spawn_with_config(WorkerConfig {
            sweep_interval: Duration::from_secs(1),
            #[cfg(feature = "invalidation-redis")]
            redis_connection_string: None,
        });

        fetch::<WkrLru, _>(1u32, || WkrLru { id: 1, _val: 10 });
        fetch::<WkrLru, _>(2u32, || WkrLru { id: 2, _val: 20 });
        assert_eq!(imc_len::<WkrLru>(), 2);

        worker.remove::<WkrLru>(&1);

        for _ in 0..50 {
            if imc_len::<WkrLru>() == 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(imc_len::<WkrLru>(), 1);

        // Verify the removed entry actually misses
        let miss: WkrLru = fetch(1u32, || WkrLru { id: 1, _val: 999 });
        assert_eq!(miss._val, 999);
    }

    // ── Invalidation tests ──────────────────────────────────────────

    #[derive(Clone, Debug, PartialEq)]
    struct InvalWidget { id: u32, name: String }

    impl ImcCacheable for InvalWidget {
        type Id = u32;
        fn cache_id(&self) -> u32 { self.id }
        fn cache_strategy() -> CacheStrategy { CacheStrategy::Lru }
        fn cache_max_size() -> usize { 100 }
        fn cache_ttl() -> Option<Duration> { None }
        fn cache_invalidation_channel() -> Option<&'static str> { Some("inval_widget") }
    }

    #[test]
    fn test_imc_invalidation_id_is_deterministic() {
        let a = imc_invalidation_id::<InvalWidget>(&42);
        let b = imc_invalidation_id::<InvalWidget>(&42);
        assert_eq!(a, b);
    }

    #[test]
    fn test_imc_invalidation_id_differs_for_diff_ids() {
        let a = imc_invalidation_id::<InvalWidget>(&1);
        let b = imc_invalidation_id::<InvalWidget>(&2);
        assert_ne!(a, b);
    }

    #[test]
    #[cfg(feature = "invalidation-redis")]
    fn test_invalidation_registry() {
        // InvalWidget was defined above, so it should have been registered
        // when PerTypeCache::from_trait::<InvalWidget>() runs.
        // Trigger first-use:
        imc_clear::<InvalWidget>();
        let _ = fetch::<InvalWidget, _>(99u32, || InvalWidget { id: 99, name: "x".into() });

        let channels = invalidation::snapshot_channels();
        assert!(channels.iter().any(|(c, _)| c == "inval_widget"));
    }
}

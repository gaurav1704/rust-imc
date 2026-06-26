use std::any::TypeId;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Mutex;
#[cfg(not(feature = "tokio"))]
use std::thread::JoinHandle;
use std::time::Duration;
#[cfg(feature = "critical")]
use std::sync::OnceLock;

use crate::cache::global;
use crate::traits::ImcCacheable;

/// Global Redis URL used by the critical-key publisher thread.
#[cfg(feature = "critical")]
static REDIS_URL: OnceLock<String> = OnceLock::new();

#[cfg(feature = "critical")]
pub(crate) fn get_redis_url() -> Option<&'static str> {
    REDIS_URL.get().map(|s| s.as_str())
}

// ---------------------------------------------------------------------------
// Background worker command
// ---------------------------------------------------------------------------

/// Commands that can be sent to the background worker thread.
///
/// Construct values via [`CacheWorker::remove`] or send them directly
/// with [`CacheWorker::send`].
pub enum CacheCmd {
    Remove(TypeId, u64),
    Clear(TypeId),
    ClearAll,
    Shutdown,
}

// ---------------------------------------------------------------------------
// Platform-agnostic thread/task handle
// ---------------------------------------------------------------------------

#[cfg(not(feature = "tokio"))]
type WorkerJoinHandle = JoinHandle<()>;
#[cfg(feature = "tokio")]
type WorkerJoinHandle = tokio::task::JoinHandle<()>;

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

// ---------------------------------------------------------------------------
// Global worker-active marker
// ---------------------------------------------------------------------------

pub(crate) static WORKER_TX: Mutex<Option<Sender<CacheCmd>>> = Mutex::new(None);

// ---------------------------------------------------------------------------
// Worker configuration
// ---------------------------------------------------------------------------

/// Configuration for the background maintenance worker.
///
/// Pass this to [`CacheWorker::spawn_with_config`] to start periodic
/// cache maintenance, cross-process invalidation, and Prometheus
/// metrics serving.
///
/// # Example (default — no Redis, no metrics)
///
/// ```rust,no_run
/// use imc::CacheWorker;
///
/// let _worker = CacheWorker::spawn_with_config(Default::default());
/// ```
///
/// # Example (with Redis invalidation + Prometheus metrics)
///
/// ```rust,no_run
/// use std::time::Duration;
/// use imc::{CacheWorker, WorkerConfig};
///
/// let _worker = CacheWorker::spawn_with_config(WorkerConfig {
///     sweep_interval: Duration::from_secs(30),
///     #[cfg(feature = "invalidation-redis")]
///     redis_connection_string: Some("redis://127.0.0.1:6379".into()),
///     #[cfg(feature = "metrics-prometheus")]
///     metrics_listen_addr: Some("127.0.0.1:9090".into()),
/// });
/// ```
#[derive(Clone, Debug)]
pub struct WorkerConfig {
    /// How often the background thread sweeps for expired & excess entries.
    pub sweep_interval: Duration,
    /// Optional listen address for the Prometheus metrics HTTP endpoint
    /// (e.g. `"127.0.0.1:9090"`).
    #[cfg(feature = "metrics-prometheus")]
    #[cfg_attr(docsrs, doc(cfg(feature = "metrics-prometheus")))]
    pub metrics_listen_addr: Option<String>,
    /// Optional Redis connection string for cross-process cache invalidation
    /// via pub/sub.
    #[cfg(feature = "invalidation-redis")]
    #[cfg_attr(docsrs, doc(cfg(feature = "invalidation-redis")))]
    pub redis_connection_string: Option<String>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            sweep_interval: Duration::from_secs(10),
            #[cfg(feature = "metrics-prometheus")]
            metrics_listen_addr: None,
            #[cfg(feature = "invalidation-redis")]
            redis_connection_string: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Worker handle
// ---------------------------------------------------------------------------

/// A background worker that periodically sweeps the cache for TTL-expired
/// entries and evicts over-capacity namespaces, and processes
/// remove/clear/shutdown commands.
///
/// When a worker is active the hot-path [`through_imc`](crate::through_imc)
/// and `through_imc_async` skip inline eviction
/// so that the main thread is never blocked by an O(n) eviction scan.
///
/// # Lifecycle
///
/// - **Spawn** the worker with [`CacheWorker::spawn`] or
///   [`CacheWorker::spawn_with_config`].
/// - **Keep alive** for the duration of your application.  The background
///   thread will shut down when the [`CacheWorker`] is dropped.
/// - Only **one worker** may exist at a time — spawning a second worker
///   while one is running will panic.
///
/// # Features
///
/// | Feature | Effect on worker |
/// |---------|------------------|
/// | `invalidation-redis` | Starts a Redis subscriber thread for invalidation messages |
/// | `critical` | Also starts a critical-key subscriber thread |
/// | `metrics-prometheus` | Starts an HTTP server on the configured address |
/// | `tokio` | Uses `tokio::task::spawn_blocking` instead of `std::thread` |
///
/// # Example
///
/// ```rust,no_run
/// use std::time::Duration;
/// use imc::{CacheWorker, WorkerConfig};
///
/// let _worker = CacheWorker::spawn_with_config(WorkerConfig {
///     sweep_interval: Duration::from_secs(30),
///     #[cfg(feature = "metrics-prometheus")]
///     metrics_listen_addr: Some("127.0.0.1:9090".into()),
///     #[cfg(feature = "invalidation-redis")]
///     redis_connection_string: Some("redis://127.0.0.1:6379".into()),
/// });
/// // _worker must not be dropped until shutdown
/// ```
pub struct CacheWorker {
    tx: Sender<CacheCmd>,
    _handle: WorkerJoinHandle,
    #[cfg(feature = "invalidation-redis")]
    _invalidation_handle: Option<std::thread::JoinHandle<()>>,
    #[cfg(feature = "critical")]
    _critical_handle: Option<std::thread::JoinHandle<()>>,
}

impl CacheWorker {
    /// Spawn a worker with default configuration.
    ///
    /// Equivalent to `CacheWorker::spawn_with_config(WorkerConfig::default())`.
    ///
    /// # Panics
    ///
    /// Panics if a worker is already running.
    pub fn spawn() -> Self {
        Self::spawn_with_config(WorkerConfig::default())
    }

    /// Spawn a background worker with the given configuration.
    ///
    /// The worker runs a command-and-sweep loop.  While it is alive:
    ///
    /// 1. Inline eviction in [`through_imc`](crate::through_imc) is
    ///    **skipped** — the sweep handles eviction, keeping the hot
    ///    path lock-free.
    /// 2. TTL-expired entries are removed every `sweep_interval`.
    /// 3. If `redis_connection_string` is set and `invalidation-redis`
    ///    is enabled, a subscriber thread listens for invalidation
    ///    messages.
    /// 4. If `metrics_listen_addr` is set and `metrics-prometheus` is
    ///    enabled, an HTTP server exposes `/metrics`.
    ///
    /// # Panics
    ///
    /// Panics if a worker is already running.
    pub fn spawn_with_config(config: WorkerConfig) -> Self {
        let (tx, rx): (Sender<CacheCmd>, Receiver<CacheCmd>) = mpsc::channel();

        {
            let mut guard = WORKER_TX.lock().unwrap_or_else(|e| e.into_inner());
            assert!(guard.is_none(), "only one imc worker may run at a time");
            *guard = Some(tx.clone());
        }

        #[cfg(feature = "invalidation-redis")]
        let invalidation_handle: Option<std::thread::JoinHandle<()>> =
            if let Some(ref redis_url) = config.redis_connection_string {
                let channels = crate::invalidation::snapshot_channels();
                let url = redis_url.clone();
                Some(
                    std::thread::Builder::new()
                        .name("imc-invalidation".into())
                        .spawn(move || crate::invalidation::redis_subscriber_loop(&url, channels))
                        .expect("failed to spawn invalidation thread"),
                )
            } else {
                None
            };

        #[cfg(feature = "critical")]
        let critical_handle: Option<std::thread::JoinHandle<()>> =
            if let Some(ref redis_url) = config.redis_connection_string {
                REDIS_URL.set(redis_url.clone()).ok();
                let url = redis_url.clone();
                Some(
                    std::thread::Builder::new()
                        .name("imc-critical".into())
                        .spawn(move || crate::critical::subscriber_loop(&url))
                        .expect("failed to spawn critical subscriber thread"),
                )
            } else {
                None
            };

        let _handle = spawn_worker_thread("imc-worker", move || {
            crate::log_event!(INFO, crate::log::WORKER, crate::log::START,
                "imc worker started");
            worker_loop(rx, config);
            crate::log_event!(INFO, crate::log::WORKER, crate::log::STOP,
                "imc worker stopped");
        });

        Self {
            tx,
            _handle,
            #[cfg(feature = "invalidation-redis")]
            _invalidation_handle: invalidation_handle,
            #[cfg(feature = "critical")]
            _critical_handle: critical_handle,
        }
    }

    /// Queue a remove-by-id command (non-blocking).
    ///
    /// The worker thread will remove the entry matching `id` during its
    /// next command cycle.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use imc::{CacheWorker, ImcCacheable};
    ///
    /// #[derive(Clone)]
    /// struct User { id: u32 }
    /// impl ImcCacheable for User {
    ///     type Id = u32; type Key = String;
    ///     fn cache_id(&self) -> u32 { self.id }
    /// }
    ///
    /// let worker = CacheWorker::spawn();
    /// let _ = worker.remove::<User>(&42);
    /// ```
    pub fn remove<T: ImcCacheable>(&self, id: &T::Id) -> Result<(), mpsc::SendError<CacheCmd>> {
        let id_hash = crate::hasher::hash_value(id);
        self.tx.send(CacheCmd::Remove(TypeId::of::<T>(), id_hash))
    }

    /// Send a command to the worker thread (non-blocking).
    ///
    /// Returns `Err` when the worker has already shut down.
    pub fn send(&self, cmd: CacheCmd) -> Result<(), mpsc::SendError<CacheCmd>> {
        self.tx.send(cmd)
    }
}

impl Drop for CacheWorker {
    fn drop(&mut self) {
        let _ = self.tx.send(CacheCmd::Shutdown);
        *WORKER_TX.lock().unwrap_or_else(|e| e.into_inner()) = None;
        crate::metrics::set_entries(0);
    }
}

// ---------------------------------------------------------------------------
// Worker loop
// ---------------------------------------------------------------------------

fn worker_loop(rx: Receiver<CacheCmd>, config: WorkerConfig) {
    loop {
        let result = rx.recv_timeout(config.sweep_interval);

        match result {
            Ok(CacheCmd::Remove(type_id, id_hash)) => {
                let mut stores = global().stores.write().unwrap();
                if let Some(cache) = stores.get_mut(&type_id) {
                    cache.remove_data(id_hash);
                }
            }
            Ok(CacheCmd::Clear(type_id)) => {
                let mut stores = global().stores.write().unwrap();
                if let Some(cache) = stores.get_mut(&type_id) {
                    cache.clear();
                }
            }
            Ok(CacheCmd::ClearAll) => {
                let mut stores = global().stores.write().unwrap();
                stores.clear();
            }
            Ok(CacheCmd::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }

        // Periodic sweep
        sweep_all(&global().stores);

        // Update metrics gauge
        let total: usize = {
            let stores = global().stores.read().unwrap();
            stores.values().map(|c| c.len()).sum()
        };
        crate::metrics::set_entries(total);
    }
}

fn sweep_all(
    stores: &std::sync::RwLock<std::collections::HashMap<TypeId, crate::cache::PerTypeCache>>,
) {
    let mut stores = stores.write().unwrap();
    for cache in stores.values_mut() {
        cache.remove_expired();
        cache.evict_to_max_size();
    }
}

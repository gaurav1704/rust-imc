use std::any::TypeId;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Mutex;
#[cfg(not(feature = "tokio"))]
use std::thread::JoinHandle;
use std::time::Duration;

use crate::cache::global;
use crate::hasher::hash_value;
use crate::traits::ImcCacheable;

// ---------------------------------------------------------------------------
// Background worker command
// ---------------------------------------------------------------------------

pub(crate) enum CacheCmd {
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
#[derive(Clone, Debug)]
pub struct WorkerConfig {
    /// How often the background thread sweeps for expired & excess entries.
    pub sweep_interval: Duration,
    /// Optional listen address for the Prometheus metrics HTTP endpoint
    /// (e.g. `"127.0.0.1:9090"`). Requires the `metrics-prometheus` feature.
    #[cfg(feature = "metrics-prometheus")]
    pub metrics_listen_addr: Option<String>,
    /// Optional Redis connection string for cross-process cache invalidation
    /// via pub/sub. Requires the `invalidation-redis` feature.
    #[cfg(feature = "invalidation-redis")]
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
/// When a worker is active the hot-path `through_imc` / `through_imc_async`
/// skip inline eviction so that the main thread is never blocked by an O(n)
/// eviction scan.
pub struct CacheWorker {
    tx: Sender<CacheCmd>,
    _handle: WorkerJoinHandle,
    #[cfg(feature = "invalidation-redis")]
    _redis_handle: Option<WorkerJoinHandle>,
    #[cfg(feature = "metrics-prometheus")]
    _metrics_handle: Option<WorkerJoinHandle>,
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
            #[cfg(feature = "metrics-prometheus")]
            metrics_listen_addr: config.metrics_listen_addr.clone(),
            #[cfg(feature = "invalidation-redis")]
            redis_connection_string: config.redis_connection_string.clone(),
        };

        let handle = spawn_worker_thread("imc-worker", move || worker_loop(rx, cfg_for_loop));

        #[cfg(feature = "invalidation-redis")]
        let redis_handle = if let Some(ref redis_url) = config.redis_connection_string {
            let channels = crate::invalidation::snapshot_channels();
            if !channels.is_empty() {
                let url = redis_url.clone();
                Some(spawn_worker_thread("imc-redis", move || {
                    crate::invalidation::redis_subscriber_loop(&url, channels)
                }))
            } else {
                None
            }
        } else {
            None
        };

        #[cfg(feature = "metrics-prometheus")]
        let metrics_handle = if let Some(ref addr) = config.metrics_listen_addr {
            let addr = addr.clone();
            Some(spawn_worker_thread("imc-metrics", move || {
                if let Err(e) = crate::metrics::serve(&addr) {
                    crate::log_event!(ERROR, crate::log::METRICS, crate::log::ERROR,
                        "metrics server on {} failed: {}", addr, e);
                }
            }))
        } else {
            None
        };

        crate::log_event!(INFO, crate::log::WORKER, crate::log::START,
            sweep_interval = ?config.sweep_interval);

        Self {
            tx,
            _handle: handle,
            #[cfg(feature = "invalidation-redis")]
            _redis_handle: redis_handle,
            #[cfg(feature = "metrics-prometheus")]
            _metrics_handle: metrics_handle,
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
        *WORKER_TX.lock().unwrap_or_else(|e| e.into_inner()) = None;
        let _ = self.tx.send(CacheCmd::Shutdown);
        crate::log_event!(INFO, crate::log::WORKER, crate::log::STOP);
    }
}

// ---------------------------------------------------------------------------
// Background loop
// ---------------------------------------------------------------------------

pub(crate) fn worker_loop(rx: Receiver<CacheCmd>, config: WorkerConfig) {
    let sweep_interval = config.sweep_interval;

    loop {
        match rx.recv_timeout(sweep_interval) {
            Ok(cmd) => match cmd {
                CacheCmd::Remove(type_id, id_hash) => {
                    let mut stores = global().stores.write().unwrap();
                    if let Some(cache) = stores.get_mut(&type_id) {
                        cache.remove_data(id_hash);
                        crate::log_event!(DEBUG, crate::log::WORKER, crate::log::REMOVE,
                            type_id = ?type_id, id_hash = id_hash);
                    }
                }
                CacheCmd::Clear(type_id) => {
                    let mut stores = global().stores.write().unwrap();
                    if let Some(cache) = stores.get_mut(&type_id) {
                        cache.clear();
                        crate::log_event!(INFO, crate::log::WORKER, crate::log::CLEAR,
                            type_id = ?type_id);
                    }
                }
                CacheCmd::ClearAll => {
                    let mut stores = global().stores.write().unwrap();
                    stores.clear();
                    crate::log_event!(INFO, crate::log::WORKER, crate::log::CLEAR, all = true);
                }
                CacheCmd::Shutdown => {
                    crate::log_event!(INFO, crate::log::WORKER, crate::log::STOP, reason = "shutdown");
                    return;
                }
            },
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
        }

        sweep_all();
    }
}

pub(crate) fn sweep_all() {
    let type_ids: Vec<TypeId> = {
        let stores = global().stores.read().unwrap();
        stores.keys().copied().collect()
    };

    for type_id in type_ids {
        let mut stores = global().stores.write().unwrap();
        if let Some(cache) = stores.get_mut(&type_id) {
            cache.remove_expired();
            cache.evict_to_max_size();
            crate::metrics::set_entries(cache.len());
        }
    }
}

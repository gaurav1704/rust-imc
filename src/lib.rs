mod traits;
mod hasher;
mod entry;
mod cache;
pub(crate) mod worker;
mod api;
pub(crate) mod log;
pub(crate) mod metrics;

#[cfg(feature = "invalidation-redis")]
pub(crate) mod invalidation;

#[cfg(test)]
mod tests;

pub use traits::{CacheStrategy, ImcCacheable};
pub use worker::{CacheWorker, WorkerConfig};
pub use api::{through_imc, through_imc_keyed, imc_remove, imc_clear, imc_len, imc_invalidation_id};

#[cfg(any(feature = "async", feature = "tokio"))]
pub use api::{through_imc_async, through_imc_keyed_async};

// ---------------------------------------------------------------------------
// Library lifecycle
// ---------------------------------------------------------------------------

/// Entry point for the imc cache system.
///
/// Call [`Imc::init`] or [`Imc::start`] once at application startup to
/// explicitly initialise the global cache store and optionally spawn the
/// background maintenance worker.
///
/// The cache store is a process-global singleton, so any call to
/// [`through_imc`], [`imc_remove`], etc. works automatically — even without
/// calling `init`.  However, calling `init` or `start` makes the lifecycle
/// explicit and gives you a chance to attach a [`CacheWorker`].
///
/// # Example
///
/// ```ignore
/// use imc::Imc;
///
/// // Basic initialisation (no background worker):
/// Imc::init();
///
/// // Or with a background worker:
/// let _worker = Imc::start(Default::default());
///
/// // Use the cache anywhere in the codebase:
/// let user = imc::through_imc(42u32, || fetch_user(42));
/// ```
pub struct Imc;

impl Imc {
    /// Ensure the global cache store is initialised.
    ///
    /// This is automatically called on first use of [`through_imc`] or any
    /// other cache function, but calling it explicitly documents the
    /// dependency.
    pub fn init() {
        cache::global_init();
    }

    /// Initialise the cache store **and** spawn a background maintenance
    /// worker.
    ///
    /// The returned [`CacheWorker`] **must** be kept alive for the lifetime
    /// of the application.  Dropping it shuts down the background thread.
    ///
    /// # Panics
    ///
    /// Panics if a worker is already running (only one instance is allowed).
    pub fn start(config: WorkerConfig) -> CacheWorker {
        Self::init();
        CacheWorker::spawn_with_config(config)
    }
}

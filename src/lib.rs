#![cfg_attr(docsrs, feature(doc_cfg))]

//! A trait-based, deduplicating, in-memory cache for Rust.
//!
//! One data copy per unique identity, even when the same record is fetched
//! through different query arguments.
//!
//! ```rust
//! use std::time::Duration;
//! use imc::{ImcCacheable, CacheStrategy, through_imc};
//!
//! #[derive(Clone, Debug, PartialEq)]
//! struct User { id: u32, name: String }
//!
//! impl ImcCacheable for User {
//!     type Id = u32;
//!     type Key = String;
//!     fn cache_id(&self) -> u32 { self.id }
//!     fn cache_strategy() -> CacheStrategy { CacheStrategy::Lru }
//!     fn cache_ttl() -> Option<Duration> { Some(Duration::from_secs(300)) }
//!     fn cache_max_size() -> usize { 10_000 }
//! }
//!
//! let user: User = through_imc(42u32, || User { id: 42, name: "Alice".into() });
//! let same: User = through_imc("alice@example.com", || User { id: 42, name: "Alice".into() });
//! assert_eq!(user, same);
//! ```
//!
//! ---
//!
//! # Feature flags
//!
//! | Feature | Description |
//! |---------|-------------|
//! | *(none)* | Core caching: [`through_imc`], dedup, eviction, TTL |
//! | `async` | Enables `through_imc_async` / `through_imc_keyed_async` (runtime-agnostic) |
//! | `tokio` | Implies `async` + makes [`CacheWorker`] use `tokio::task::spawn_blocking` |
//! | `invalidation-redis` | Cross-process cache invalidation via Redis pub/sub |
//! | `critical` | Critical-key broadcast via Redis pub/sub. Implies `invalidation-redis`. Requires `#[derive(CriticalKey)]` on key enums used with [`through_imc_keyed`] |
//! | `logging` | Structured tracing events via [`log_event!`] macro |
//! | `metrics-prometheus` | Prometheus counters/gauges + HTTP `/metrics` endpoint |
//!
//! ---
//!
//! # Quick start
//!
//! 1. **Add `imc` to `Cargo.toml`**
//!
//! ```toml
//! [dependencies]
//! imc = "0.1"
//! ```
//!
//! 2. **Implement [`ImcCacheable`] on your type**
//!
//! ```rust
//! use std::time::Duration;
//! use imc::{ImcCacheable, CacheStrategy};
//!
//! #[derive(Clone)]
//! struct User { id: u32, name: String }
//!
//! impl ImcCacheable for User {
//!     type Id = u32;
//!     type Key = String;
//!
//!     fn cache_id(&self) -> u32 { self.id }
//!
//!     fn cache_strategy() -> CacheStrategy { CacheStrategy::Lru }
//!     fn cache_ttl() -> Option<Duration> { Some(Duration::from_secs(300)) }
//!     fn cache_max_size() -> usize { 10_000 }
//! }
//! ```
//!
//! 3. **Cache expensive operations**
//!
//! ```rust
//! # use std::time::Duration;
//! # use imc::{ImcCacheable, CacheStrategy, through_imc};
//! # #[derive(Clone, PartialEq, Debug)]
//! # struct User { id: u32, name: String }
//! # impl ImcCacheable for User {
//! #     type Id = u32; type Key = String;
//! #     fn cache_id(&self) -> u32 { self.id }
//! #     fn cache_strategy() -> CacheStrategy { CacheStrategy::Lru }
//! #     fn cache_ttl() -> Option<Duration> { Some(Duration::from_secs(300)) }
//! #     fn cache_max_size() -> usize { 10_000 }
//! # }
//! let user: User = through_imc(42u32, || fetch_user(42));
//! let same: User = through_imc("alice@example.com", || fetch_user_by_email("alice"));
//! assert_eq!(user.id, same.id);
//! # fn fetch_user(id: u32) -> User { User { id, name: "Alice".into() } }
//! # fn fetch_user_by_email(_: &str) -> User { User { id: 42, name: "Alice".into() } }
//! ```
//!
//! # Re-exports
//!
//! The `CriticalKey` derive macro is provided by
//! the `imc-derive` crate and re-exported here for convenience:
//!
//! ```rust,ignore
//! use imc::CriticalKey;
//!
//! #[derive(Hash, Clone, CriticalKey)]
//! enum UserKey { ById(i32), ByEmail(String) }
//! ```
#[cfg(feature = "critical")]
#[cfg_attr(docsrs, doc(cfg(feature = "critical")))]
#[doc(inline)]
pub use imc_derive::CriticalKey;

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

#[cfg(feature = "critical")]
pub(crate) mod critical;

#[cfg(test)]
mod tests;

pub use traits::{CacheStrategy, ImcCacheable};
#[cfg(feature = "critical")]
#[cfg_attr(docsrs, doc(cfg(feature = "critical")))]
pub use traits::CriticalKey;
pub use worker::{CacheWorker, WorkerConfig};
pub use api::{
    imc_clear, imc_invalidation_id, imc_len, imc_remove, through_imc, through_imc_keyed,
};

#[cfg(any(feature = "async", feature = "tokio"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "async", feature = "tokio"))))]
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
/// ```rust,ignore
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

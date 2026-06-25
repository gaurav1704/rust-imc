#![allow(dead_code)]

// ---------------------------------------------------------------------------
// Component and action constants (always available, zero cost)
// ---------------------------------------------------------------------------

pub const CACHE: &str = "cache";
pub const WORKER: &str = "worker";
pub const API: &str = "api";
pub const INVALIDATION: &str = "invalidation";
pub const METRICS: &str = "metrics";
pub const CRITICAL: &str = "critical";

pub const HIT: &str = "hit";
pub const MISS: &str = "miss";
pub const SET: &str = "set";
pub const EVICT: &str = "evict";
pub const PUBLISH: &str = "publish";
pub const EXPIRY: &str = "expiry";
pub const REMOVE: &str = "remove";
pub const CLEAR: &str = "clear";
pub const DEDUP: &str = "dedup";
pub const SWEEP: &str = "sweep";
pub const START: &str = "start";
pub const STOP: &str = "stop";
pub const ERROR: &str = "error";

// ---------------------------------------------------------------------------
// Structured logging macro
// ---------------------------------------------------------------------------

/// Emit a structured tracing event when the `logging` feature is enabled.
///
/// Expands to nothing at compile time when the feature is off.
///
/// # Syntax
///
/// ```ignore
/// log_event!(INFO, CACHE, HIT, "cache hit for args_hash={}", args_hash);
/// log_event!(DEBUG, CACHE, EVICT, id_hash = id_hash, strategy = ?strategy);
/// log_event!(INFO, WORKER, STOP); // message-only defaults to component+action
/// ```
#[macro_export]
macro_rules! log_event {
    // With extra fields/message (3+ required trailing tokens)
    ($level:ident, $component:expr, $action:expr, $($arg:tt)+) => {
        #[cfg(feature = "logging")]
        tracing::event!(
            target: module_path!(),
            tracing::Level::$level,
            component = $component,
            action = $action,
            $($arg)+
        )
    };
    // Bare component+action only (no extra fields)
    ($level:ident, $component:expr, $action:expr) => {
        #[cfg(feature = "logging")]
        tracing::event!(
            target: module_path!(),
            tracing::Level::$level,
            component = $component,
            action = $action,
        )
    };
}

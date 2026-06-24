use std::hash::Hash;
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
use std::time::Duration;

use serial_test::serial;

use crate::*;

/// Enter a tokio runtime context when the `tokio` feature is active.
macro_rules! enter_tokio {
    () => {
        #[cfg(feature = "tokio")]
        let _tokio_rt = tokio::runtime::Runtime::new().unwrap();
        #[cfg(feature = "tokio")]
        let _tokio_guard = _tokio_rt.enter();
    };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Strategy tests — each uses its own struct (unique TypeId → no interference)
// ---------------------------------------------------------------------------

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

    fetch::<StratFifo, _>(1u32, || StratFifo { id: 1, _val: 999 });
    fetch::<StratFifo, _>(4u32, || StratFifo { id: 4, _val: 40 });
    assert_eq!(imc_len::<StratFifo>(), 3);

    let miss: StratFifo = fetch(1u32, || StratFifo { id: 1, _val: 999 });
    assert_eq!(miss._val, 999);
}

// ---------------------------------------------------------------------------
// Core behaviour — uses Widget (TypeId clash → #[serial])
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// TTL tests
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Verify the doc examples compile
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Worker tests
// ---------------------------------------------------------------------------

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
        ..Default::default()
    });

    fetch::<WkrLru, _>(1u32, || WkrLru { id: 1, _val: 10 });
    fetch::<WkrLru, _>(2u32, || WkrLru { id: 2, _val: 20 });
    fetch::<WkrLru, _>(3u32, || WkrLru { id: 3, _val: 30 });
    fetch::<WkrLru, _>(4u32, || WkrLru { id: 4, _val: 40 });

    assert_eq!(imc_len::<WkrLru>(), 4);
}

#[test]
#[serial]
fn test_worker_eviction_sweep() {
    enter_tokio!();
    imc_clear::<WkrFifo>();

    let worker = CacheWorker::spawn_with_config(WorkerConfig {
        sweep_interval: Duration::from_secs(1),
        ..Default::default()
    });

    fetch::<WkrFifo, _>(1u32, || WkrFifo { id: 1, _val: 10 });
    fetch::<WkrFifo, _>(2u32, || WkrFifo { id: 2, _val: 20 });
    fetch::<WkrFifo, _>(3u32, || WkrFifo { id: 3, _val: 30 });
    fetch::<WkrFifo, _>(4u32, || WkrFifo { id: 4, _val: 40 });
    assert_eq!(imc_len::<WkrFifo>(), 4);

    worker.remove::<WkrFifo>(&999);

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
        ..Default::default()
    });

    fetch::<WkrFifo, _>(10u32, || WkrFifo { id: 10, _val: 100 });
    fetch::<WkrFifo, _>(20u32, || WkrFifo { id: 20, _val: 200 });
    fetch::<WkrFifo, _>(30u32, || WkrFifo { id: 30, _val: 300 });
    fetch::<WkrFifo, _>(40u32, || WkrFifo { id: 40, _val: 400 });
    assert_eq!(imc_len::<WkrFifo>(), 4);

    drop(worker);

    fetch::<WkrFifo, _>(50u32, || WkrFifo { id: 50, _val: 500 });
    assert_eq!(imc_len::<WkrFifo>(), 4);

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
        ..Default::default()
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

    let miss: WkrLru = fetch(1u32, || WkrLru { id: 1, _val: 999 });
    assert_eq!(miss._val, 999);
}

// ---------------------------------------------------------------------------
// Invalidation tests
// ---------------------------------------------------------------------------

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
    imc_clear::<InvalWidget>();
    let _ = fetch::<InvalWidget, _>(99u32, || InvalWidget { id: 99, name: "x".into() });

    let channels = crate::invalidation::snapshot_channels();
    assert!(channels.iter().any(|(c, _)| c == "inval_widget"));
}

// ---------------------------------------------------------------------------
// Prometheus metrics – behind the `metrics-prometheus` feature flag
// ---------------------------------------------------------------------------

#[cfg(feature = "metrics-prometheus")]
pub(crate) mod inner {
    use prometheus::{Counter, Gauge, Registry};
    use std::sync::OnceLock;

    struct Metrics {
        hits: Counter,
        misses: Counter,
        sets: Counter,
        evictions: Counter,
        expired: Counter,
        entries: Gauge,
        registry: Registry,
    }

    fn metrics() -> &'static Metrics {
        static M: OnceLock<Metrics> = OnceLock::new();
        M.get_or_init(|| {
            let registry = Registry::new();

            let hits = Counter::new("imc_cache_hits_total", "Total number of cache hits")
                .expect("metric imc_cache_hits_total");
            let misses =
                Counter::new("imc_cache_misses_total", "Total number of cache misses")
                    .expect("metric imc_cache_misses_total");
            let sets = Counter::new("imc_cache_sets_total", "Total number of cache sets")
                .expect("metric imc_cache_sets_total");
            let evictions =
                Counter::new("imc_cache_evictions_total", "Total number of cache evictions")
                    .expect("metric imc_cache_evictions_total");
            let expired = Counter::new(
                "imc_cache_expired_total",
                "Total number of expired entries removed",
            )
            .expect("metric imc_cache_expired_total");
            let entries =
                Gauge::new("imc_cache_entries", "Current number of cached entries")
                    .expect("metric imc_cache_entries");

            registry
                .register(Box::new(hits.clone()))
                .expect("register hits");
            registry
                .register(Box::new(misses.clone()))
                .expect("register misses");
            registry
                .register(Box::new(sets.clone()))
                .expect("register sets");
            registry
                .register(Box::new(evictions.clone()))
                .expect("register evictions");
            registry
                .register(Box::new(expired.clone()))
                .expect("register expired");
            registry
                .register(Box::new(entries.clone()))
                .expect("register entries");

            Metrics { hits, misses, sets, evictions, expired, entries, registry }
        })
    }

    pub fn record_hit() {
        metrics().hits.inc();
    }

    pub fn record_miss() {
        metrics().misses.inc();
    }

    pub fn record_set() {
        metrics().sets.inc();
    }

    pub fn record_eviction() {
        metrics().evictions.inc();
    }

    pub fn record_expired(count: u64) {
        metrics().expired.inc_by(count as f64);
    }

    pub fn set_entries(count: usize) {
        metrics().entries.set(count as f64);
    }

    /// Push all metrics to a Prometheus Push Gateway.
    ///
    /// ```ignore
    /// metrics::push("http://pushgateway:9091").unwrap();
    /// ```
    pub fn push(gateway_url: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let metric_families = metrics().registry.gather();
        let labels = std::collections::HashMap::<String, String>::new();
        prometheus::push_metrics("imc", labels, gateway_url, metric_families, None)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// No-op stubs when the feature is disabled
// ---------------------------------------------------------------------------

#[cfg(not(feature = "metrics-prometheus"))]
pub(crate) mod inner {
    pub fn record_hit() {}
    pub fn record_miss() {}
    pub fn record_set() {}
    pub fn record_eviction() {}
    pub fn record_expired(_count: u64) {}
    pub fn set_entries(_count: usize) {}
    pub fn push(_gateway_url: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

// Re-export for convenient access
pub(crate) use inner::{push, record_eviction, record_expired, record_hit, record_miss, record_set, set_entries};

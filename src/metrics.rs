// ---------------------------------------------------------------------------
// Prometheus metrics – behind the `metrics-prometheus` feature flag
// ---------------------------------------------------------------------------

#[cfg(feature = "metrics-prometheus")]
pub(crate) mod inner {
    use prometheus::{Counter, Gauge, Registry, TextEncoder};
    use std::sync::OnceLock;

    struct Metrics {
        hits: Counter,
        misses: Counter,
        sets: Counter,
        evictions: Counter,
        expired: Counter,
        invalidation_received: Counter,
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
            let invalidation_received = Counter::new(
                "imc_cache_invalidation_received_total",
                "Total number of cross-process invalidation messages received",
            )
            .expect("metric imc_cache_invalidation_received_total");
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
                .register(Box::new(invalidation_received.clone()))
                .expect("register invalidation_received");
            registry
                .register(Box::new(entries.clone()))
                .expect("register entries");

            Metrics {
                hits,
                misses,
                sets,
                evictions,
                expired,
                invalidation_received,
                entries,
                registry,
            }
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

    pub fn record_invalidation_received() {
        metrics().invalidation_received.inc();
    }

    pub fn set_entries(count: usize) {
        metrics().entries.set(count as f64);
    }

    /// Encode all metrics as Prometheus text format (for HTTP scraping).
    pub fn encode() -> String {
        let metric_families = metrics().registry.gather();
        TextEncoder::new()
            .encode_to_string(&metric_families)
            .unwrap_or_default()
    }

    /// Start a simple HTTP server on `addr` that serves `/metrics` for
    /// Prometheus scraping.
    ///
    /// Blocks the calling thread indefinitely. Run in a background thread:
    ///
    /// ```ignore
    /// std::thread::spawn(|| metrics::serve("127.0.0.1:9090"));
    /// ```
    pub fn serve(addr: &str) -> std::io::Result<()> {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            let mut stream = match stream {
                Ok(s) => s,
                Err(_) => continue,
            };
            let mut buf = [0u8; 1024];
            let n = match stream.read(&mut buf) {
                Ok(n) => n,
                Err(_) => continue,
            };
            let request = String::from_utf8_lossy(&buf[..n]);

            if request.starts_with("GET /metrics ") || request.starts_with("GET /metrics\r\n") {
                let body = encode();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes());
            } else {
                let response =
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                let _ = stream.write_all(response.as_bytes());
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// No-op stubs when the feature is disabled
// ---------------------------------------------------------------------------

#[cfg(not(feature = "metrics-prometheus"))]
pub(crate) mod inner {
    #![allow(dead_code, unused_imports)]
    pub fn record_hit() {}
    pub fn record_miss() {}
    pub fn record_set() {}
    pub fn record_eviction() {}
    pub fn record_expired(_count: u64) {}
    pub fn record_invalidation_received() {}
    pub fn set_entries(_count: usize) {}
    pub fn encode() -> String {
        String::new()
    }
    pub fn serve(_addr: &str) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg_attr(
    not(feature = "metrics-prometheus"),
    allow(unused_imports)
)]
pub(crate) use inner::{
    record_eviction, record_expired, record_hit, record_invalidation_received, record_miss,
    record_set, serve, set_entries,
};

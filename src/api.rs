use std::any::TypeId;
use std::hash::Hash;
#[cfg(any(feature = "async", feature = "tokio"))]
use std::future::Future;

use crate::cache::{global, PerTypeCache};
use crate::hasher::hash_value;
use crate::traits::ImcCacheable;

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
/// # Free-form args
///
/// The `args` parameter is a free generic — any `Hash + Clone + Send + 'static`
/// type works.  This gives maximum flexibility but permits accidental cache-key
/// collisions from typos or inconsistent call sites.
///
/// ```ignore
/// through_imc(42u32, || …);                     // single id
/// through_imc(("gt", 10, "india"), || …);       // tuple of conditions
/// through_imc("select * from …", || …);         // raw SQL string
/// ```
///
/// For strict, compiler-enforced keys see [`through_imc_keyed`].
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
///     type Key = String;
///     fn cache_id(&self) -> u32 { self.id }
///     fn cache_strategy() -> CacheStrategy { CacheStrategy::Lru }
///     fn cache_ttl() -> Option<Duration> { Some(Duration::from_secs(300)) }
/// }
///
/// let u: User = through_imc(42u32, || User { id: 42, name: "Alice".into() });
/// assert_eq!(u.name, "Alice");
///
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

    {
        let stores = global().stores.read().unwrap();
        if let Some(cache) = stores.get(&type_id) {
            if let Some(value) = cache.get::<T>(args_hash) {
                crate::metrics::record_hit();
                crate::log_event!(DEBUG, crate::log::API, crate::log::HIT, type_id = ?type_id);
                return value;
            }
        }
    }

    crate::metrics::record_miss();
    crate::log_event!(DEBUG, crate::log::API, crate::log::MISS, type_id = ?type_id);

    let value = f();
    let id = value.cache_id();
    let id_hash = hash_value(&id);
    let ttl = T::cache_ttl();

    if let Some(vsize) = value.cache_value_size() {
        if vsize > T::cache_max_value_size() {
            crate::log_event!(WARN, crate::log::API, crate::log::SET,
                "value too large ({} bytes, max {}), skipping cache", vsize, T::cache_max_value_size());
            return value;
        }
    }

    let mut stores = global().stores.write().unwrap();
    let cache = stores
        .entry(type_id)
        .or_insert_with(|| PerTypeCache::from_trait::<T>());
    cache.set::<T>(args_hash, id_hash, value, ttl);

    cache
        .get::<T>(args_hash)
        .expect("value was just stored")
}

/// Typed-key variant of [`through_imc`].
///
/// Instead of a free-form generic `A`, the key is obtained from the type's
/// [`ImcCacheable::Key`] associated type.  Override `Key` with a closed enum
/// to make invalid cache keys a compile-time error.
///
/// # Example
///
/// ```ignore
/// #[derive(Hash, Clone)]
/// enum UserKey { ById(i32), ByEmail(String) }
///
/// impl ImcCacheable for User {
///     type Id = i32;
///     type Key = UserKey;
///     // …
/// }
///
/// through_imc_keyed(UserKey::ById(42), || fetch_user(42));          // ✓
/// through_imc_keyed(UserKey::ByEmail("a@b".into()), || …);          // ✓
/// // through_imc_keyed("anything", || …);                           // ✗ compile error
/// ```
pub fn through_imc_keyed<T, F>(key: T::Key, f: F) -> T
where
    T: ImcCacheable,
    F: FnOnce() -> T,
{
    let type_id = TypeId::of::<T>();
    let args_hash = hash_value(&key);

    {
        let stores = global().stores.read().unwrap();
        if let Some(cache) = stores.get(&type_id) {
            if let Some(value) = cache.get::<T>(args_hash) {
                crate::metrics::record_hit();
                crate::log_event!(DEBUG, crate::log::API, crate::log::HIT, type_id = ?type_id);
                return value;
            }
        }
    }

    crate::metrics::record_miss();
    crate::log_event!(DEBUG, crate::log::API, crate::log::MISS, type_id = ?type_id);

    let value = f();
    let id = value.cache_id();
    let id_hash = hash_value(&id);
    let ttl = T::cache_ttl();

    if let Some(vsize) = value.cache_value_size() {
        if vsize > T::cache_max_value_size() {
            crate::log_event!(WARN, crate::log::API, crate::log::SET,
                "value too large ({} bytes, max {}), skipping cache", vsize, T::cache_max_value_size());
            return value;
        }
    }

    let mut stores = global().stores.write().unwrap();
    let cache = stores
        .entry(type_id)
        .or_insert_with(|| PerTypeCache::from_trait::<T>());
    cache.set::<T>(args_hash, id_hash, value, ttl);

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
                crate::metrics::record_hit();
                crate::log_event!(DEBUG, crate::log::API, crate::log::HIT, type_id = ?type_id);
                return value;
            }
        }
    }

    crate::metrics::record_miss();
    crate::log_event!(DEBUG, crate::log::API, crate::log::MISS, type_id = ?type_id);

    let value = f().await;
    let id = value.cache_id();
    let id_hash = hash_value(&id);
    let ttl = T::cache_ttl();

    if let Some(vsize) = value.cache_value_size() {
        if vsize > T::cache_max_value_size() {
            crate::log_event!(WARN, crate::log::API, crate::log::SET,
                "value too large ({} bytes, max {}), skipping cache", vsize, T::cache_max_value_size());
            return value;
        }
    }

    let mut stores = global().stores.write().unwrap();
    let cache = stores
        .entry(type_id)
        .or_insert_with(|| PerTypeCache::from_trait::<T>());
    cache.set::<T>(args_hash, id_hash, value, ttl);

    cache
        .get::<T>(args_hash)
        .expect("value was just stored")
}

/// Typed-key variant of [`through_imc_async`].
///
/// See [`through_imc_keyed`] for details — this is the async equivalent.
#[cfg(any(feature = "async", feature = "tokio"))]
pub async fn through_imc_keyed_async<T, F, Fut>(key: T::Key, f: F) -> T
where
    T: ImcCacheable,
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    let type_id = TypeId::of::<T>();
    let args_hash = hash_value(&key);

    {
        let stores = global().stores.read().unwrap();
        if let Some(cache) = stores.get(&type_id) {
            if let Some(value) = cache.get::<T>(args_hash) {
                crate::metrics::record_hit();
                crate::log_event!(DEBUG, crate::log::API, crate::log::HIT, type_id = ?type_id);
                return value;
            }
        }
    }

    crate::metrics::record_miss();
    crate::log_event!(DEBUG, crate::log::API, crate::log::MISS, type_id = ?type_id);

    let value = f().await;
    let id = value.cache_id();
    let id_hash = hash_value(&id);
    let ttl = T::cache_ttl();

    if let Some(vsize) = value.cache_value_size() {
        if vsize > T::cache_max_value_size() {
            crate::log_event!(WARN, crate::log::API, crate::log::SET,
                "value too large ({} bytes, max {}), skipping cache", vsize, T::cache_max_value_size());
            return value;
        }
    }

    let mut stores = global().stores.write().unwrap();
    let cache = stores
        .entry(type_id)
        .or_insert_with(|| PerTypeCache::from_trait::<T>());
    cache.set::<T>(args_hash, id_hash, value, ttl);

    cache
        .get::<T>(args_hash)
        .expect("value was just stored")
}

/// Remove a single entry (by its unique [`Id`](ImcCacheable::Id)).
pub fn imc_remove<T: ImcCacheable>(id: &T::Id) {
    let type_id = TypeId::of::<T>();
    let id_hash = hash_value(id);

    let mut stores = global().stores.write().unwrap();
    if let Some(cache) = stores.get_mut(&type_id) {
        cache.remove_data(id_hash);
        crate::log_event!(DEBUG, crate::log::API, crate::log::REMOVE, type_id = ?type_id, id_hash = id_hash);
    }
}

/// Evict every cached entry for `T`.
pub fn imc_clear<T: ImcCacheable>() {
    let type_id = TypeId::of::<T>();

    let mut stores = global().stores.write().unwrap();
    if let Some(cache) = stores.get_mut(&type_id) {
        cache.clear();
        crate::log_event!(INFO, crate::log::API, crate::log::CLEAR, type_id = ?type_id);
    }
}

/// Number of unique entries currently cached for `T`.
pub fn imc_len<T: ImcCacheable>() -> usize {
    let type_id = TypeId::of::<T>();

    let stores = global().stores.read().unwrap();
    let len = stores.get(&type_id).map_or(0, |c| c.len());
    crate::metrics::set_entries(len);
    len
}

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
/// redis_publish("users", inval_id);
/// ```
pub fn imc_invalidation_id<T: ImcCacheable>(id: &T::Id) -> String {
    let id_hash = hash_value(id);
    id_hash.to_string()
}

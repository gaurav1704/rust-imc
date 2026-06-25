use std::any::TypeId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};

use crate::cache::global;
use crate::traits::{CriticalKey, ImcCacheable};

// ---------------------------------------------------------------------------
// Registry: channel name → value TypeId
// ---------------------------------------------------------------------------

struct CriticalRegistry {
    channels: HashMap<String, Vec<TypeId>>,
}

static CRITICAL_REGISTRY: OnceLock<Mutex<CriticalRegistry>> = OnceLock::new();

fn registry() -> &'static Mutex<CriticalRegistry> {
    CRITICAL_REGISTRY.get_or_init(|| {
        Mutex::new(CriticalRegistry {
            channels: HashMap::new(),
        })
    })
}

/// Return all registered critical channels for the subscriber thread.
pub(crate) fn snapshot_channels() -> Vec<String> {
    let reg = registry().lock().unwrap();
    reg.channels.keys().cloned().collect()
}

// ---------------------------------------------------------------------------
// Publishing
// ---------------------------------------------------------------------------

/// Called from [`through_imc_keyed`](crate::through_imc_keyed) after storing
/// a freshly computed value.  Registers the channel and publishes the key
/// hash when a subscriber is active.
pub(crate) fn after_store<T, K>(_key: &K, args_hash: u64)
where
    T: ImcCacheable<Key = K>,
    K: CriticalKey,
{
    // register this (value type, key type) mapping
    {
        let mut reg = registry().lock().unwrap();
        reg.channels
            .entry(K::channel().to_string())
            .or_default()
            .push(TypeId::of::<T>());
    }

    if !SUBSCRIBER_ACTIVE.load(Ordering::Relaxed) {
        return;
    }

    let channel = K::channel();
    let payload = args_hash.to_string();

    crate::log_event!(DEBUG, crate::log::CRITICAL, crate::log::PUBLISH,
        channel = channel, key_hash = payload);

    if let Some(url) = crate::worker::get_redis_url() {
        if let Ok(client) = redis::Client::open(url) {
            if let Ok(mut conn) = client.get_connection() {
                let _: Result<(), _> = redis::Cmd::publish(channel, &payload).query(&mut conn);
            }
        }
    }
}

/// Set to `true` by the subscriber thread once it begins listening.
pub(crate) static SUBSCRIBER_ACTIVE: AtomicBool = AtomicBool::new(false);

// ---------------------------------------------------------------------------
// Subscriber
// ---------------------------------------------------------------------------

/// Subscribe to all registered critical channels and remove stale cache
/// entries when messages arrive.
pub(crate) fn subscriber_loop(redis_url: &str) {
    crate::log_event!(INFO, crate::log::CRITICAL, crate::log::START,
        redis_url = redis_url);

    let client = match redis::Client::open(redis_url) {
        Ok(c) => c,
        Err(_e) => {
            crate::log_event!(ERROR, crate::log::CRITICAL, crate::log::ERROR,
                "failed to connect to Redis: {}", _e);
            return;
        }
    };

    SUBSCRIBER_ACTIVE.store(true, Ordering::Relaxed);

    loop {
        if crate::worker::WORKER_TX.lock().unwrap_or_else(|e| e.into_inner()).is_none() {
            crate::log_event!(INFO, crate::log::CRITICAL, crate::log::STOP,
                reason = "worker gone");
            break;
        }

        let channels = snapshot_channels();
        if channels.is_empty() {
            std::thread::sleep(std::time::Duration::from_secs(1));
            continue;
        }

        match run_subscriber(&client, &channels) {
            Ok(()) => break,
            Err(_err) => {
                crate::log_event!(WARN, crate::log::CRITICAL, crate::log::ERROR,
                    "critical subscriber error: {}, reconnecting in 5s", _err);
                std::thread::sleep(std::time::Duration::from_secs(5));
            }
        }
    }
}

fn run_subscriber(
    client: &redis::Client,
    channels: &[String],
) -> redis::RedisResult<()> {
    let mut conn = client.get_connection()?;
    conn.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;
    let mut pubsub = conn.as_pubsub();

    for channel in channels {
        pubsub.subscribe(channel.as_str())?;
        crate::log_event!(DEBUG, crate::log::CRITICAL, crate::log::START,
            "subscribed to critical channel '{}'", channel);
    }

    loop {
        if crate::worker::WORKER_TX.lock().unwrap_or_else(|e| e.into_inner()).is_none() {
            break;
        }

        match pubsub.get_message() {
            Ok(msg) => {
                let channel = msg.get_channel_name().to_string();
                let payload: String = msg.get_payload()?;

                crate::log_event!(DEBUG, crate::log::CRITICAL, crate::log::REMOVE,
                    channel = &channel, payload = &payload);

                if let Ok(key_hash) = payload.parse::<u64>() {
                    handle_invalidation(&channel, key_hash);
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

fn handle_invalidation(channel: &str, key_hash: u64) {
    let reg = registry().lock().unwrap();
    if let Some(type_ids) = reg.channels.get(channel) {
        for &type_id in type_ids {
            let mut stores = global().stores.write().unwrap();
            if let Some(cache) = stores.get_mut(&type_id) {
                cache.remove_by_args_hash(key_hash);
                crate::log_event!(DEBUG, crate::log::CRITICAL, crate::log::REMOVE,
                    type_id = ?type_id, key_hash = key_hash);
            }
        }
    }
}

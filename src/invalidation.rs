use std::any::TypeId;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use crate::cache::global;
use crate::traits::ImcCacheable;
use crate::worker::WORKER_TX;

struct Registry {
    channels: HashMap<String, TypeId>,
}

static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();

fn registry() -> &'static Mutex<Registry> {
    REGISTRY.get_or_init(|| Mutex::new(Registry { channels: HashMap::new() }))
}

pub(crate) fn register<T: ImcCacheable>() {
    let channel = match T::cache_invalidation_channel() {
        Some(c) => c,
        None => return,
    };
    let mut reg = registry().lock().unwrap();
    reg.channels.entry(channel.to_string()).or_insert(TypeId::of::<T>());
}

pub(crate) fn snapshot_channels() -> Vec<(String, TypeId)> {
    let reg = registry().lock().unwrap();
    reg.channels.iter().map(|(c, t)| (c.clone(), *t)).collect()
}

pub(crate) fn redis_subscriber_loop(redis_url: &str, channels: Vec<(String, TypeId)>) {
    crate::log_event!(INFO, crate::log::INVALIDATION, crate::log::START,
        redis_url = redis_url, channel_count = channels.len());

    let chan_map: HashMap<String, TypeId> = channels.into_iter().collect();
    let client = match redis::Client::open(redis_url) {
        Ok(c) => c,
        Err(_e) => {
            crate::log_event!(ERROR, crate::log::INVALIDATION, crate::log::ERROR,
                "failed to connect to Redis: {}", _e);
            return;
        }
    };

    loop {
        if WORKER_TX.lock().unwrap_or_else(|e| e.into_inner()).is_none() {
            crate::log_event!(INFO, crate::log::INVALIDATION, crate::log::STOP,
                reason = "worker gone");
            break;
        }
        match run_subscriber(&client, &chan_map) {
            Ok(()) => break,
            Err(_err) => {
                crate::log_event!(WARN, crate::log::INVALIDATION, crate::log::ERROR,
                    "Redis subscriber error: {}, reconnecting in 5s", _err);
                std::thread::sleep(Duration::from_secs(5));
            }
        }
    }
}

fn run_subscriber(
    client: &redis::Client,
    chan_map: &HashMap<String, TypeId>,
) -> redis::RedisResult<()> {
    let mut conn = client.get_connection()?;
    conn.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut pubsub = conn.as_pubsub();

    for channel in chan_map.keys() {
        pubsub.subscribe(channel.as_str())?;
        crate::log_event!(DEBUG, crate::log::INVALIDATION, crate::log::START,
            "subscribed to channel '{}'", channel);
    }

    loop {
        if WORKER_TX.lock().unwrap_or_else(|e| e.into_inner()).is_none() {
            break;
        }

        match pubsub.get_message() {
            Ok(msg) => {
                let channel = msg.get_channel_name();
                let payload: String = msg.get_payload()?;

                crate::log_event!(DEBUG, crate::log::INVALIDATION, crate::log::REMOVE,
                    channel = channel, payload = &payload);

                crate::metrics::record_invalidation_received();

                if let Ok(id_hash) = payload.parse::<u64>() {
                    if let Some(&type_id) = chan_map.get(channel) {
                        let mut stores = global().stores.write().unwrap();
                        if let Some(cache) = stores.get_mut(&type_id) {
                            cache.remove_data(id_hash);
                        }
                    }
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

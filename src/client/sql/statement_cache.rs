use std::{mem, num::NonZeroUsize};

use lru::LruCache;
use parking_lot::Mutex;

/// Prepared statement ids, cached per SQL text for one server session.
///
/// Tarantool checks statement ids per session, so a reconnect (a new session)
/// invalidates every id. The cache is labelled with the generation its ids
/// belong to: a newer generation empties it, an older one is a plain miss.
pub(crate) struct SqlStatementCache {
    state: Mutex<CacheState>,
    /// Held while one statement is prepared for the cache. Only ever taken
    /// with `try_lock`, so holding it across an await blocks no one.
    pub(crate) preparing: Mutex<()>,
}

struct CacheState {
    generation: u64,
    lru: LruCache<String, u64>,
}

impl SqlStatementCache {
    pub(crate) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            state: Mutex::new(CacheState {
                generation: 0,
                lru: LruCache::new(capacity),
            }),
            preparing: Mutex::new(()),
        }
    }

    /// Id cached for `text` in the session of `generation`.
    ///
    /// A newer generation empties the cache first. The old map is dropped
    /// after the lock is released: its `Drop` frees the entries without
    /// rehashing them, while `LruCache::clear` would pop them one by one under
    /// the lock. An older generation is a plain miss that leaves the cache
    /// alone.
    pub(crate) fn get(&self, text: &str, generation: u64) -> Option<u64> {
        let mut state = self.state.lock();
        if generation > state.generation {
            state.generation = generation;
            // Read first: `mem::replace` borrows `state` mutably.
            let cap = state.lru.cap();
            let old_lru = mem::replace(&mut state.lru, LruCache::new(cap));
            drop(state);
            drop(old_lru);
            return None;
        }
        if generation < state.generation {
            return None;
        }
        state.lru.get(text).copied()
    }

    /// Cache `id` for `text` if the cache still belongs to `generation`, the
    /// generation read before the PREPARE was sent. Returns whether it stored.
    pub(crate) fn put(&self, text: &str, id: u64, generation: u64) -> bool {
        let mut state = self.state.lock();
        if state.generation != generation {
            return false;
        }
        state.lru.put(text.to_owned(), id);
        true
    }

    /// Remove `text` if it still maps to `id`; `peek` does not refresh the
    /// recency of an entry it keeps. A concurrent re-prepare yields the same
    /// id, since Tarantool derives it from the text, so this can evict a fresh
    /// entry; that costs one extra PREPARE.
    pub(crate) fn evict(&self, text: &str, id: u64) {
        let mut state = self.state.lock();
        if state.lru.peek(text) == Some(&id) {
            state.lru.pop(text);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_ignores_an_older_generation() {
        let cache = SqlStatementCache::new(NonZeroUsize::new(10).unwrap());
        assert_eq!(cache.get("SELECT 1", 1), None);
        assert!(cache.put("SELECT 1", 42, 1));
        // A lookup that read the generation before a reconnect: a plain miss
        // that relabels nothing.
        assert_eq!(cache.get("SELECT 1", 0), None);
        assert_eq!(cache.get("SELECT 1", 1), Some(42));
    }

    #[test]
    fn relabel_empties_the_cache_and_keeps_its_capacity() {
        let cache = SqlStatementCache::new(NonZeroUsize::new(10).unwrap());
        assert!(cache.put("SELECT 1", 42, 0));
        assert_eq!(cache.get("SELECT 1", 1), None);
        let state = cache.state.lock();
        assert_eq!(state.generation, 1);
        assert_eq!(state.lru.len(), 0);
        assert_eq!(state.lru.cap().get(), 10);
    }

    #[test]
    fn put_for_an_older_generation_stores_nothing() {
        let cache = SqlStatementCache::new(NonZeroUsize::new(10).unwrap());
        assert_eq!(cache.get("SELECT 2", 1), None);
        assert!(!cache.put("SELECT 1", 42, 0));
        assert_eq!(cache.get("SELECT 1", 1), None);
    }

    #[test]
    fn evict_removes_only_the_rejected_id() {
        let cache = SqlStatementCache::new(NonZeroUsize::new(10).unwrap());
        assert!(cache.put("SELECT 1", 42, 0));
        cache.evict("SELECT 1", 7);
        assert_eq!(cache.get("SELECT 1", 0), Some(42));
        cache.evict("SELECT 1", 42);
        assert_eq!(cache.get("SELECT 1", 0), None);
    }
}

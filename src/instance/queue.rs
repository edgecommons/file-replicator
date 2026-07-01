//! # file-replicator — durable work-queue + concurrency governors (DESIGN §8/§8.3)
//!
//! A thin coordinator over the durable [`StateStore`]: it enqueues ready files, atomically claims the
//! oldest-ready batch (write-ahead `Ready → InProgress`, gated by each item's backoff clock), and
//! hands out concurrency **slots**. Two semaphores bound in-flight transfers — a **per-instance** one
//! (`limits.maxConcurrentFiles`) so one instance can't monopolize the process, and a **global** one
//! (`component.global.limits.maxConcurrentFiles`) shared across every instance (FR-REL-5). The
//! durable state machine itself lives in [`state`](crate::state) (the transitions) and
//! [`worker`](crate::instance::worker) (the retry/backoff decision); this module owns only the
//! queue-front + backpressure.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::domain::{ItemState, WorkItem};
use crate::error::Result;
use crate::state::StateStore;

/// The per-instance queue front: durable claim/enqueue plus the two concurrency semaphores.
///
/// `Clone` is cheap (three `Arc`s + a `String` + a `usize`) and shares the same underlying store and
/// semaphores, so a clone can be moved into a `spawn_blocking` closure to run a bulk scan
/// (`promote_due`) off the async runtime without disturbing slot accounting (DESIGN §6.3).
#[derive(Clone)]
pub struct Queue {
    instance: String,
    store: Arc<dyn StateStore>,
    /// Bounds this instance's in-flight transfers (`limits.maxConcurrentFiles`).
    per_instance: Arc<Semaphore>,
    /// Shared across all instances (`component.global.limits.maxConcurrentFiles`).
    global: Arc<Semaphore>,
    per_limit: usize,
}

/// A held concurrency slot: one global permit and one per-instance permit. Both release on drop, so a
/// worker simply keeps the guard alive for the duration of a transfer.
pub struct SlotGuard {
    _global: OwnedSemaphorePermit,
    _per_instance: OwnedSemaphorePermit,
}

impl Queue {
    /// Build the queue for `instance`. `per_limit` (clamped to ≥ 1) sizes the per-instance semaphore;
    /// `global` is the shared semaphore built once by the app.
    pub fn new(
        instance: String,
        store: Arc<dyn StateStore>,
        per_limit: usize,
        global: Arc<Semaphore>,
    ) -> Self {
        let per_limit = per_limit.max(1);
        Queue {
            instance,
            store,
            per_instance: Arc::new(Semaphore::new(per_limit)),
            global,
            per_limit,
        }
    }

    /// The per-instance concurrency cap (also the max batch size a tick claims).
    pub fn per_limit(&self) -> usize {
        self.per_limit
    }

    /// Currently-available per-instance permits (diagnostics / concurrency assertions in tests).
    #[allow(dead_code)]
    pub fn available(&self) -> usize {
        self.per_instance.available_permits()
    }

    /// Durably record a discovered-ready file (write-ahead of any transfer). `mtime_ms` is the file's
    /// change signature: an unchanged terminal/in-flight item of the same identity is preserved
    /// (never resurrected to `Ready`), while a genuinely-new file at a completed relpath is reset to
    /// `Ready` for re-replication (see [`StateStore::upsert_ready`]).
    pub fn enqueue(&self, relpath: &str, size: u64, mtime_ms: i64, now: i64) -> Result<()> {
        self.store
            .upsert_ready(&self.instance, relpath, size, mtime_ms, now)
    }

    /// Retry manager (DESIGN §8.2 step 5): promote `Failed` items whose backoff gate has elapsed back
    /// to `Ready` so [`claim`](Self::claim) re-picks them (the store's `claim_ready` only selects
    /// `Ready`). The gate (`next_attempt_at`) is preserved — it has, by definition, already elapsed.
    /// Returns how many were promoted.
    pub fn promote_due(&self, now: i64) -> Result<usize> {
        let mut promoted = 0;
        for it in self.store.list_by_state(&self.instance, ItemState::Failed)? {
            if it.next_attempt_at <= now {
                self.store
                    .set_state(&self.instance, &it.relpath, ItemState::Ready, now)?;
                promoted += 1;
            }
        }
        Ok(promoted)
    }

    /// Atomically claim up to `limit` oldest ready items whose backoff gate has elapsed, transitioning
    /// them `Ready → InProgress` in the store (write-ahead, DESIGN §13.2).
    pub fn claim(&self, limit: usize, now: i64) -> Result<Vec<WorkItem>> {
        self.store.claim_ready(&self.instance, limit, now)
    }

    /// Acquire one concurrency slot, awaiting a free permit in **both** the global and per-instance
    /// semaphores (global first, then per-instance — a consistent order, so no deadlock). The returned
    /// [`SlotGuard`] releases both on drop.
    pub async fn acquire_slot(&self) -> SlotGuard {
        // `Semaphore`s live for the whole run and are never closed, so `acquire_owned` cannot error.
        let global = self
            .global
            .clone()
            .acquire_owned()
            .await
            .expect("global concurrency semaphore closed");
        let per_instance = self
            .per_instance
            .clone()
            .acquire_owned()
            .await
            .expect("per-instance concurrency semaphore closed");
        SlotGuard {
            _global: global,
            _per_instance: per_instance,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ItemState;
    use crate::state::SqliteStore;

    const INST: &str = "q";

    fn queue(per_limit: usize, global_permits: usize) -> Queue {
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        Queue::new(
            INST.to_string(),
            store,
            per_limit,
            Arc::new(Semaphore::new(global_permits)),
        )
    }

    #[test]
    fn per_limit_is_clamped_to_one() {
        assert_eq!(queue(0, 8).per_limit(), 1);
        assert_eq!(queue(4, 8).per_limit(), 4);
    }

    #[test]
    fn enqueue_then_claim_transitions_in_progress() {
        let q = queue(4, 8);
        q.enqueue("a.txt", 10, 0, 1).unwrap();
        q.enqueue("b.txt", 20, 0, 2).unwrap();
        let claimed = q.claim(10, 100).unwrap();
        assert_eq!(claimed.len(), 2);
        assert!(claimed.iter().all(|i| i.state == ItemState::InProgress));
        // A second claim finds nothing left ready.
        assert!(q.claim(10, 100).unwrap().is_empty());
    }

    #[test]
    fn claim_is_bounded_by_limit() {
        let q = queue(4, 8);
        for i in 0..5 {
            q.enqueue(&format!("f{i}"), 1, 0, i as i64).unwrap();
        }
        assert_eq!(q.claim(2, 100).unwrap().len(), 2);
        assert_eq!(q.claim(2, 100).unwrap().len(), 2);
        assert_eq!(q.claim(2, 100).unwrap().len(), 1);
    }

    #[test]
    fn promote_due_reclaims_failed_after_gate() {
        let q = queue(4, 8);
        q.enqueue("f", 1, 0, 1).unwrap();
        let item = q.claim(1, 10).unwrap().pop().unwrap();
        // Simulate a recorded failure with a future backoff gate.
        q.store
            .record_attempt(INST, &item.relpath, "boom", ItemState::Failed, 500, 20)
            .unwrap();
        // Before the gate: not promoted, not claimable.
        assert_eq!(q.promote_due(100).unwrap(), 0);
        assert!(q.claim(10, 100).unwrap().is_empty());
        // At/after the gate: promoted to Ready and re-claimable.
        assert_eq!(q.promote_due(500).unwrap(), 1);
        let again = q.claim(10, 500).unwrap();
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].relpath, "f");
    }

    #[tokio::test]
    async fn slot_guard_bounds_and_releases_permits() {
        let q = queue(2, 5);
        assert_eq!(q.available(), 2);
        let g1 = q.acquire_slot().await;
        let g2 = q.acquire_slot().await;
        assert_eq!(q.available(), 0, "per-instance permits exhausted");
        // A third acquire would block; prove it is pending, then free one and let it proceed.
        assert!(tokio::time::timeout(std::time::Duration::from_millis(50), q.acquire_slot())
            .await
            .is_err());
        drop(g1);
        assert_eq!(q.available(), 1);
        let _g3 = tokio::time::timeout(std::time::Duration::from_millis(50), q.acquire_slot())
            .await
            .expect("slot frees up after a guard drops");
        drop(g2);
    }

    #[tokio::test]
    async fn global_semaphore_gates_across_instances() {
        // Two instances share a global cap of 1.
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let global = Arc::new(Semaphore::new(1));
        let a = Queue::new("a".into(), store.clone(), 4, global.clone());
        let b = Queue::new("b".into(), store, 4, global.clone());
        let ga = a.acquire_slot().await; // takes the only global permit
        assert!(tokio::time::timeout(std::time::Duration::from_millis(50), b.acquire_slot())
            .await
            .is_err());
        drop(ga);
        let _gb = tokio::time::timeout(std::time::Duration::from_millis(50), b.acquire_slot())
            .await
            .expect("global permit frees up for the other instance");
    }
}

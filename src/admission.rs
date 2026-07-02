//! # file-replicator — priority-aware global concurrency admission (Feature B)
//!
//! [`PriorityGate`] replaces the plain `Arc<Semaphore>` that used to bound the **global**
//! (cross-instance) in-flight-file cap. Every instance still shares ONE pool of permits (sized from
//! `component.global.limits.maxConcurrentFiles`, DESIGN §8.3), but under contention a waiter is now
//! admitted in **priority order** (lower `priority` = admitted first, matching the file-replicator-wide
//! 0-255 convention — see `InstanceCfg::priority`), with FIFO among equal priorities. This governs the
//! GLOBAL admission decision ONLY: the per-instance semaphore in [`super::instance::queue::Queue`] is
//! untouched, and there is no bandwidth weighting by priority (explicitly out of scope).
//!
//! ## Design
//! A single `std::sync::Mutex<Inner>` guards the permit count and the waiter queue; the mutex is only
//! ever held across a *synchronous* critical section (never across an `.await`), so it cannot deadlock
//! or stall the async runtime. Each queued waiter is a `tokio::sync::oneshot::Receiver<()>` keyed by
//! `(priority, seq)` in a `BTreeMap` — `seq` is a monotonically increasing enqueue counter, so the
//! ordering key gives exactly "lowest priority number first, oldest enqueue time breaks ties" (strict
//! priority + FIFO-within-priority). A `BTreeMap` (not a `BinaryHeap`) is deliberate: it supports
//! O(log n) removal **by key**, which is what makes cancellation cleanup (see below) cheap.
//!
//! ## Fast path / no barging
//! [`PriorityGate::acquire`] takes a free permit immediately **only when no one is already queued**. If
//! it instead barged ahead of a queued waiter whenever a permit happened to be free, a new lower-priority
//! caller could repeatedly steal permits out from under an older, higher-priority waiter that just missed
//! the fast path by one instruction — violating both the priority guarantee and FIFO-within-priority.
//! Symmetrically, [`release_one`](PriorityGate::release_one) (called by [`GateGuard::drop`]) never
//! increments the free-permit count while waiters exist — it always **hands the permit directly** to the
//! highest-priority queued waiter instead. Together these two rules mean a permit release can never be
//! observed by a caller that arrived after an already-queued waiter.
//!
//! ## Cancellation safety (no permit leak)
//! A waiter's future can be dropped at any point while queued (or even in the narrow window right after
//! being granted) — e.g. an instance shutting down, or a window closing and aborting its in-flight
//! `JoinSet` (DESIGN §12.4). [`WaiterTicket`]'s `Drop` handles every case:
//! - **still queued** (never granted): remove its entry from the map. Its permit, if any, was never
//!   allocated to it — nothing to give back.
//! - **granted but the future was dropped before observing it** (the receive already completed a
//!   `poll()` that returned `Ready`, i.e. [`WaiterTicket::granted`] was set): `Drop` is a no-op — the
//!   live [`GateGuard`] now owns that permit and will release it normally.
//! - **granted (its `oneshot::Sender::send` already ran and removed it from the map) but the future is
//!   dropped BEFORE it is polled again to observe the grant**: the ticket is not in the map (so the
//!   "still queued" branch doesn't apply) and `granted` is still `false` (the code that sets it never
//!   ran) — this is the one case that would leak a permit if unhandled, so `Drop` detects exactly this
//!   state (not-in-map AND not-granted) and calls `release_one_locked` again to hand the permit to the
//!   NEXT waiter (or return it to the pool). See `grant_then_cancel_redistributes_permit` below.
//!
//! ## Demand-adaptive / bounded
//! A waiter entry exists in the map **only while a caller is actually blocked** in `acquire()` — an
//! idle high-priority instance that never calls `acquire()` reserves nothing (no entry, no permit held
//! back). Memory is O(current blocked waiters), never O(configured instances).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

/// Guarded shared state; the `Mutex` is only ever held synchronously (see the module doc).
struct Inner {
    /// Free permits not currently held by anyone and not earmarked for a waiter.
    permits: usize,
    /// Monotonic enqueue counter — the FIFO tiebreaker within a priority.
    next_seq: u64,
    /// Queued waiters ordered by `(priority, seq)`: iterating from the front (`.iter().next()`) always
    /// yields the highest-priority, oldest-enqueued waiter (lower `priority` number sorts first).
    waiters: BTreeMap<(i32, u64), oneshot::Sender<()>>,
}

impl Inner {
    /// Hand the one available permit to the highest-priority queued waiter, or (nobody queued) return
    /// it to the free pool. Called with the lock already held. Defensive `while`: if a sender's
    /// receiver somehow already vanished (not reachable under this module's invariants — a `Receiver`
    /// is only dropped by [`WaiterTicket::drop`], which always removes its own entry first — but kept
    /// as a safety net rather than an assumed-unreachable `unwrap`), fall through to the next waiter
    /// instead of losing the permit.
    fn release_one_locked(&mut self) {
        while let Some((&key, _)) = self.waiters.iter().next() {
            let tx = self.waiters.remove(&key).expect("key just read from the map");
            if tx.send(()).is_ok() {
                return;
            }
        }
        self.permits += 1;
    }
}

/// Priority-aware admission gate: the process-wide cross-instance concurrency cap (DESIGN §8.3),
/// replacing the plain `Arc<Semaphore>` global governor. See the module doc for the full design.
pub struct PriorityGate {
    inner: Mutex<Inner>,
}

impl PriorityGate {
    /// Build a gate with `permits` free slots.
    pub fn new(permits: usize) -> Self {
        PriorityGate {
            inner: Mutex::new(Inner {
                permits,
                next_seq: 0,
                waiters: BTreeMap::new(),
            }),
        }
    }

    /// Acquire one slot, admitted in priority order under contention (lower `priority` = admitted
    /// first; FIFO among equal priorities). Takes `self: &Arc<Self>` so a queued waiter's cleanup
    /// ([`WaiterTicket`]) can hold its own `Arc` clone — the gate stays alive for as long as anything
    /// might still need to release into it, independent of the caller's own `Arc<PriorityGate>`
    /// lifetime at any single call site.
    pub async fn acquire(self: &Arc<Self>, priority: i32) -> GateGuard {
        let (rx, key) = {
            let mut g = self.inner.lock().expect("priority gate mutex poisoned");
            // Fast path: take a free permit immediately, but ONLY when nobody is already queued — see
            // "Fast path / no barging" in the module doc. This lock scope never crosses an `.await`.
            if g.permits > 0 && g.waiters.is_empty() {
                g.permits -= 1;
                return GateGuard { gate: self.clone() };
            }
            let seq = g.next_seq;
            g.next_seq += 1;
            let key = (priority, seq);
            let (tx, rx) = oneshot::channel();
            g.waiters.insert(key, tx);
            (rx, key)
        };

        let mut ticket = WaiterTicket {
            gate: self.clone(),
            key,
            granted: false,
        };
        // Semaphores in this codebase are never closed for the process lifetime (see
        // `Queue::acquire_slot`'s identical `.expect`); a `PriorityGate` has the same guarantee — the
        // sender side only vanishes via `WaiterTicket::drop`, which always sends-or-redistributes first.
        rx.await.expect("priority gate dropped a waiter without granting or redistributing its permit");
        // No `.await` between here and returning: the ticket is either forgotten (its `Drop` becomes a
        // no-op) or, on the way out, converted into the `GateGuard` below — either way `granted` being
        // observably `true` from this point on is safe.
        ticket.granted = true;
        GateGuard { gate: self.clone() }
    }

    /// Release one permit: hand it directly to the highest-priority waiter, or return it to the pool.
    fn release_one(&self) {
        let mut g = self.inner.lock().expect("priority gate mutex poisoned");
        g.release_one_locked();
    }

    /// Currently-free permits not earmarked for any waiter (diagnostics / test assertions).
    #[cfg(test)]
    fn available(&self) -> usize {
        self.inner.lock().expect("priority gate mutex poisoned").permits
    }

    /// Currently-queued waiter count (diagnostics / test assertions) — the demand-adaptive "bounded"
    /// property this module promises: 0 whenever nobody is actually blocked.
    #[cfg(test)]
    fn waiting(&self) -> usize {
        self.inner.lock().expect("priority gate mutex poisoned").waiters.len()
    }
}

/// A held admission slot. Releases on drop (handing off directly to the next waiter, or returning the
/// permit to the pool — see [`PriorityGate::release_one`]).
pub struct GateGuard {
    gate: Arc<PriorityGate>,
}

impl Drop for GateGuard {
    fn drop(&mut self) {
        self.gate.release_one();
    }
}

/// RAII bookkeeping for one in-flight [`PriorityGate::acquire`] call, whether it's still queued,
/// currently being granted, or already converted into a live [`GateGuard`]. See "Cancellation safety"
/// in the module doc for exactly what each `Drop` branch handles.
struct WaiterTicket {
    gate: Arc<PriorityGate>,
    key: (i32, u64),
    granted: bool,
}

impl Drop for WaiterTicket {
    fn drop(&mut self) {
        if self.granted {
            // Ownership of the permit has already moved to a `GateGuard` — nothing to do here.
            return;
        }
        let mut g = self.gate.inner.lock().expect("priority gate mutex poisoned");
        if g.waiters.remove(&self.key).is_some() {
            // Still queued, never granted: no permit was ever allocated to this waiter.
            return;
        }
        // Not queued and not granted: `release_one_locked` already sent this waiter its permit (via
        // `oneshot::Sender::send`) but the future was dropped before it was polled again to observe
        // that grant and set `granted = true`. That permit would otherwise be lost — redistribute it.
        g.release_one_locked();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn fast_path_takes_free_permit() {
        let gate = Arc::new(PriorityGate::new(1));
        let g1 = gate.acquire(100).await;
        assert_eq!(gate.available(), 0);

        // A second acquire, with the only permit held, blocks.
        let gate2 = gate.clone();
        let res = tokio::time::timeout(Duration::from_millis(50), async move {
            gate2.acquire(100).await
        })
        .await;
        assert!(res.is_err(), "second acquire should block while the only permit is held");
        drop(g1);
    }

    #[tokio::test]
    async fn release_returns_permit_to_pool_when_no_waiters() {
        let gate = Arc::new(PriorityGate::new(1));
        let g1 = gate.acquire(100).await;
        assert_eq!(gate.available(), 0);
        drop(g1);
        assert_eq!(gate.available(), 1, "no waiters -> the permit returns to the free pool");
        assert_eq!(gate.waiting(), 0);
    }

    /// Higher priority (lower number) waiters are admitted first under contention, regardless of the
    /// order they enqueued in.
    #[tokio::test]
    async fn higher_priority_waiter_wakes_first() {
        let gate = Arc::new(PriorityGate::new(1));
        let holder = gate.acquire(0).await; // takes the only permit

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<&'static str>();
        let mut handles = Vec::new();
        // Enqueued in this order: 100 (low prio), 10 (high prio), 50 (mid prio).
        for (label, prio) in [("p100", 100), ("p10", 10), ("p50", 50)] {
            let gate = gate.clone();
            let tx = tx.clone();
            handles.push(tokio::spawn(async move {
                let _g = gate.acquire(prio).await;
                tx.send(label).unwrap();
            }));
            tokio::task::yield_now().await; // let it register as a waiter before spawning the next
        }
        assert_eq!(gate.waiting(), 3, "all three are contended and queued");

        drop(holder); // releases the permit -> hands off in priority order, not enqueue order
        assert_eq!(rx.recv().await, Some("p10"), "highest priority (lowest number) admitted first");
        assert_eq!(rx.recv().await, Some("p50"), "then the mid priority");
        assert_eq!(rx.recv().await, Some("p100"), "then the low priority");

        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(gate.waiting(), 0);
        assert_eq!(gate.available(), 1);
    }

    /// Waiters at the SAME priority are served strictly in enqueue order (no reordering within a
    /// priority).
    #[tokio::test]
    async fn fifo_within_equal_priority() {
        let gate = Arc::new(PriorityGate::new(1));
        let holder = gate.acquire(0).await;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
        let mut handles = Vec::new();
        for label in [1u32, 2, 3] {
            let gate = gate.clone();
            let tx = tx.clone();
            handles.push(tokio::spawn(async move {
                let _g = gate.acquire(42).await; // identical priority for all three
                tx.send(label).unwrap();
            }));
            tokio::task::yield_now().await; // deterministic registration order
        }
        assert_eq!(gate.waiting(), 3);

        drop(holder);
        assert_eq!(rx.recv().await, Some(1), "first enqueued, first served");
        assert_eq!(rx.recv().await, Some(2));
        assert_eq!(rx.recv().await, Some(3));

        for h in handles {
            h.await.unwrap();
        }
    }

    /// An idle high-priority caller that never calls `acquire()` reserves nothing: a low-priority
    /// caller freely takes and releases the permit every time, with no reservation held back for the
    /// absent high-priority instance (demand-adaptive).
    #[tokio::test]
    async fn demand_adaptive_idle_high_pri_reserves_nothing() {
        let gate = Arc::new(PriorityGate::new(1));
        for _ in 0..5 {
            let g = tokio::time::timeout(Duration::from_millis(50), gate.acquire(200))
                .await
                .expect("low-priority acquire is never blocked by an absent high-priority instance");
            assert_eq!(gate.available(), 0);
            assert_eq!(gate.waiting(), 0, "nothing is ever queued for the idle high-priority caller");
            drop(g);
        }
        assert_eq!(gate.available(), 1);
    }

    /// A queued waiter whose future is dropped (cancelled — e.g. instance shutdown / window abort)
    /// while still waiting is cleanly removed from the queue with no permit leaked.
    #[tokio::test]
    async fn cancelled_queued_waiter_is_removed_no_leak() {
        let gate = Arc::new(PriorityGate::new(1));
        let holder = gate.acquire(0).await;

        let g2 = gate.clone();
        let handle = tokio::spawn(async move {
            let _g = g2.acquire(50).await;
        });
        tokio::task::yield_now().await;
        assert_eq!(gate.waiting(), 1, "the second caller is contended and queued");

        handle.abort();
        let joined = handle.await;
        assert!(joined.is_err(), "the task was aborted (cancelled) while still queued");
        assert_eq!(gate.waiting(), 0, "cancellation removes the waiter from the queue");

        drop(holder);
        // The permit must still be usable — not lost to the cancelled waiter.
        let g3 = tokio::time::timeout(Duration::from_millis(50), gate.acquire(0))
            .await
            .expect("the permit is available after the holder released, unaffected by the cancellation");
        assert_eq!(gate.available(), 0);
        drop(g3);
        assert_eq!(gate.available(), 1);
    }

    /// The narrow grant-then-cancel race (module doc, "Cancellation safety" 3rd bullet): a waiter is
    /// handed a permit (its sender fires, removing it from the queue) but its future is dropped before
    /// it is ever polled again to observe that grant. The permit must be redistributed to the next
    /// waiter, not lost. Driven with manual polling (`tokio_test::task::spawn`) so the exact
    /// interleaving — poll to Pending, release, drop-without-repolling — is deterministic rather than
    /// timing-dependent.
    #[test]
    fn grant_then_cancel_redistributes_permit() {
        let gate = Arc::new(PriorityGate::new(0)); // nothing free -> both callers queue immediately

        let g1 = gate.clone();
        let g2 = gate.clone();
        let mut t1 = tokio_test::task::spawn(async move { g1.acquire(10).await }); // higher priority
        let mut t2 = tokio_test::task::spawn(async move { g2.acquire(100).await }); // lower priority

        assert!(t1.poll().is_pending());
        assert!(t2.poll().is_pending());
        assert_eq!(gate.waiting(), 2);

        // A (simulated) release hands the permit to t1, the highest-priority waiter.
        gate.release_one();
        assert_eq!(gate.waiting(), 1, "t1 was dequeued by the handoff");
        assert!(t1.is_woken(), "t1's task was woken by the grant");

        // t1's future is dropped WITHOUT being polled again — it never observes the grant and never
        // sets `granted = true`. This is exactly the leak-prone window `WaiterTicket::drop` must catch.
        drop(t1);

        assert!(t2.is_woken(), "the permit was redistributed to t2, not lost");
        match t2.poll() {
            std::task::Poll::Ready(_guard) => {}
            std::task::Poll::Pending => panic!("t2 should complete once handed the redistributed permit"),
        }
        assert_eq!(gate.waiting(), 0);
    }

    /// Heavy churn (many more acquire/hold/drop cycles than permits, across a range of priorities)
    /// never leaks or double-counts a permit.
    #[tokio::test]
    async fn no_permit_leak_under_churn() {
        const PERMITS: usize = 4;
        let gate = Arc::new(PriorityGate::new(PERMITS));
        let mut handles = Vec::new();
        for i in 0..200u32 {
            let gate = gate.clone();
            handles.push(tokio::spawn(async move {
                let _g = gate.acquire((i % 7) as i32).await;
                tokio::task::yield_now().await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(gate.available(), PERMITS, "every permit accounted for after churn");
        assert_eq!(gate.waiting(), 0);
    }

    /// Exactly one waiter is dequeued/woken per guard drop — a release never over-admits.
    #[tokio::test]
    async fn guard_drop_wakes_exactly_one_waiter() {
        let gate = Arc::new(PriorityGate::new(1));
        let holder = gate.acquire(0).await;

        let g2 = gate.clone();
        let g3 = gate.clone();
        let h2 = tokio::spawn(async move {
            let _g = g2.acquire(10).await;
            tokio::time::sleep(Duration::from_secs(3600)).await; // hold until aborted
        });
        let h3 = tokio::spawn(async move {
            let _g = g3.acquire(10).await;
            tokio::time::sleep(Duration::from_secs(3600)).await;
        });
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(gate.waiting(), 2);

        drop(holder);
        // Give the woken waiter's task a chance to actually run and take the permit.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(gate.waiting(), 1, "exactly one waiter was dequeued by the single release");
        assert_eq!(gate.available(), 0, "the permit was handed off directly, not returned to the pool");

        h2.abort();
        h3.abort();
    }
}

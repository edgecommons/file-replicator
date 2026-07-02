//! # file-replicator — per-file worker (DESIGN §8.1/§13/§20-B)
//!
//! Drives one claimed [`WorkItem`] through the crash-safe pipeline, fanning out to every configured
//! destination **independently**, and owns the retry/backoff and completion-action policy:
//!
//! ```text
//! for each destination (concurrently, independent retry/backoff):
//!   deliver → verify → persist DestPhase::Verified (write-ahead)
//!                   └─ error → record per-dest attempt + backoff → retry | dest Exhausted
//!
//! once EVERY destination is DestPhase::Verified:
//!   persist ItemState::Verified (write-ahead) → completion (delete|archive) → persist Completed
//!
//! if ANY destination permanently exhausts its retry budget:
//!   the item can never complete (even though other destinations already succeeded) →
//!   Exhausted → quarantine|retain, leaving already-`Verified` destinations' data intact
//! ```
//!
//! The **write-ahead** ordering is load-bearing (DESIGN §13.2/§20-B): each destination's
//! `DestPhase::Verified` is persisted *before* the aggregate item is promoted to `ItemState::Verified`,
//! which itself is persisted *before* the source side effect, and `Completed` *after* — so a crash at
//! any point is recovered idempotently by [`recover`](Worker::recover): an already-`Verified`
//! destination is never re-delivered (the destination object already matches — stable key → idempotent
//! overwrite), and the completion action (delete/archive) fires **exactly once**, only after the last
//! destination verifies.
//!
//! Failures are classified by [`ReplError`](crate::error::ReplError): *permanent* errors fail fast to
//! that destination's `Exhausted`; *transient*/*integrity* errors back off with **full-jitter
//! exponential** delay capped at `maxDelayMs`, retrying until the **time-based** `giveUpAfter` budget
//! (default 7 days) or an optional `maxAttempts` cap — built for hours-to-days offline
//! (DESIGN §13.4/§13.6). Every destination retries on its **own** clock (its own `attempts` and
//! `next_attempt_at`), so a slow/flaky destination never blocks — or is blocked by — another.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use ggcommons::metrics::MetricService;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::task::JoinSet;

use crate::config::{Collision, CompletionCfg, OnExhausted, OnSuccess, RetryCfg, Verify};
use crate::dest::SharedDestination;
use crate::domain::{Checksum, Delivered, DestPhase, ItemState, ProgressSink, ResumeState, WorkItem};
use crate::error::{ReplError, Result};
use crate::events::{Event, Events, ProgressThrottle};
use crate::ratelimit::Bandwidth;
use crate::state::{StatsDelta, StateStore};

use super::now_ms;

/// Persist streamed byte-progress at most once per this many bytes (keeps the DB write rate sane for
/// large files while still giving `get-status`/resume a recent-enough checkpoint).
const PROGRESS_STEP_BYTES: u64 = 4 * 1024 * 1024;

/// Default retry knobs (DESIGN §13.4): 1 s base, 15 min cap, 7-day give-up, no attempt cap.
const DEFAULT_BASE_DELAY_MS: u64 = 1_000;
const DEFAULT_MAX_DELAY_MS: u64 = 900_000;
const DEFAULT_GIVE_UP_AFTER_MS: i64 = 7 * 24 * 60 * 60 * 1_000;

/// The resolved retry/backoff policy for an instance (instance `retry` ▸ `global.defaults.retry` ▸
/// built-in defaults, field-by-field).
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// First-retry backoff (ms); doubles each attempt up to `max_delay_ms`.
    pub base_delay_ms: u64,
    /// Backoff cap (ms).
    pub max_delay_ms: u64,
    /// Time budget from first discovery before giving up (ms); `None` = never give up on time.
    pub give_up_after_ms: Option<i64>,
    /// Optional hard attempt cap; `None` = time-governed only.
    pub max_attempts: Option<u32>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            base_delay_ms: DEFAULT_BASE_DELAY_MS,
            max_delay_ms: DEFAULT_MAX_DELAY_MS,
            give_up_after_ms: Some(DEFAULT_GIVE_UP_AFTER_MS),
            max_attempts: None,
        }
    }
}

/// The retry-vs-give-up decision for a failed attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    /// Back off; the item returns to `Ready` and is re-claimable at `next_attempt_at`.
    Retry { next_attempt_at: i64 },
    /// Budget/attempts exhausted (or a permanent error): move to `Exhausted`.
    GiveUp,
}

impl RetryPolicy {
    /// Resolve the effective policy from the instance's own `retry` and the global default, each
    /// field independently: instance ▸ global ▸ default.
    pub fn resolve(instance: Option<&RetryCfg>, global: Option<&RetryCfg>) -> Self {
        let def = RetryPolicy::default();
        let base = instance
            .and_then(|r| r.base_delay_ms)
            .or_else(|| global.and_then(|r| r.base_delay_ms))
            .unwrap_or(def.base_delay_ms)
            .max(1);
        let max = instance
            .and_then(|r| r.max_delay_ms)
            .or_else(|| global.and_then(|r| r.max_delay_ms))
            .unwrap_or(def.max_delay_ms)
            .max(base);
        let give_up_after_ms = match instance
            .and_then(|r| r.give_up_after.clone())
            .or_else(|| global.and_then(|r| r.give_up_after.clone()))
        {
            Some(s) => match parse_duration_ms(&s) {
                Some(ms) => Some(ms as i64),
                None => {
                    tracing::warn!(value = %s, "invalid giveUpAfter; using default");
                    def.give_up_after_ms
                }
            },
            None => def.give_up_after_ms,
        };
        let max_attempts = instance
            .and_then(|r| r.max_attempts)
            .or_else(|| global.and_then(|r| r.max_attempts));
        RetryPolicy {
            base_delay_ms: base,
            max_delay_ms: max,
            give_up_after_ms,
            max_attempts,
        }
    }

    /// Full-jitter exponential backoff for the `attempts`-th attempt (1 = first failure): a uniformly
    /// random delay in `[0, min(base·2^(attempts-1), maxDelay)]` (AWS "full jitter"; DESIGN §13.6).
    pub fn backoff_ms(&self, attempts: u32, rng: &mut impl Rng) -> u64 {
        let shift = attempts.saturating_sub(1).min(32);
        let exp = self.base_delay_ms.saturating_mul(1u64 << shift);
        let cap = exp.min(self.max_delay_ms).max(1);
        rng.gen_range(0..=cap)
    }

    /// Decide retry vs give-up after a failed attempt. `permanent` short-circuits to `GiveUp`;
    /// otherwise give up once `maxAttempts` is reached or `now - discovered_at ≥ giveUpAfter`.
    pub fn decide(
        &self,
        item: &WorkItem,
        permanent: bool,
        now: i64,
        rng: &mut impl Rng,
    ) -> RetryDecision {
        let attempts = item.attempts.saturating_add(1);
        if permanent {
            return RetryDecision::GiveUp;
        }
        if let Some(cap) = self.max_attempts {
            if attempts >= cap {
                return RetryDecision::GiveUp;
            }
        }
        if let Some(budget) = self.give_up_after_ms {
            if now.saturating_sub(item.discovered_at) >= budget {
                return RetryDecision::GiveUp;
            }
        }
        let delay = self.backoff_ms(attempts, rng) as i64;
        RetryDecision::Retry {
            next_attempt_at: now.saturating_add(delay),
        }
    }
}

/// Parse a human duration into milliseconds: a bare number = ms, or a suffix `ms`/`s`/`m`/`h`/`d`.
/// e.g. `"7d"` → 604_800_000, `"15m"` → 900_000, `"500ms"` → 500, `"1.5h"` → 5_400_000.
pub fn parse_duration_ms(input: &str) -> Option<u64> {
    let s = input.trim().to_ascii_lowercase();
    if s.is_empty() {
        return None;
    }
    // Check "ms" before the single-letter suffixes so "500ms" is milliseconds, not minutes.
    let (num, mult) = if let Some(x) = s.strip_suffix("ms") {
        (x, 1u64)
    } else if let Some(x) = s.strip_suffix('s') {
        (x, 1_000)
    } else if let Some(x) = s.strip_suffix('m') {
        (x, 60_000)
    } else if let Some(x) = s.strip_suffix('h') {
        (x, 3_600_000)
    } else if let Some(x) = s.strip_suffix('d') {
        (x, 86_400_000)
    } else {
        (s.as_str(), 1)
    };
    let v: f64 = num.trim().parse().ok()?;
    if v < 0.0 || !v.is_finite() {
        return None;
    }
    Some((v * mult as f64).round() as u64)
}

/// One configured destination, as the worker sees it: the [`SharedDestination`] handle plus its
/// stable **label** — the `dest` key under which per-destination [`ResumeState`]/[`DestState`]
/// (DESIGN §20-B) is persisted. The label is [`Destination::kind()`](crate::dest::Destination::kind)
/// when it is unique among the instance's configured destinations (the N=1 case, and the common N>1
/// case of distinct backend kinds); when two-or-more destinations share a `kind()` (e.g. two `s3`
/// egresses to different buckets) it is disambiguated as `"{kind}#{n}"` (1-based occurrence order) so
/// their resume/completion bookkeeping never collides. See [`label_destinations`].
#[derive(Clone)]
struct DestSlot {
    label: String,
    dest: SharedDestination,
}

/// Assign each destination its stable [`DestSlot::label`] (see there for the disambiguation rule).
/// Order-preserving; `N=1` always yields exactly `[kind()]`, identical to the pre-P6 single-destination
/// resume/completion key.
fn label_destinations(dests: &[SharedDestination]) -> Vec<String> {
    let mut counts: HashMap<&'static str, usize> = HashMap::new();
    for d in dests {
        *counts.entry(d.kind()).or_insert(0) += 1;
    }
    let mut seen: HashMap<&'static str, usize> = HashMap::new();
    dests
        .iter()
        .map(|d| {
            let kind = d.kind();
            if counts[kind] == 1 {
                kind.to_string()
            } else {
                let n = seen.entry(kind).or_insert(0);
                *n += 1;
                format!("{kind}#{n}")
            }
        })
        .collect()
}

/// The per-file driver for one instance. Shared (`Arc<Worker>`) across the instance's worker tasks.
/// Fans a claimed item out to every configured [`DestSlot`] (DESIGN §20-B): each destination retries
/// independently, and the source completion action fires exactly once, after the last one verifies.
pub struct Worker {
    instance: String,
    store: Arc<dyn StateStore>,
    dests: Vec<DestSlot>,
    completion: CompletionCfg,
    /// Source root — used to rebuild each item's absolute path (the store keeps only `relpath`).
    ingress_root: PathBuf,
    bw: Bandwidth,
    retry: RetryPolicy,
    /// Backoff jitter source (seedable for deterministic tests); shared (`Arc`) so it can be cloned
    /// into the per-destination fan-out tasks ([`run_one_dest`]) without borrowing `self`.
    rng: Arc<Mutex<StdRng>>,
    /// Optional metric emitter (`filesReplicated`/`bytesReplicated`/`filesFailed`).
    metrics: Option<Arc<dyn MetricService>>,
    /// UNS event emitter for the per-file lifecycle events (`ReplicationStarted`/`…Progress`/
    /// `…Completed`/`…Failed`/`FileDeleted`/`FileArchived`/`RetriesExhausted`/`FileQuarantined`,
    /// DESIGN §17). A no-op [`Events::disabled`] by default, so a worker built without messaging (or
    /// in a unit test) runs the exact P1/P2 pipeline with zero event overhead.
    events: Events,
}

impl Worker {
    /// Build a single-destination worker. `ingress_root` is the instance's source directory; `bw` is
    /// the per-instance ∩ global bandwidth governor; `retry` is the resolved policy. A thin wrapper
    /// over [`new_multi`](Self::new_multi) with a one-element destination list — by construction this
    /// is byte-for-byte the N=1 case of the fan-out pipeline (DESIGN §20-B "preserve N=1 exactly").
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        instance: String,
        store: Arc<dyn StateStore>,
        dest: SharedDestination,
        completion: CompletionCfg,
        ingress_root: PathBuf,
        bw: Bandwidth,
        retry: RetryPolicy,
        metrics: Option<Arc<dyn MetricService>>,
    ) -> Self {
        Self::new_multi(
            instance,
            store,
            vec![dest],
            completion,
            ingress_root,
            bw,
            retry,
            metrics,
        )
    }

    /// Build a worker that fans a claimed item out to every destination in `dests` (P6, DESIGN §20-B).
    /// `dests` must be non-empty (an empty instance-level egress list is rejected earlier, at config
    /// validation — see [`crate::instance::Instance::build`]).
    #[allow(clippy::too_many_arguments)]
    pub fn new_multi(
        instance: String,
        store: Arc<dyn StateStore>,
        dests: Vec<SharedDestination>,
        completion: CompletionCfg,
        ingress_root: PathBuf,
        bw: Bandwidth,
        retry: RetryPolicy,
        metrics: Option<Arc<dyn MetricService>>,
    ) -> Self {
        let labels = label_destinations(&dests);
        let dests = labels
            .into_iter()
            .zip(dests)
            .map(|(label, dest)| DestSlot { label, dest })
            .collect();
        Worker {
            instance,
            store,
            dests,
            completion,
            ingress_root,
            bw,
            retry,
            rng: Arc::new(Mutex::new(StdRng::from_entropy())),
            metrics,
            events: Events::disabled(),
        }
    }

    /// Attach the UNS event emitter (the P3 control-plane wiring path, [`crate::app`]). Consumes and
    /// returns `self` so it composes in [`crate::instance::Instance::build_with_dest`] before the
    /// worker is shared behind an `Arc`. With the default [`Events::disabled`] every emit is a no-op.
    pub fn with_events(mut self, events: Events) -> Self {
        self.events = events;
        self
    }

    /// This worker's configured destination labels, joined for diagnostics (error sidecars, logs) —
    /// e.g. `"local"` (N=1) or `"local+s3"` (N=2). See [`DestSlot::label`].
    fn dest_labels_joined(&self) -> String {
        self.dests
            .iter()
            .map(|s| s.label.as_str())
            .collect::<Vec<_>>()
            .join("+")
    }

    /// The absolute source path for `item`, rebuilt from the ingress root and the stored `relpath`
    /// (traversal segments dropped). Never trusts `item.abs_source`, which a store round-trip loses.
    fn abs_source(&self, item: &WorkItem) -> PathBuf {
        join_rel(&self.ingress_root, &item.relpath)
    }

    /// Process one claimed (`InProgress`) item to a terminal (or `Failed` retry) state, emitting
    /// metrics. A durable-store fault is logged and the item is left as-is for the next recovery pass.
    pub async fn process_item(&self, item: &WorkItem, now: i64) -> ItemState {
        // The store returns `relpath` only; rebuild the live absolute source path for delivery.
        let mut item = item.clone();
        item.abs_source = self.abs_source(&item);

        // Per-destination `ReplicationStarted` events are emitted from `run_one_dest`, right before
        // that destination's own attempt (DESIGN §17.1) — an already-`Verified` destination this round
        // (a partial-fan-out re-claim) is skipped and emits nothing, so `attempt` always reflects that
        // ONE destination's own attempt count, not the aggregate item's.

        let state = match self.run_pipeline(&item, now).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    instance = %self.instance, relpath = %item.relpath, error = %e,
                    "durable-store fault while processing item; leaving for recovery"
                );
                item.state
            }
        };
        self.emit(state, item.size).await;
        state
    }

    /// The fan-out deliver → verify → completion pipeline (DESIGN §13.2/§20-B). Every not-yet-
    /// [`DestPhase::Verified`] destination whose OWN backoff clock is due (its `next_attempt_at ≤ now`) is
    /// attempted concurrently, independently retried/backed off; a destination still waiting on its own
    /// gate is left untouched this round. The aggregate item completes exactly once, after the LAST
    /// destination verifies. Returns the terminal (or `Failed`-awaiting-retry) state; `Err` is a
    /// durable-store fault only (destination transfer errors are handled internally, per destination).
    ///
    /// **Concurrency:** the caller already holds ONE queue concurrency slot for this item (acquired per
    /// file in [`crate::instance::Instance::run_batch`]); the per-destination attempts here run
    /// concurrently under that single slot, so `maxConcurrentFiles` bounds in-flight FILES, not
    /// per-destination transfers (up to `maxConcurrentFiles × N`). Aggregate throughput stays bounded
    /// regardless by the global bandwidth token bucket (DESIGN §13.5), and a hung destination pins only
    /// its own item's slot.
    async fn run_pipeline(&self, item: &WorkItem, now: i64) -> Result<ItemState> {
        let existing = self.store.dest_states(&self.instance, &item.relpath)?;
        let phase_of = |label: &str| -> DestPhase {
            existing
                .iter()
                .find(|d| d.dest == label)
                .map(|d| d.phase)
                .unwrap_or(DestPhase::Pending)
        };
        let attempts_of = |label: &str| -> u32 {
            existing
                .iter()
                .find(|d| d.dest == label)
                .map(|d| d.attempts)
                .unwrap_or(0)
        };
        // Each destination's OWN backoff gate (`0` when it has no row yet → never attempted → always
        // due). Honoring this per-destination is what makes retries independent (FR-EGR-3 / DESIGN
        // §20-B): a destination in a long backoff must NOT be re-attempted just because a faster
        // sibling's shorter gate elapsed and re-claimed the aggregate item.
        let next_attempt_of = |label: &str| -> i64 {
            existing
                .iter()
                .find(|d| d.dest == label)
                .map(|d| d.next_attempt_at)
                .unwrap_or(0)
        };

        let mut verified: HashSet<String> = self
            .dests
            .iter()
            .filter(|s| phase_of(&s.label) == DestPhase::Verified)
            .map(|s| s.label.clone())
            .collect();

        // Split the not-yet-`Verified` destinations by whether their own backoff clock is DUE this
        // round. Only the `due` ones are (re-)attempted; a `not_due` destination is left exactly as it
        // is so its independent backoff is preserved across the re-claim, and its gate still feeds the
        // aggregate re-claim time computed below.
        let mut due: Vec<&DestSlot> = Vec::new();
        let mut not_due: Vec<&DestSlot> = Vec::new();
        for slot in &self.dests {
            if verified.contains(&slot.label) {
                continue;
            }
            if next_attempt_of(&slot.label) <= now {
                due.push(slot);
            } else {
                not_due.push(slot);
            }
        }

        let mut giveups: Vec<(String, String, u32)> = Vec::new(); // (label, err, attempts)
        let mut retries: Vec<(String, i64, String)> = Vec::new(); // (label, next_attempt_at, err)

        if !due.is_empty() {
            let mut set: JoinSet<Result<(String, DestOutcome)>> = JoinSet::new();
            for slot in &due {
                let ctx = DestAttemptCtx {
                    instance: self.instance.clone(),
                    item: item.clone(),
                    label: slot.label.clone(),
                    dest: slot.dest.clone(),
                    store: self.store.clone(),
                    bw: self.bw.clone(),
                    retry: self.retry,
                    verify_policy: self.completion.verify,
                    events: self.events.clone(),
                    now,
                    attempts_so_far: attempts_of(&slot.label),
                    rng: self.rng.clone(),
                };
                set.spawn(run_one_dest(ctx));
            }
            while let Some(res) = set.join_next().await {
                match res {
                    Ok(Ok((label, DestOutcome::Verified))) => {
                        verified.insert(label);
                    }
                    Ok(Ok((label, DestOutcome::Retry { next_attempt_at, err }))) => {
                        retries.push((label, next_attempt_at, err));
                    }
                    Ok(Ok((label, DestOutcome::GiveUp { err, attempts }))) => {
                        giveups.push((label, err, attempts));
                    }
                    Ok(Err(e)) => return Err(e), // a store fault — leave the item as-is for recovery
                    Err(join_err) => {
                        tracing::error!(
                            instance = %self.instance, relpath = %item.relpath, error = %join_err,
                            "a destination fan-out task failed to run"
                        );
                        return Err(ReplError::Transient(format!(
                            "destination task join failed: {join_err}"
                        )));
                    }
                }
            }
        }

        if !giveups.is_empty() {
            // One or more destinations permanently exhausted their retry budget: the item can never
            // complete (FR-EGR-3(d)) even though other destinations may already be `Verified`. Every
            // still-pending destination — whether it retried this round (`retries`) or is only waiting
            // on its own backoff (`not_due`) — has no further chance now the item is going terminal, so
            // abort its partial transfer and drop the checkpoint too (mirrors the single-destination
            // give-up cleanup); an already-`Verified` destination's delivered data is left untouched.
            for (label, _, _) in &retries {
                if let Some(slot) = self.dests.iter().find(|s| &s.label == label) {
                    self.abort_and_clear_resume(&slot.label, &slot.dest, item).await;
                }
            }
            for slot in &not_due {
                self.abort_and_clear_resume(&slot.label, &slot.dest, item).await;
            }
            let attempts = giveups
                .iter()
                .map(|(_, _, a)| *a)
                .max()
                .unwrap_or_else(|| item.attempts.saturating_add(1));
            let err_str = giveups
                .iter()
                .map(|(label, err, _)| format!("{label}: {err}"))
                .collect::<Vec<_>>()
                .join("; ");
            self.store.roll_up_item(
                &self.instance,
                &item.relpath,
                ItemState::Exhausted,
                attempts,
                now,
                Some(&err_str),
                now,
            )?;
            // Count the file's failure EXACTLY ONCE (FR): only when a destination NEWLY exhausts this
            // round (was not already `Exhausted` in the durable state). A manual re-Ready of an
            // already-terminal item that re-attempts and re-exhausts the same destination must not
            // re-increment `filesFailed` — the file already counted as failed on its first exhaustion.
            let newly_exhausted = giveups.iter().any(|(label, _, _)| {
                !existing
                    .iter()
                    .any(|d| &d.dest == label && d.phase == DestPhase::Exhausted)
            });
            if newly_exhausted {
                self.store.bump_stats(
                    &self.instance,
                    StatsDelta {
                        replicated: 0,
                        failed: 1,
                        bytes: 0,
                    },
                )?;
            }
            tracing::error!(
                instance = %self.instance, relpath = %item.relpath, attempts, error = %err_str,
                "retries exhausted on one or more destinations"
            );
            return self.finish_exhausted(item, &err_str, attempts, now).await;
        }

        if verified.len() == self.dests.len() {
            // Every configured destination is verified — write-ahead the aggregate `Verified` state
            // (DESIGN §13.2), then run the completion action exactly once.
            self.store
                .set_state(&self.instance, &item.relpath, ItemState::Verified, now)?;
            return self.complete_verified(item, now).await;
        }

        // At least one destination is still pending (a retry scheduled this round, or one only skipped
        // because its own backoff clock has not elapsed): roll up the aggregate as `Failed`, gated at
        // the EARLIEST of EVERY still-pending destination's own next attempt — the ones retried this
        // round AND the ones left as not-yet-due — so the item is re-claimed exactly when the soonest
        // destination is next due (a claim re-runs the pipeline, which skips every already-`Verified`
        // and every still-not-due destination, only attempting what is actually due).
        let next_attempt_at = retries
            .iter()
            .map(|(_, t, _)| *t)
            .chain(not_due.iter().map(|s| next_attempt_of(&s.label)))
            .min()
            .unwrap_or(now);
        let last_error = retries
            .iter()
            .map(|(_, _, e)| e.clone())
            .next_back()
            .or_else(|| item.last_error.clone());
        let attempts = item.attempts.saturating_add(1);
        self.store.roll_up_item(
            &self.instance,
            &item.relpath,
            ItemState::Failed,
            attempts,
            next_attempt_at,
            last_error.as_deref(),
            now,
        )?;
        Ok(ItemState::Failed)
    }

    /// On give-up (terminal), abort any in-flight partial transfer at `dest` (S3 →
    /// `AbortMultipartUpload`; local → temp cleanup) so no orphaned upload lingers, then drop that
    /// destination's resume checkpoint. Resume is deliberately **kept** on the retry path (so the next
    /// attempt resumes) and cleared only here, at that destination's terminal give-up or at the item's
    /// aggregate give-up (DESIGN §11.3/§13.4). Idempotent + best-effort.
    async fn abort_and_clear_resume(&self, label: &str, dest: &SharedDestination, item: &WorkItem) {
        match self.store.load_resume(&self.instance, &item.relpath, label) {
            Ok(Some(resume)) => {
                if let Err(e) = dest.abort(item, &resume).await {
                    tracing::warn!(
                        relpath = %item.relpath, dest = %label, error = %e,
                        "aborting partial transfer on give-up failed"
                    );
                }
            }
            Ok(None) => {}
            Err(e) => tracing::warn!(
                relpath = %item.relpath, dest = %label, error = %e,
                "load resume for give-up abort failed"
            ),
        }
        let _ = self.store.clear_resume(&self.instance, &item.relpath, label);
    }

    /// Run the success completion action (`delete` | `archive`) on the source, then persist
    /// `Completed`. Idempotent: a missing source (already completed before a crash) is fine. Filesystem
    /// errors are logged but still advance to `Completed` — the delivery succeeded and the durable
    /// `Completed` marker stops the file being re-discovered, so a stale source can never loop.
    ///
    /// The source-side filesystem work runs on the blocking pool: a cross-device archive falls back to
    /// a whole-file copy, which must not block a shared async worker thread (DESIGN §6.3).
    async fn complete_verified(&self, item: &WorkItem, now: i64) -> Result<ItemState> {
        let src = self.abs_source(item);
        let on_success = self.completion.on_success;
        let archive_dir = self.completion.archive_dir.clone();
        let collision = self.completion.on_collision;
        let relpath = item.relpath.clone();
        let instance = self.instance.clone();
        if let Err(e) = tokio::task::spawn_blocking(move || {
            apply_success_action(on_success, &src, archive_dir.as_deref(), &relpath, collision, &instance)
        })
        .await
        {
            tracing::warn!(
                instance = %self.instance, relpath = %item.relpath, error = %e,
                "completion task join failed; marking complete anyway (delivery already succeeded)"
            );
        }
        self.store
            .set_state(&self.instance, &item.relpath, ItemState::Completed, now)?;
        tracing::info!(
            instance = %self.instance, relpath = %item.relpath,
            action = ?self.completion.on_success, "item completed; source side-effect applied"
        );
        // Clear every destination's resume checkpoint + per-destination completion bookkeeping only
        // AFTER Completed is durable, so a crash in the aggregate Verified→Completed gap still finds
        // the expected-checksum checkpoints and per-dest `Verified` phases recovery needs (DESIGN §20-B).
        let _ = self.store.clear_resume_all(&self.instance, &item.relpath);
        let _ = self.store.clear_dest_states(&self.instance, &item.relpath);
        self.store.bump_stats(
            &self.instance,
            StatsDelta {
                replicated: 1,
                failed: 0,
                bytes: item.size as i64,
            },
        )?;

        // Lifecycle events (DESIGN §17.1): the source side effect that ran. `ReplicationCompleted` was
        // already emitted per destination (by `run_one_dest`, or by `recover_verified` re-verifying on
        // recovery) — this is the aggregate/source-side event, fired exactly once.
        match self.completion.on_success {
            OnSuccess::Delete => {
                self.events
                    .instance_event(
                        &self.instance,
                        Event::FileDeleted {
                            path: item.relpath.clone(),
                        },
                        now,
                    )
                    .await;
            }
            OnSuccess::Archive => {
                let archive_path = self
                    .completion
                    .archive_dir
                    .as_ref()
                    .map(|d| join_rel(d, &item.relpath).display().to_string());
                self.events
                    .instance_event(
                        &self.instance,
                        Event::FileArchived {
                            path: item.relpath.clone(),
                            archive_path,
                        },
                        now,
                    )
                    .await;
            }
        }
        Ok(ItemState::Completed)
    }

    /// Terminal handling for an `Exhausted` item: `quarantine` (move to `failedDir` + an
    /// `.error.json` sidecar) or `retainInPlace` (leave it, mark `Retained`). Idempotent for recovery.
    /// The quarantine move + sidecar write run on the blocking pool (cross-device moves copy).
    async fn finish_exhausted(
        &self,
        item: &WorkItem,
        last_error: &str,
        attempts: u32,
        now: i64,
    ) -> Result<ItemState> {
        match self.completion.on_exhausted {
            OnExhausted::RetainInPlace => {
                self.store
                    .set_state(&self.instance, &item.relpath, ItemState::Retained, now)?;
                Ok(ItemState::Retained)
            }
            OnExhausted::Quarantine => {
                let failed_dir = self.completion.failed_dir.clone();
                let collision = self.completion.on_collision;
                let src = self.abs_source(item);
                let ctx = QuarantineCtx {
                    instance: self.instance.clone(),
                    relpath: item.relpath.clone(),
                    last_error: last_error.to_string(),
                    attempts,
                    first_seen: item.discovered_at,
                    last_try: now,
                    destination: self.dest_labels_joined(),
                    bytes_done: item.bytes_done,
                };
                if let Err(e) = tokio::task::spawn_blocking(move || {
                    apply_quarantine_action(failed_dir.as_deref(), &src, collision, &ctx)
                })
                .await
                {
                    tracing::warn!(
                        instance = %self.instance, relpath = %item.relpath, error = %e,
                        "quarantine task join failed"
                    );
                }
                self.store
                    .set_state(&self.instance, &item.relpath, ItemState::Quarantined, now)?;
                let quarantine_path = self
                    .completion
                    .failed_dir
                    .as_ref()
                    .map(|d| join_rel(d, &item.relpath).display().to_string());
                self.events
                    .instance_event(
                        &self.instance,
                        Event::FileQuarantined {
                            path: item.relpath.clone(),
                            attempts,
                            last_error: last_error.to_string(),
                            quarantine_path,
                        },
                        now,
                    )
                    .await;
                Ok(ItemState::Quarantined)
            }
        }
    }

    /// Crash recovery (DESIGN §13.2), idempotent. `Verified` items are **re-verified against the
    /// destination before** the source side effect (see [`recover_verified`](Self::recover_verified));
    /// `InProgress` items return to `Ready` for idempotent re-delivery; `Exhausted` items (a crash
    /// before their terminal action) re-run `onExhausted`.
    pub async fn recover(&self, now: i64) -> Result<()> {
        for it in self.store.recover_incomplete(&self.instance)? {
            match it.state {
                ItemState::Verified => self.recover_verified(&it, now).await?,
                ItemState::InProgress => {
                    // Before re-readying, re-verify any destination a PRIOR session already marked
                    // `DestPhase::Verified` (fan-out narrows the §13.2 crash window — see
                    // [`reverify_persisted_verified`]): the next claim's `run_pipeline` trusts and skips
                    // a `Verified` destination, so one whose object was lost/corrupted during downtime
                    // must be caught here or completion would delete the only copy (FR-CMP-3/4).
                    self.reverify_persisted_verified(&it, now).await?;
                    tracing::info!(instance = %self.instance, relpath = %it.relpath, "recovering InProgress → Ready (re-deliver)");
                    self.store
                        .set_state(&self.instance, &it.relpath, ItemState::Ready, now)?;
                }
                _ => {}
            }
        }
        for it in self.store.list_by_state(&self.instance, ItemState::Exhausted)? {
            let last = it.last_error.clone().unwrap_or_else(|| "exhausted".to_string());
            let _ = self.finish_exhausted(&it, &last, it.attempts, now).await?;
        }
        Ok(())
    }

    /// Recover one `Verified` item (DESIGN §13.2). The write-ahead `Verified` marker was persisted
    /// *before* the source delete/archive, so on restart the object should already be live at its
    /// stable key — but the destination could have been lost/corrupted during the downtime (a wiped
    /// NAS mount, a bucket-lifecycle deletion, a truncated/tampered object). Completing (deleting/
    /// archiving the source) without checking would destroy the only copy. So **re-verify the
    /// destination first**, using the persisted expected result:
    ///
    /// - if the `Verified` checkpoint holds the full [`Delivered`] (this build), re-verify by
    ///   **checksum** (local re-hashes the object; S3 compares the stored object checksum) — this
    ///   catches silent corruption a size check would miss;
    /// - for a legacy row written before the checkpoint refinement, fall back to a **size** check.
    ///
    /// On success, complete (delete/archive the source); on failure, requeue to `Ready` for
    /// idempotent re-delivery and clear the stale checkpoint — never delete the only copy (FR-CMP-3).
    ///
    /// Also invoked outside crash recovery by a window-close **pauseResume** abort
    /// ([`crate::instance::Instance::run_batch_windowed`]): if the abort lands in the
    /// `Verified → Completed` gap, the item is driven to completion here rather than being stranded
    /// non-terminal until the next process restart (the abort only reverts still-`InProgress` rows).
    ///
    /// **Fan-out (DESIGN §20-B):** re-verifies **every** configured destination, independently, using
    /// each one's own persisted per-destination checkpoint. A destination that re-verifies clean is
    /// (re-)marked [`DestPhase::Verified`] (idempotent — also backfills a legacy pre-P6 row with no
    /// `dest_state` history). If they ALL re-verify, completion runs exactly once; if even one fails
    /// re-verification, that destination alone is demoted to `Pending` (its checkpoint cleared) and the
    /// AGGREGATE item reverts to `Ready` — the next claim skips every other, already-re-verified
    /// destination and re-delivers only the one that failed (no re-delivery, no double completion).
    pub(crate) async fn recover_verified(&self, it: &WorkItem, now: i64) -> Result<()> {
        let mut all_ok = true;
        for slot in &self.dests {
            if !self.reverify_one_dest(slot, it, now).await? {
                all_ok = false;
            }
        }

        if all_ok {
            tracing::info!(instance = %self.instance, relpath = %it.relpath, "recovering Verified → every destination re-verified, completing");
            let _ = self.complete_verified(it, now).await?;
        } else {
            self.store
                .set_state(&self.instance, &it.relpath, ItemState::Ready, now)?;
        }
        Ok(())
    }

    /// Re-verify ONE destination against its persisted checkpoint (DESIGN §13.2/§20-B) — the shared
    /// unit of both aggregate-`Verified` recovery ([`recover_verified`](Self::recover_verified)) and the
    /// per-destination re-check on `InProgress` recovery
    /// ([`reverify_persisted_verified`](Self::reverify_persisted_verified)). The write-ahead `Verified`
    /// marker was persisted *before* any source side effect, so on restart the object should already be
    /// live at its stable key — but it could have been lost/corrupted during the downtime, and trusting
    /// it blindly would risk deleting the only copy (FR-CMP-3). So re-verify against the persisted
    /// expected result:
    ///
    /// - a full [`Delivered`] checkpoint (this build) → re-verify by **checksum** (local re-hashes the
    ///   object, S3 compares the stored object checksum) — catches silent corruption a size check misses;
    /// - a legacy row with no checkpoint → fall back to a **size** check.
    ///
    /// On a clean re-verify, (re-)mark the destination [`DestPhase::Verified`] (idempotent; also backfills
    /// a legacy pre-P6 row) and return `true`. On failure, demote it to [`DestPhase::Pending`], clear its
    /// stale checkpoint, and return `false` — the next claim then re-delivers ONLY that destination.
    async fn reverify_one_dest(&self, slot: &DestSlot, it: &WorkItem, now: i64) -> Result<bool> {
        let resume = self
            .store
            .load_resume(&self.instance, &it.relpath, &slot.label)?;
        let persisted: Option<Delivered> = resume
            .as_ref()
            .and_then(|r| r.token.get("verified").cloned())
            .and_then(|v| serde_json::from_value(v).ok());

        let (expected, policy) = match persisted {
            Some(d) => (d, Verify::Checksum),
            None => (
                Delivered {
                    bytes: it.size,
                    checksum: Checksum::None,
                    handle: serde_json::Value::Null,
                },
                Verify::Size,
            ),
        };

        match slot.dest.verify(it, &expected, policy).await {
            Ok(()) => {
                tracing::info!(
                    instance = %self.instance, relpath = %it.relpath, dest = %slot.label,
                    "recovery: destination re-verified"
                );
                self.store.set_dest_phase(
                    &self.instance,
                    &it.relpath,
                    &slot.label,
                    DestPhase::Verified,
                    now,
                )?;
                Ok(true)
            }
            Err(e) => {
                tracing::warn!(
                    instance = %self.instance, relpath = %it.relpath, dest = %slot.label, error = %e,
                    "recovery: destination failed re-verification; requeuing that destination"
                );
                self.store.set_dest_phase(
                    &self.instance,
                    &it.relpath,
                    &slot.label,
                    DestPhase::Pending,
                    now,
                )?;
                let _ = self
                    .store
                    .clear_resume(&self.instance, &it.relpath, &slot.label);
                Ok(false)
            }
        }
    }

    /// Re-verify every destination a PRIOR session persisted as [`DestPhase::Verified`] on an item that
    /// crashed while still `InProgress`, BEFORE it is re-readied (DESIGN §13.2/§20-B, FR-CMP-3/4).
    ///
    /// Fan-out widens the crash window [`recover_verified`](Self::recover_verified) guards: a destination
    /// can reach `DestPhase::Verified` (its own write-ahead) while the AGGREGATE item is still
    /// `InProgress` (other destinations pending). The next claim's
    /// [`run_pipeline`](Self::run_pipeline) SKIPS every `Verified` destination — so if such a
    /// destination's object was lost/corrupted during downtime, the item would later complete
    /// (delete/archive the source) trusting a stale row and silently short that destination. Re-verifying
    /// here closes that gap for the InProgress window exactly as `recover_verified` does for the
    /// aggregate-`Verified` window: a destination that fails is demoted to `Pending` (checkpoint cleared)
    /// so the subsequent claim re-delivers only it; a clean one keeps its `Verified` phase and is
    /// correctly skipped. Only destinations currently marked `Verified` are re-checked — a still-`Pending`
    /// or `Exhausted` destination is (re-)attempted by the pipeline anyway.
    async fn reverify_persisted_verified(&self, it: &WorkItem, now: i64) -> Result<()> {
        let existing = self.store.dest_states(&self.instance, &it.relpath)?;
        for slot in &self.dests {
            let is_verified = existing
                .iter()
                .any(|d| d.dest == slot.label && d.phase == DestPhase::Verified);
            if is_verified {
                let _ = self.reverify_one_dest(slot, it, now).await?;
            }
        }
        Ok(())
    }

    /// Emit the completion/failure metric for a terminal state (best-effort).
    async fn emit(&self, state: ItemState, size: u64) {
        let Some(m) = &self.metrics else { return };
        let values: Option<HashMap<String, f64>> = match state {
            ItemState::Completed => Some(HashMap::from([
                ("filesReplicated".to_string(), 1.0),
                ("bytesReplicated".to_string(), size as f64),
            ])),
            ItemState::Quarantined | ItemState::Retained => {
                Some(HashMap::from([("filesFailed".to_string(), 1.0)]))
            }
            _ => None,
        };
        if let Some(v) = values {
            let _ = m.emit_metric("fileReplicator", v).await;
        }
    }
}

/// The result of one destination's independent deliver→verify attempt within a fan-out round
/// (DESIGN §20-B), returned by [`run_one_dest`].
enum DestOutcome {
    /// Delivered + integrity-verified; the caller write-ahead persists [`DestPhase::Verified`] before
    /// this returns (so the caller only needs to fold the result into the aggregate rollup).
    Verified,
    /// A transient/integrity failure within this destination's own retry budget.
    Retry { next_attempt_at: i64, err: String },
    /// This destination's own retry budget (`maxAttempts`/`giveUpAfter`) is exhausted, or the error was
    /// permanent — this destination can never complete without operator intervention.
    GiveUp { err: String, attempts: u32 },
}

/// Owned inputs for one destination's fan-out attempt ([`run_one_dest`]) — everything a spawned,
/// `'static` task needs, cloned out of the [`Worker`] up front so the attempt never borrows `self`
/// (letting every pending destination run concurrently in its own [`tokio::task::JoinSet`] entry).
struct DestAttemptCtx {
    instance: String,
    item: WorkItem,
    label: String,
    dest: SharedDestination,
    store: Arc<dyn StateStore>,
    bw: Bandwidth,
    retry: RetryPolicy,
    verify_policy: Verify,
    events: Events,
    now: i64,
    /// This destination's attempts-so-far (from its [`DestState`](crate::domain::DestState), `0` if
    /// this is its first attempt) — the per-destination analogue of [`WorkItem::attempts`].
    attempts_so_far: u32,
    rng: Arc<Mutex<StdRng>>,
}

/// Run one destination's independent deliver → verify attempt (DESIGN §20-B): the fan-out unit spawned
/// concurrently, once per not-yet-`Verified` destination, by [`Worker::run_pipeline`]. Persists that
/// destination's own write-ahead checkpoint / [`DestPhase`] / retry bookkeeping and emits that
/// destination's own lifecycle events — mirroring, per destination, exactly what the pre-P6
/// single-destination pipeline did for its one destination. `Err` is a durable-store fault (propagated
/// so the caller leaves the item as-is for the next recovery pass); a destination transfer error is
/// always resolved into a `Retry`/`GiveUp` outcome, never an `Err`.
async fn run_one_dest(ctx: DestAttemptCtx) -> Result<(String, DestOutcome)> {
    let DestAttemptCtx {
        instance,
        item,
        label,
        dest,
        store,
        bw,
        retry,
        verify_policy,
        events,
        now,
        attempts_so_far,
        rng,
    } = ctx;

    let resume = store.load_resume(&instance, &item.relpath, &label)?;
    let attempt = attempts_so_far.saturating_add(1);

    tracing::info!(
        instance = %instance, relpath = %item.relpath, dest = %label,
        size = item.size, attempt, resuming = resume.is_some(),
        "delivering to destination"
    );

    events
        .instance_event(
            &instance,
            Event::ReplicationStarted {
                path: item.relpath.clone(),
                size: item.size,
                destination: label.clone(),
                attempt,
            },
            now,
        )
        .await;

    let (progress, drainer) = build_progress(
        store.clone(),
        events.clone(),
        instance.clone(),
        item.relpath.clone(),
        label.clone(),
        item.size,
        attempt,
    );

    let outcome = async {
        let delivered = dest.deliver(&item, resume, &progress, &bw).await?;
        dest.verify(&item, &delivered, verify_policy).await?;
        Ok::<Delivered, ReplError>(delivered)
    }
    .await;

    // Drop our sink handle so the progress channel closes once every destination-held clone is gone
    // (deliver has returned), then flush any still-queued `ReplicationProgress` events.
    drop(progress);
    if let Some(d) = drainer {
        let _ = d.await;
    }

    match outcome {
        Ok(delivered) => {
            // Write-ahead the FULL expected result at this destination's Verified point (DESIGN
            // §13.2/§20-B): persist the delivered checksum + handle as ITS resume checkpoint BEFORE
            // marking it `DestPhase::Verified`, so crash recovery re-verifies by re-hash (not just
            // size). This overwrites any mid-transfer resume token (e.g. an S3 uploadId) — this
            // destination's transfer is complete.
            save_verified_checkpoint(&store, &instance, &item.relpath, &label, &delivered);
            store.set_dest_phase(&instance, &item.relpath, &label, DestPhase::Verified, now)?;
            tracing::info!(
                instance = %instance, relpath = %item.relpath, dest = %label,
                bytes = delivered.bytes, attempt, "destination delivered + verified"
            );
            events
                .instance_event(
                    &instance,
                    Event::ReplicationCompleted {
                        path: item.relpath.clone(),
                        size: item.size,
                        destination: label.clone(),
                        bytes: delivered.bytes,
                    },
                    now,
                )
                .await;
            Ok((label, DestOutcome::Verified))
        }
        Err(e) => {
            let permanent = e.is_permanent();
            let err_str = e.to_string();
            // `RetryPolicy::decide` reads `attempts`/`discovered_at` off a `WorkItem` — probe with
            // THIS destination's own attempts-so-far so the decision (and its backoff exponent) is
            // independent per destination, while the time-based `giveUpAfter` budget still starts at
            // the file's original discovery (shared across every destination).
            let mut probe = item.clone();
            probe.attempts = attempts_so_far;
            let decision = {
                let mut r = rng.lock().expect("rng mutex");
                retry.decide(&probe, permanent, now, &mut *r)
            };
            match decision {
                RetryDecision::Retry { next_attempt_at } => {
                    store.record_dest_attempt(
                        &instance,
                        &item.relpath,
                        &label,
                        &err_str,
                        DestPhase::Pending,
                        next_attempt_at,
                        now,
                    )?;
                    tracing::warn!(
                        instance = %instance, relpath = %item.relpath, dest = %label,
                        attempts = attempt, retry_at = next_attempt_at, error = %err_str,
                        "destination transfer failed; scheduled for retry"
                    );
                    events
                        .instance_event(
                            &instance,
                            Event::ReplicationFailed {
                                path: item.relpath.clone(),
                                destination: label.clone(),
                                attempt,
                                error: err_str.clone(),
                                next_attempt_at_ms: Some(next_attempt_at),
                            },
                            now,
                        )
                        .await;
                    Ok((label, DestOutcome::Retry { next_attempt_at, err: err_str }))
                }
                RetryDecision::GiveUp => {
                    store.record_dest_attempt(
                        &instance,
                        &item.relpath,
                        &label,
                        &err_str,
                        DestPhase::Exhausted,
                        now,
                        now,
                    )?;
                    // This destination's terminal give-up: release any in-flight partial upload and
                    // drop its resume checkpoint (idempotent + best-effort).
                    match store.load_resume(&instance, &item.relpath, &label) {
                        Ok(Some(r)) => {
                            if let Err(e) = dest.abort(&item, &r).await {
                                tracing::warn!(
                                    relpath = %item.relpath, dest = %label, error = %e,
                                    "aborting partial transfer on give-up failed"
                                );
                            }
                        }
                        Ok(None) => {}
                        Err(e) => tracing::warn!(
                            relpath = %item.relpath, dest = %label, error = %e,
                            "load resume for give-up abort failed"
                        ),
                    }
                    let _ = store.clear_resume(&instance, &item.relpath, &label);
                    tracing::error!(
                        instance = %instance, relpath = %item.relpath, dest = %label, attempts = attempt,
                        error = %err_str, "retries exhausted on this destination"
                    );
                    events
                        .instance_event(
                            &instance,
                            Event::RetriesExhausted {
                                path: item.relpath.clone(),
                                destination: label.clone(),
                                attempts: attempt,
                                last_error: err_str.clone(),
                            },
                            now,
                        )
                        .await;
                    Ok((label, DestOutcome::GiveUp { err: err_str, attempts: attempt }))
                }
            }
        }
    }
}

/// Persist the delivered result as `label`'s `Verified` write-ahead checkpoint (DESIGN §13.2/§20-B).
/// The whole [`Delivered`] rides in [`ResumeState::token`] under `"verified"`, reusing the resume row
/// (single blob per `(instance, relpath, dest)`) across lifecycle phases — no schema change.
/// Best-effort: a store fault here only costs recovery its checksum-precision (it falls back to a size
/// check), so it must not fail the transfer that already succeeded.
fn save_verified_checkpoint(
    store: &Arc<dyn StateStore>,
    instance: &str,
    relpath: &str,
    label: &str,
    delivered: &Delivered,
) {
    let token = match serde_json::to_value(delivered) {
        Ok(v) => serde_json::json!({ "verified": v }),
        Err(e) => {
            tracing::warn!(relpath = %relpath, dest = %label, error = %e, "serialize verified checkpoint failed");
            return;
        }
    };
    let checkpoint = ResumeState {
        bytes_committed: delivered.bytes,
        token,
    };
    if let Err(e) = store.save_resume(instance, relpath, label, &checkpoint) {
        tracing::warn!(relpath = %relpath, dest = %label, error = %e, "persist verified checkpoint failed");
    }
}

/// Build the [`ProgressSink`] a destination streams byte-progress into, plus (only when the UNS event
/// stream is live) a drainer task that turns those checkpoints into throttled `ReplicationProgress`
/// events tagged with `label` (DESIGN §17.2/§20-B / FR-EVT-2).
///
/// The sink persists `bytes_done` to the durable store at most once per [`PROGRESS_STEP_BYTES`] (for
/// `get-status`/resume) and, when events are enabled, forwards a checkpoint over an unbounded channel —
/// a non-blocking send from the sync callback, so the transfer never awaits the broker. The drainer
/// applies the percent-step/interval [`ProgressThrottle`] and publishes, ending when the transfer drops
/// every clone of the sink (closing the channel). When events are disabled the sink is exactly the
/// P1/P2 store-persist sink and no task is spawned (zero overhead, identical behavior). With one
/// destination this reproduces the pre-P6 single-destination progress pipeline exactly.
///
/// To honor the FR-EVT-2 "0%/100% always" guarantee, the events-enabled sink forwards the **first**
/// observation and the **terminal** (`done == size`) report unconditionally — not just the ≥4 MiB
/// persist checkpoints. Otherwise the sub-4 MiB head/tail is swallowed by the persist gate and the
/// throttle never sees percent 0 or 100 (small files would emit no progress at all).
#[allow(clippy::too_many_arguments)]
fn build_progress(
    store: Arc<dyn StateStore>,
    events: Events,
    instance: String,
    relpath: String,
    label: String,
    size: u64,
    attempt: u32,
) -> (ProgressSink, Option<tokio::task::JoinHandle<()>>) {
    let last = Arc::new(AtomicU64::new(0));

    if !events.is_enabled() {
        let store = store.clone();
        let instance2 = instance.clone();
        let relpath2 = relpath.clone();
        let sink = ProgressSink::new(move |done| {
            persist_progress(&store, &instance2, &relpath2, &last, done);
        });
        return (sink, None);
    }

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<u64>();
    let drainer_events = events.clone();
    let id = instance.clone();
    let path = relpath.clone();
    let destination = label.clone();
    let drainer = tokio::spawn(async move {
        let mut throttle = ProgressThrottle::with_defaults();
        while let Some(done) = rx.recv().await {
            let pct = percent_int(done, size);
            let now = now_ms();
            if throttle.should_emit(pct, now) {
                drainer_events
                    .instance_event(
                        &id,
                        Event::ReplicationProgress {
                            path: path.clone(),
                            size,
                            bytes_done: done,
                            percent: pct as f64,
                            destination: destination.clone(),
                            attempt,
                        },
                        now,
                    )
                    .await;
            }
        }
    });

    let first = Arc::new(AtomicBool::new(true));
    let sink = ProgressSink::new(move |done| {
        let checkpoint = persist_progress(&store, &instance, &relpath, &last, done);
        // Always forward the first observation (→ the throttle's guaranteed 0%) and the terminal
        // `done == size` report (→ the guaranteed 100%), plus every ≥4 MiB persist checkpoint in
        // between. The drainer's [`ProgressThrottle`] then decides what actually publishes.
        let is_first = first.swap(false, Ordering::Relaxed);
        let is_terminal = size > 0 && done >= size;
        if checkpoint || is_first || is_terminal {
            let _ = tx.send(done);
        }
    });
    (sink, Some(drainer))
}

/// Persist `done` bytes to the store when it advanced ≥ [`PROGRESS_STEP_BYTES`] since the last
/// checkpoint, returning whether a checkpoint was written this call (the cadence the event drainer
/// also rides). Best-effort: a store fault is logged at `debug!`, never fatal.
fn persist_progress(
    store: &Arc<dyn StateStore>,
    instance: &str,
    relpath: &str,
    last: &AtomicU64,
    done: u64,
) -> bool {
    let prev = last.load(Ordering::Relaxed);
    if done.saturating_sub(prev) >= PROGRESS_STEP_BYTES {
        last.store(done, Ordering::Relaxed);
        if let Err(e) = store.set_bytes_done(instance, relpath, done, now_ms()) {
            tracing::debug!(relpath = %relpath, error = %e, "persist progress failed");
        }
        return true;
    }
    false
}

/// Integer percent complete (`0..=100`) for the progress throttle. A zero-length file is `0%` until
/// any byte lands, then `100%`.
fn percent_int(done: u64, size: u64) -> i32 {
    if size == 0 {
        return if done == 0 { 0 } else { 100 };
    }
    (((done as f64 / size as f64) * 100.0).round() as i32).clamp(0, 100)
}

/// Join a forward-slash `relpath` onto `root`, dropping empty/`.`/`..` segments so the result always
/// stays under `root` (mirrors the destination key derivation).
fn join_rel(root: &Path, relpath: &str) -> PathBuf {
    let mut p = root.to_path_buf();
    for seg in relpath.split('/') {
        if seg.is_empty() || seg == "." || seg == ".." {
            continue;
        }
        p.push(seg);
    }
    p
}

/// Move `src` → `dst`, creating parents and honoring the collision policy. Prefers an atomic rename;
/// falls back to a **crash-atomic** copy across filesystems.
///
/// The cross-device fallback copies into a sibling temp file in the destination directory, then
/// atomically renames it onto the resolved target and removes the source. Copying via a temp keeps
/// the move crash-atomic: a crash *during* the (long) copy leaves only an orphan temp, never a
/// partially-written `target` that a recovery pass would mistake for a real file and duplicate under
/// the default `suffix` collision policy (`name` + `name.1.ext`). (DESIGN §13.2.)
fn move_file(src: &Path, dst: &Path, collision: Collision) -> Result<()> {
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent).map_err(ReplError::classify_io)?;
    }
    let target = resolve_collision(dst, collision)?;
    // Fast path: same-filesystem rename is already atomic.
    if std::fs::rename(src, &target).is_ok() {
        return Ok(());
    }
    // Cross-device (or Windows target-exists) fallback: copy → temp, atomic rename, remove source.
    let tmp = move_temp_path(&target);
    if let Err(e) = std::fs::copy(src, &tmp) {
        let _ = std::fs::remove_file(&tmp);
        return Err(ReplError::classify_io(e));
    }
    if std::fs::rename(&tmp, &target).is_err() {
        // Windows rename won't overwrite; `resolve_collision` should have freed the target, but be
        // defensive: remove an existing target then retry, cleaning the temp on any failure.
        if target.exists() {
            if let Err(e) = std::fs::remove_file(&target) {
                let _ = std::fs::remove_file(&tmp);
                return Err(ReplError::classify_io(e));
            }
        }
        if let Err(e) = std::fs::rename(&tmp, &target) {
            let _ = std::fs::remove_file(&tmp);
            return Err(ReplError::classify_io(e));
        }
    }
    std::fs::remove_file(src).map_err(ReplError::classify_io)?;
    Ok(())
}

/// A collision-resistant temp path in the destination directory for the crash-atomic move fallback:
/// `.<name>.<nanos>.movetmp` (same dir as `target`, so the final rename is same-volume/atomic).
fn move_temp_path(target: &Path) -> PathBuf {
    let dir = target.parent().unwrap_or_else(|| Path::new("."));
    let name = target
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "file".to_string());
    let tag = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    dir.join(format!(".{name}.{tag:032x}.movetmp"))
}

/// The success completion side effect (`delete` | `archive`) on the source file, run on the blocking
/// pool by [`Worker::complete_verified`]. Idempotent: a missing source (already completed before a
/// crash) is fine. Errors are logged, not fatal — delivery already succeeded and the durable
/// `Completed` marker stops re-discovery, so a stale source can never loop.
fn apply_success_action(
    on_success: OnSuccess,
    src: &Path,
    archive_dir: Option<&Path>,
    relpath: &str,
    collision: Collision,
    instance: &str,
) {
    match on_success {
        OnSuccess::Delete => {
            if let Err(e) = std::fs::remove_file(src) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(path = %src.display(), error = %e, "delete source failed");
                }
            }
        }
        OnSuccess::Archive => match archive_dir {
            Some(dir) if src.exists() => {
                let dst = join_rel(dir, relpath);
                if let Err(e) = move_file(src, &dst, collision) {
                    tracing::warn!(
                        src = %src.display(), dst = %dst.display(), error = %e,
                        "archive move failed; leaving source in place"
                    );
                }
            }
            Some(_) => {} // source already gone (idempotent recovery)
            None => tracing::warn!(
                instance = %instance, relpath = %relpath,
                "onSuccess=archive but no archiveDir configured; leaving source in place"
            ),
        },
    }
}

/// Diagnostics carried into the blocking quarantine action (DESIGN §13.3 `.error.json` sidecar).
struct QuarantineCtx {
    instance: String,
    relpath: String,
    last_error: String,
    attempts: u32,
    first_seen: i64,
    last_try: i64,
    destination: String,
    bytes_done: u64,
}

/// The quarantine completion side effect, run on the blocking pool by
/// [`Worker::finish_exhausted`]: move the source into `failed_dir` (subtree preserved) and write the
/// `<dst>.error.json` sidecar. Idempotent for recovery (a missing source is fine).
fn apply_quarantine_action(
    failed_dir: Option<&Path>,
    src: &Path,
    collision: Collision,
    ctx: &QuarantineCtx,
) {
    match failed_dir {
        Some(dir) => {
            let dst = join_rel(dir, &ctx.relpath);
            if src.exists() {
                if let Err(e) = move_file(src, &dst, collision) {
                    tracing::warn!(
                        src = %src.display(), dst = %dst.display(), error = %e,
                        "quarantine move failed"
                    );
                }
            }
            write_error_sidecar(&dst, ctx);
        }
        None => tracing::warn!(
            instance = %ctx.instance, relpath = %ctx.relpath,
            "onExhausted=quarantine but no failedDir configured; leaving source in place"
        ),
    }
}

/// Write the `<dst>.error.json` diagnostic sidecar next to a quarantined file (DESIGN §13.3).
fn write_error_sidecar(dst: &Path, ctx: &QuarantineCtx) {
    let mut name = dst.as_os_str().to_owned();
    name.push(".error.json");
    let sidecar = PathBuf::from(name);
    if let Some(p) = sidecar.parent() {
        let _ = std::fs::create_dir_all(p);
    }
    let payload = serde_json::json!({
        "instance": ctx.instance,
        "relpath": ctx.relpath,
        "lastError": ctx.last_error,
        "attempts": ctx.attempts,
        "firstSeen": ctx.first_seen,
        "lastTry": ctx.last_try,
        "destination": ctx.destination,
        "bytesDone": ctx.bytes_done,
    });
    if let Err(e) = std::fs::write(&sidecar, serde_json::to_vec_pretty(&payload).unwrap_or_default())
    {
        tracing::warn!(path = %sidecar.display(), error = %e, "write error sidecar failed");
    }
}

/// Resolve the effective target path for a collision policy: `overwrite` removes the existing file,
/// `suffix` finds a free `name.N.ext`, `fail` errors permanently.
fn resolve_collision(dst: &Path, collision: Collision) -> Result<PathBuf> {
    if !dst.exists() {
        return Ok(dst.to_path_buf());
    }
    match collision {
        Collision::Overwrite => {
            std::fs::remove_file(dst).map_err(ReplError::classify_io)?;
            Ok(dst.to_path_buf())
        }
        Collision::Fail => Err(ReplError::Permanent(format!(
            "destination already exists: {}",
            dst.display()
        ))),
        Collision::Suffix => {
            for i in 1..10_000 {
                let cand = suffixed(dst, i);
                if !cand.exists() {
                    return Ok(cand);
                }
            }
            Err(ReplError::Permanent(format!(
                "no free suffixed name for {}",
                dst.display()
            )))
        }
    }
}

/// Insert `.N` before the extension: `report.csv` + 1 → `report.1.csv`; extension-less `data` → `data.1`.
fn suffixed(path: &Path, n: u32) -> PathBuf {
    let stem = path.file_stem().map(|s| s.to_string_lossy().into_owned());
    let ext = path.extension().map(|s| s.to_string_lossy().into_owned());
    let name = match (stem, ext) {
        (Some(s), Some(e)) => format!("{s}.{n}.{e}"),
        (Some(s), None) => format!("{s}.{n}"),
        _ => format!("file.{n}"),
    };
    path.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{LocalEgress, Verify};
    use crate::dest::{Destination, LocalDest};
    use crate::domain::ItemState;
    use crate::error::Result as ReplResult;
    use crate::integrity::Algorithm;
    use crate::state::SqliteStore;
    use async_trait::async_trait;
    use std::path::Path;
    use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

    const INST: &str = "w";

    // ---- duration + backoff (pure) --------------------------------------------------------------

    #[test]
    fn parse_duration_units() {
        assert_eq!(parse_duration_ms("7d"), Some(604_800_000));
        assert_eq!(parse_duration_ms("15m"), Some(900_000));
        assert_eq!(parse_duration_ms("30s"), Some(30_000));
        assert_eq!(parse_duration_ms("2h"), Some(7_200_000));
        assert_eq!(parse_duration_ms("500ms"), Some(500));
        assert_eq!(parse_duration_ms("1.5h"), Some(5_400_000));
        assert_eq!(parse_duration_ms("250"), Some(250)); // bare = ms
        assert_eq!(parse_duration_ms(""), None);
        assert_eq!(parse_duration_ms("abc"), None);
        assert_eq!(parse_duration_ms("-5s"), None);
    }

    #[test]
    fn resolve_prefers_instance_then_global_then_default() {
        let def = RetryPolicy::resolve(None, None);
        assert_eq!(def.base_delay_ms, DEFAULT_BASE_DELAY_MS);
        assert_eq!(def.give_up_after_ms, Some(DEFAULT_GIVE_UP_AFTER_MS));
        assert!(def.max_attempts.is_none());

        let global = RetryCfg {
            base_delay_ms: Some(2_000),
            max_delay_ms: Some(60_000),
            give_up_after: Some("1d".into()),
            max_attempts: Some(9),
        };
        let instance = RetryCfg {
            base_delay_ms: Some(500),
            max_delay_ms: None, // falls back to global
            give_up_after: None,
            max_attempts: None,
        };
        let p = RetryPolicy::resolve(Some(&instance), Some(&global));
        assert_eq!(p.base_delay_ms, 500, "instance wins");
        assert_eq!(p.max_delay_ms, 60_000, "falls back to global");
        assert_eq!(p.give_up_after_ms, Some(86_400_000), "global 1d");
        assert_eq!(p.max_attempts, Some(9), "global");
    }

    #[test]
    fn backoff_is_capped_and_within_full_jitter_bounds() {
        let p = RetryPolicy {
            base_delay_ms: 1_000,
            max_delay_ms: 8_000,
            give_up_after_ms: Some(i64::MAX),
            max_attempts: None,
        };
        let mut rng = StdRng::seed_from_u64(42);
        for attempts in 1..=10u32 {
            let cap = (1_000u64 << (attempts - 1).min(32)).clamp(1, 8_000);
            for _ in 0..50 {
                let d = p.backoff_ms(attempts, &mut rng);
                assert!(d <= cap, "attempt {attempts}: {d} > cap {cap}");
            }
        }
    }

    #[test]
    fn decide_retry_then_give_up_on_attempts_and_time() {
        let p = RetryPolicy {
            base_delay_ms: 1_000,
            max_delay_ms: 10_000,
            give_up_after_ms: Some(100_000),
            max_attempts: Some(3),
        };
        let mut rng = StdRng::seed_from_u64(7);
        let mut item = WorkItem {
            instance: INST.into(),
            relpath: "f".into(),
            abs_source: PathBuf::new(),
            state: ItemState::InProgress,
            size: 1,
            discovered_at: 0,
            attempts: 0,
            next_attempt_at: 0,
            last_error: None,
            bytes_done: 0,
            updated_at: 0,
        };
        // Transient, attempts under cap and within time budget → Retry.
        match p.decide(&item, false, 1_000, &mut rng) {
            RetryDecision::Retry { next_attempt_at } => assert!(next_attempt_at >= 1_000),
            d => panic!("expected retry, got {d:?}"),
        }
        // Permanent → always GiveUp.
        assert_eq!(p.decide(&item, true, 1_000, &mut rng), RetryDecision::GiveUp);
        // Attempts cap: after 2 prior attempts, this (3rd) reaches maxAttempts=3 → GiveUp.
        item.attempts = 2;
        assert_eq!(p.decide(&item, false, 1_000, &mut rng), RetryDecision::GiveUp);
        // Time budget: within attempts but past giveUpAfter → GiveUp.
        item.attempts = 0;
        assert_eq!(
            p.decide(&item, false, 200_000, &mut rng),
            RetryDecision::GiveUp
        );
    }

    #[test]
    fn suffixed_inserts_before_extension() {
        assert_eq!(suffixed(Path::new("/a/report.csv"), 1), PathBuf::from("/a/report.1.csv"));
        assert_eq!(suffixed(Path::new("/a/data"), 2), PathBuf::from("/a/data.2"));
    }

    // ---- move_file / collision ------------------------------------------------------------------

    #[test]
    fn move_file_renames_and_creates_parents() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.txt");
        std::fs::write(&src, b"hi").unwrap();
        let dst = dir.path().join("sub/dir/out.txt");
        move_file(&src, &dst, Collision::Fail).unwrap();
        assert!(!src.exists());
        assert_eq!(std::fs::read(&dst).unwrap(), b"hi");
    }

    #[test]
    fn move_file_collision_policies() {
        let dir = tempfile::tempdir().unwrap();
        let mk = |name: &str, body: &[u8]| {
            let p = dir.path().join(name);
            std::fs::write(&p, body).unwrap();
            p
        };
        let dst = mk("dst.txt", b"existing");

        // Fail → error, source untouched.
        let s1 = mk("s1.txt", b"a");
        assert!(move_file(&s1, &dst, Collision::Fail).is_err());
        assert!(s1.exists());

        // Suffix → writes dst.1.txt, original preserved.
        let s2 = mk("s2.txt", b"b");
        move_file(&s2, &dst, Collision::Suffix).unwrap();
        assert_eq!(std::fs::read(dir.path().join("dst.1.txt")).unwrap(), b"b");
        assert_eq!(std::fs::read(&dst).unwrap(), b"existing");

        // Overwrite → replaces dst.
        let s3 = mk("s3.txt", b"c");
        move_file(&s3, &dst, Collision::Overwrite).unwrap();
        assert_eq!(std::fs::read(&dst).unwrap(), b"c");
    }

    // ---- worker pipeline (integration with the real local dest + sqlite store) ------------------

    /// A destination that always fails with a caller-chosen error (transfer error-path tests).
    struct FailingDest {
        err: fn() -> ReplError,
    }
    #[async_trait]
    impl crate::dest::Destination for FailingDest {
        fn kind(&self) -> &'static str {
            "failing"
        }
        fn supports_resume(&self) -> bool {
            false
        }
        async fn deliver(
            &self,
            _item: &WorkItem,
            _resume: Option<crate::domain::ResumeState>,
            _progress: &ProgressSink,
            _bw: &Bandwidth,
        ) -> ReplResult<crate::domain::Delivered> {
            Err((self.err)())
        }
        async fn verify(
            &self,
            _item: &WorkItem,
            _delivered: &crate::domain::Delivered,
            _policy: Verify,
        ) -> ReplResult<()> {
            Ok(())
        }
        async fn abort(
            &self,
            _item: &WorkItem,
            _resume: &crate::domain::ResumeState,
        ) -> ReplResult<()> {
            Ok(())
        }
    }

    fn store() -> Arc<dyn StateStore> {
        Arc::new(SqliteStore::open_in_memory().unwrap())
    }

    fn completion(on_success: OnSuccess) -> CompletionCfg {
        CompletionCfg {
            on_success,
            archive_dir: None,
            on_exhausted: OnExhausted::RetainInPlace,
            failed_dir: None,
            on_collision: Collision::Suffix,
            verify: Verify::Checksum,
        }
    }

    fn local_worker(
        store: Arc<dyn StateStore>,
        src_root: &Path,
        dst_root: &Path,
        completion: CompletionCfg,
        retry: RetryPolicy,
    ) -> Worker {
        let dest: SharedDestination = Arc::new(LocalDest::new(
            &LocalEgress {
                path: dst_root.to_path_buf(),
                fsync: false,
            },
            Algorithm::Crc32c,
        ));
        Worker::new(
            INST.to_string(),
            store,
            dest,
            completion,
            src_root.to_path_buf(),
            Bandwidth::unlimited(),
            retry,
            None,
        )
    }

    fn enqueue_file(store: &Arc<dyn StateStore>, src_root: &Path, rel: &str, body: &[u8]) {
        let abs = src_root.join(rel.replace('/', std::path::MAIN_SEPARATOR_STR));
        if let Some(p) = abs.parent() {
            std::fs::create_dir_all(p).unwrap();
        }
        std::fs::write(&abs, body).unwrap();
        store.upsert_ready(INST, rel, body.len() as u64, 0, 1).unwrap();
    }

    #[tokio::test]
    async fn process_item_delete_success_end_to_end() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let store = store();
        enqueue_file(&store, src.path(), "a/b.txt", b"payload");
        let worker = local_worker(store.clone(), src.path(), dst.path(), completion(OnSuccess::Delete), RetryPolicy::default());

        let item = store.claim_ready(INST, 10, 100).unwrap().pop().unwrap();
        let state = worker.process_item(&item, 100).await;

        assert_eq!(state, ItemState::Completed);
        assert_eq!(std::fs::read(dst.path().join("a/b.txt")).unwrap(), b"payload");
        assert!(!src.path().join("a/b.txt").exists(), "source deleted on success");
        assert_eq!(store.get(INST, "a/b.txt").unwrap().unwrap().state, ItemState::Completed);
        assert_eq!(store.stats(INST).unwrap().replicated, 1);
    }

    #[tokio::test]
    async fn process_item_archive_moves_source() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let archive = tempfile::tempdir().unwrap();
        let store = store();
        enqueue_file(&store, src.path(), "r.csv", b"1,2,3");
        let mut comp = completion(OnSuccess::Archive);
        comp.archive_dir = Some(archive.path().to_path_buf());
        let worker = local_worker(store.clone(), src.path(), dst.path(), comp, RetryPolicy::default());

        let item = store.claim_ready(INST, 10, 100).unwrap().pop().unwrap();
        assert_eq!(worker.process_item(&item, 100).await, ItemState::Completed);
        assert_eq!(std::fs::read(dst.path().join("r.csv")).unwrap(), b"1,2,3");
        assert_eq!(std::fs::read(archive.path().join("r.csv")).unwrap(), b"1,2,3");
        assert!(!src.path().join("r.csv").exists(), "source archived (moved)");
    }

    #[tokio::test]
    async fn transient_failure_schedules_retry() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let store = store();
        store.upsert_ready(INST, "f", 10, 0, 1).unwrap();
        let dest: SharedDestination = Arc::new(FailingDest {
            err: || ReplError::Transient("network".into()),
        });
        let worker = Worker::new(
            INST.into(),
            store.clone(),
            dest,
            completion(OnSuccess::Delete),
            src.path().to_path_buf(),
            Bandwidth::unlimited(),
            RetryPolicy::default(),
            None,
        );
        let _ = dst; // dst dir unused for the failing dest
        let item = store.claim_ready(INST, 10, 100).unwrap().pop().unwrap();
        assert_eq!(worker.process_item(&item, 100).await, ItemState::Failed);
        let got = store.get(INST, "f").unwrap().unwrap();
        assert_eq!(got.state, ItemState::Failed);
        assert_eq!(got.attempts, 1);
        assert!(got.next_attempt_at >= 100, "backoff gate set in the future");
        assert!(got.last_error.as_deref().unwrap().contains("network"));
    }

    #[tokio::test]
    async fn permanent_failure_exhausts_and_retains() {
        let src = tempfile::tempdir().unwrap();
        let store = store();
        std::fs::write(src.path().join("f"), b"x").unwrap();
        store.upsert_ready(INST, "f", 1, 0, 1).unwrap();
        let dest: SharedDestination = Arc::new(FailingDest {
            err: || ReplError::Permanent("no such bucket".into()),
        });
        let worker = Worker::new(
            INST.into(),
            store.clone(),
            dest,
            completion(OnSuccess::Delete), // retainInPlace on_exhausted
            src.path().to_path_buf(),
            Bandwidth::unlimited(),
            RetryPolicy::default(),
            None,
        );
        let item = store.claim_ready(INST, 10, 100).unwrap().pop().unwrap();
        assert_eq!(worker.process_item(&item, 100).await, ItemState::Retained);
        assert_eq!(store.get(INST, "f").unwrap().unwrap().state, ItemState::Retained);
        assert!(src.path().join("f").exists(), "retainInPlace leaves the source");
        assert_eq!(store.stats(INST).unwrap().failed, 1);
    }

    #[tokio::test]
    async fn permanent_failure_quarantines_with_sidecar() {
        let src = tempfile::tempdir().unwrap();
        let failed = tempfile::tempdir().unwrap();
        let store = store();
        std::fs::write(src.path().join("bad.dat"), b"oops").unwrap();
        store.upsert_ready(INST, "bad.dat", 4, 0, 1).unwrap();
        let mut comp = completion(OnSuccess::Delete);
        comp.on_exhausted = OnExhausted::Quarantine;
        comp.failed_dir = Some(failed.path().to_path_buf());
        let dest: SharedDestination = Arc::new(FailingDest {
            err: || ReplError::Permanent("denied".into()),
        });
        let worker = Worker::new(
            INST.into(),
            store.clone(),
            dest,
            comp,
            src.path().to_path_buf(),
            Bandwidth::unlimited(),
            RetryPolicy::default(),
            None,
        );
        let item = store.claim_ready(INST, 10, 100).unwrap().pop().unwrap();
        assert_eq!(worker.process_item(&item, 100).await, ItemState::Quarantined);
        assert!(!src.path().join("bad.dat").exists(), "quarantined (moved)");
        assert_eq!(std::fs::read(failed.path().join("bad.dat")).unwrap(), b"oops");
        let sidecar = failed.path().join("bad.dat.error.json");
        let json: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&sidecar).unwrap()).unwrap();
        assert_eq!(json["relpath"], "bad.dat");
        assert_eq!(json["attempts"], 1);
        assert!(json["lastError"].as_str().unwrap().contains("denied"));
    }

    #[tokio::test]
    async fn give_up_after_repeated_transient_failures() {
        // maxAttempts=2 so the second transient failure exhausts.
        let src = tempfile::tempdir().unwrap();
        let store = store();
        std::fs::write(src.path().join("f"), b"x").unwrap();
        store.upsert_ready(INST, "f", 1, 0, 1).unwrap();
        let dest: SharedDestination = Arc::new(FailingDest {
            err: || ReplError::Transient("flaky".into()),
        });
        let retry = RetryPolicy {
            base_delay_ms: 1,
            max_delay_ms: 1,
            give_up_after_ms: Some(i64::MAX),
            max_attempts: Some(2),
        };
        let worker = Worker::new(
            INST.into(),
            store.clone(),
            dest,
            completion(OnSuccess::Delete),
            src.path().to_path_buf(),
            Bandwidth::unlimited(),
            retry,
            None,
        );
        // Attempt 1 → Failed (retry), with a backoff gate recorded.
        let item = store.claim_ready(INST, 10, 0).unwrap().pop().unwrap();
        assert_eq!(worker.process_item(&item, 0).await, ItemState::Failed);
        assert_eq!(store.get(INST, "f").unwrap().unwrap().attempts, 1);
        // The retry manager promotes Failed → Ready once the gate elapses (see Queue::promote_due).
        store.set_state(INST, "f", ItemState::Ready, 1_000_000).unwrap();
        // Re-claim; attempt 2 hits maxAttempts → Exhausted → Retained (default onExhausted).
        let item = store.claim_ready(INST, 10, 1_000_000).unwrap().pop().unwrap();
        assert_eq!(worker.process_item(&item, 1_000_000).await, ItemState::Retained);
    }

    #[tokio::test]
    async fn recover_completes_verified_and_reready_in_progress() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let store = store();

        // A Verified item whose object was already delivered to the destination: recovery must finish
        // completion (delete the source) without re-delivering.
        std::fs::write(src.path().join("v.txt"), b"verified-src").unwrap();
        std::fs::write(dst.path().join("v.txt"), b"verified-src").unwrap();
        store.upsert_ready(INST, "v.txt", 12, 0, 1).unwrap();
        store.set_state(INST, "v.txt", ItemState::Verified, 2).unwrap();

        // An InProgress item interrupted mid-flight: recovery must return it to Ready.
        std::fs::write(src.path().join("p.txt"), b"partial").unwrap();
        store.upsert_ready(INST, "p.txt", 7, 0, 1).unwrap();
        store.set_state(INST, "p.txt", ItemState::InProgress, 2).unwrap();

        let worker = local_worker(store.clone(), src.path(), dst.path(), completion(OnSuccess::Delete), RetryPolicy::default());
        worker.recover(100).await.unwrap();

        assert_eq!(store.get(INST, "v.txt").unwrap().unwrap().state, ItemState::Completed);
        assert!(!src.path().join("v.txt").exists(), "verified recovery deletes the source");
        assert_eq!(store.get(INST, "p.txt").unwrap().unwrap().state, ItemState::Ready);

        // The re-readied InProgress item must actually be re-delivered on the next claim, exactly
        // once (idempotent re-delivery, no double-count): drive the claim→process the engine would.
        let item = store.claim_ready(INST, 10, 200).unwrap().pop().unwrap();
        assert_eq!(item.relpath, "p.txt");
        assert_eq!(worker.process_item(&item, 200).await, ItemState::Completed);
        assert_eq!(std::fs::read(dst.path().join("p.txt")).unwrap(), b"partial", "re-delivered correctly");
        assert!(!src.path().join("p.txt").exists(), "source deleted after re-delivery");
        // Exactly-once across the crash: v.txt completed via recovery (+1) and p.txt via re-delivery
        // (+1) — two files, each counted once, no double-delivery.
        assert_eq!(store.stats(INST).unwrap().replicated, 2);
    }

    #[tokio::test]
    async fn recover_verified_requeues_when_destination_missing() {
        // §13.2 guard: if the destination object was lost/corrupted during the downtime, recovery of
        // a Verified item MUST NOT delete the source — it re-verifies, fails, and requeues to Ready.
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let store = store();
        std::fs::write(src.path().join("v.txt"), b"the only copy").unwrap();
        // NOTE: destination object deliberately absent (simulating a wiped mount).
        store.upsert_ready(INST, "v.txt", 13, 0, 1).unwrap();
        store.set_state(INST, "v.txt", ItemState::Verified, 2).unwrap();

        let worker = local_worker(store.clone(), src.path(), dst.path(), completion(OnSuccess::Delete), RetryPolicy::default());
        worker.recover(100).await.unwrap();

        assert_eq!(
            store.get(INST, "v.txt").unwrap().unwrap().state,
            ItemState::Ready,
            "missing destination → requeue, never complete"
        );
        assert!(src.path().join("v.txt").exists(), "source preserved — no data loss");
        assert_eq!(store.stats(INST).unwrap().replicated, 0, "not counted as replicated");
    }

    #[tokio::test]
    async fn recover_verified_with_checkpoint_completes_via_checksum() {
        // §13.2 refinement: a Verified item with the full Delivered checkpoint persisted re-verifies
        // by CHECKSUM (not just size) and, on a match, completes.
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let store = store();
        std::fs::write(src.path().join("v.bin"), b"checkpoint payload").unwrap();

        // Deliver once via a local dest to capture the real Delivered (checksum + handle).
        let dest = LocalDest::new(
            &LocalEgress {
                path: dst.path().to_path_buf(),
                fsync: false,
            },
            Algorithm::Crc32c,
        );
        let mut item = WorkItem {
            instance: INST.into(),
            relpath: "v.bin".into(),
            abs_source: src.path().join("v.bin"),
            state: ItemState::InProgress,
            size: 18,
            discovered_at: 0,
            attempts: 0,
            next_attempt_at: 0,
            last_error: None,
            bytes_done: 0,
            updated_at: 0,
        };
        let delivered = dest
            .deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .unwrap();
        item.abs_source = src.path().join("v.bin"); // (unchanged; kept explicit)

        // Seed the store as a crashed-at-Verified item WITH the checkpoint (dest key = "local").
        store.upsert_ready(INST, "v.bin", 18, 0, 1).unwrap();
        store.set_state(INST, "v.bin", ItemState::Verified, 2).unwrap();
        store
            .save_resume(
                INST,
                "v.bin",
                "local",
                &crate::domain::ResumeState {
                    bytes_committed: delivered.bytes,
                    token: serde_json::json!({ "verified": serde_json::to_value(&delivered).unwrap() }),
                },
            )
            .unwrap();

        let worker = local_worker(store.clone(), src.path(), dst.path(), completion(OnSuccess::Delete), RetryPolicy::default());
        worker.recover(100).await.unwrap();

        assert_eq!(store.get(INST, "v.bin").unwrap().unwrap().state, ItemState::Completed);
        assert!(!src.path().join("v.bin").exists(), "checksum-verified recovery deletes the source");
        // Resume checkpoint cleared after Completed.
        assert!(store.load_resume(INST, "v.bin", "local").unwrap().is_none());
        assert_eq!(store.stats(INST).unwrap().replicated, 1);
    }

    #[tokio::test]
    async fn recover_verified_requeues_on_corruption_checksum_catches_it() {
        // The whole point of persisting the checksum: a destination object that was CORRUPTED (not
        // just deleted) during downtime — same size, different bytes — must be caught by the re-hash
        // and requeued, where the old size-only check would have silently completed + deleted the src.
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let store = store();
        std::fs::write(src.path().join("c.bin"), b"the true bytes!!").unwrap();

        let dest = LocalDest::new(
            &LocalEgress {
                path: dst.path().to_path_buf(),
                fsync: false,
            },
            Algorithm::Crc32c,
        );
        let item = WorkItem {
            instance: INST.into(),
            relpath: "c.bin".into(),
            abs_source: src.path().join("c.bin"),
            state: ItemState::InProgress,
            size: 16,
            discovered_at: 0,
            attempts: 0,
            next_attempt_at: 0,
            last_error: None,
            bytes_done: 0,
            updated_at: 0,
        };
        let delivered = dest
            .deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .unwrap();

        // Corrupt the delivered object in place: SAME length, different content (a size check passes).
        std::fs::write(dst.path().join("c.bin"), b"CORRUPTED bytes!").unwrap();
        assert_eq!(std::fs::metadata(dst.path().join("c.bin")).unwrap().len(), 16);

        store.upsert_ready(INST, "c.bin", 16, 0, 1).unwrap();
        store.set_state(INST, "c.bin", ItemState::Verified, 2).unwrap();
        store
            .save_resume(
                INST,
                "c.bin",
                "local",
                &crate::domain::ResumeState {
                    bytes_committed: delivered.bytes,
                    token: serde_json::json!({ "verified": serde_json::to_value(&delivered).unwrap() }),
                },
            )
            .unwrap();

        let worker = local_worker(store.clone(), src.path(), dst.path(), completion(OnSuccess::Delete), RetryPolicy::default());
        worker.recover(100).await.unwrap();

        assert_eq!(
            store.get(INST, "c.bin").unwrap().unwrap().state,
            ItemState::Ready,
            "corruption caught by checksum → requeue"
        );
        assert!(src.path().join("c.bin").exists(), "source preserved — no data loss");
        assert!(store.load_resume(INST, "c.bin", "local").unwrap().is_none(), "stale checkpoint cleared");
        assert_eq!(store.stats(INST).unwrap().replicated, 0);
    }

    #[tokio::test]
    async fn give_up_aborts_partial_transfer_and_clears_resume() {
        // A resumable destination that fails delivery permanently and records abort() calls. On
        // give-up the worker must abort the in-flight partial (the S3 MPU lifecycle) and clear resume.
        struct RecordingDest {
            aborts: Arc<AtomicU32>,
        }
        #[async_trait]
        impl crate::dest::Destination for RecordingDest {
            fn kind(&self) -> &'static str {
                "recording"
            }
            fn supports_resume(&self) -> bool {
                true
            }
            async fn deliver(
                &self,
                _i: &WorkItem,
                _r: Option<crate::domain::ResumeState>,
                _p: &ProgressSink,
                _b: &Bandwidth,
            ) -> ReplResult<crate::domain::Delivered> {
                Err(ReplError::Permanent("nope".into()))
            }
            async fn verify(
                &self,
                _i: &WorkItem,
                _d: &crate::domain::Delivered,
                _p: Verify,
            ) -> ReplResult<()> {
                Ok(())
            }
            async fn abort(
                &self,
                _i: &WorkItem,
                _r: &crate::domain::ResumeState,
            ) -> ReplResult<()> {
                self.aborts.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(())
            }
        }

        let src = tempfile::tempdir().unwrap();
        let store = store();
        std::fs::write(src.path().join("m.bin"), b"partial").unwrap();
        store.upsert_ready(INST, "m.bin", 7, 0, 1).unwrap();
        // Seed a mid-transfer resume checkpoint under the dest's key.
        store
            .save_resume(
                INST,
                "m.bin",
                "recording",
                &crate::domain::ResumeState {
                    bytes_committed: 3,
                    token: serde_json::json!({ "s3": { "uploadId": "UP", "key": "m.bin", "partSize": 5242880, "completed": [] } }),
                },
            )
            .unwrap();

        let aborts = Arc::new(AtomicU32::new(0));
        let dest: SharedDestination = Arc::new(RecordingDest {
            aborts: aborts.clone(),
        });
        let worker = Worker::new(
            INST.into(),
            store.clone(),
            dest,
            completion(OnSuccess::Delete), // retainInPlace on_exhausted
            src.path().to_path_buf(),
            Bandwidth::unlimited(),
            RetryPolicy::default(),
            None,
        );
        let item = store.claim_ready(INST, 10, 100).unwrap().pop().unwrap();
        assert_eq!(worker.process_item(&item, 100).await, ItemState::Retained);
        assert_eq!(aborts.load(AtomicOrdering::SeqCst), 1, "give-up aborts the partial upload once");
        assert!(
            store.load_resume(INST, "m.bin", "recording").unwrap().is_none(),
            "resume checkpoint cleared on give-up"
        );
    }

    #[tokio::test]
    async fn transient_retry_keeps_resume_for_next_attempt() {
        // On the RETRY path (not give-up) the resume checkpoint must be PRESERVED so the next attempt
        // resumes rather than restarting.
        let src = tempfile::tempdir().unwrap();
        let store = store();
        std::fs::write(src.path().join("r.bin"), b"data").unwrap();
        store.upsert_ready(INST, "r.bin", 4, 0, 1).unwrap();
        store
            .save_resume(
                INST,
                "r.bin",
                "failing",
                &crate::domain::ResumeState {
                    bytes_committed: 2,
                    token: serde_json::json!({ "s3": { "uploadId": "UP2", "key": "r.bin", "partSize": 5242880, "completed": [] } }),
                },
            )
            .unwrap();
        let dest: SharedDestination = Arc::new(FailingDest {
            err: || ReplError::Transient("flaky".into()),
        });
        let worker = Worker::new(
            INST.into(),
            store.clone(),
            dest,
            completion(OnSuccess::Delete),
            src.path().to_path_buf(),
            Bandwidth::unlimited(),
            RetryPolicy::default(),
            None,
        );
        let item = store.claim_ready(INST, 10, 100).unwrap().pop().unwrap();
        assert_eq!(worker.process_item(&item, 100).await, ItemState::Failed);
        assert!(
            store.load_resume(INST, "r.bin", "failing").unwrap().is_some(),
            "retry preserves the resume checkpoint"
        );
    }

    #[tokio::test]
    async fn recover_finishes_exhausted_items() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let store = store();
        std::fs::write(src.path().join("e.txt"), b"stuck").unwrap();
        store.upsert_ready(INST, "e.txt", 5, 0, 1).unwrap();
        store
            .record_attempt(INST, "e.txt", "boom", ItemState::Exhausted, 2, 2)
            .unwrap();
        let worker = local_worker(store.clone(), src.path(), dst.path(), completion(OnSuccess::Delete), RetryPolicy::default());
        worker.recover(100).await.unwrap();
        // Default on_exhausted = retainInPlace.
        assert_eq!(store.get(INST, "e.txt").unwrap().unwrap().state, ItemState::Retained);
    }

    #[tokio::test]
    async fn metrics_emitted_on_completion() {
        // A tiny in-crate MetricService double that counts emit_metric calls.
        struct CountingMetrics {
            calls: AtomicU32,
        }
        #[async_trait]
        impl MetricService for CountingMetrics {
            fn define_metric(&self, _m: ggcommons::metrics::Metric) {}
            fn is_metric_defined(&self, _n: &str) -> bool {
                true
            }
            async fn emit_metric(
                &self,
                _name: &str,
                _values: HashMap<String, f64>,
            ) -> ggcommons::Result<()> {
                self.calls.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(())
            }
            async fn emit_metric_now(
                &self,
                _name: &str,
                _values: HashMap<String, f64>,
            ) -> ggcommons::Result<()> {
                Ok(())
            }
            async fn flush_metrics(&self) -> ggcommons::Result<()> {
                Ok(())
            }
            async fn shutdown(&self) {}
        }

        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let store = store();
        enqueue_file(&store, src.path(), "m.txt", b"metric");
        let metrics = Arc::new(CountingMetrics {
            calls: AtomicU32::new(0),
        });
        let dest: SharedDestination = Arc::new(LocalDest::new(
            &LocalEgress {
                path: dst.path().to_path_buf(),
                fsync: false,
            },
            Algorithm::Crc32c,
        ));
        let worker = Worker::new(
            INST.into(),
            store.clone(),
            dest,
            completion(OnSuccess::Delete),
            src.path().to_path_buf(),
            Bandwidth::unlimited(),
            RetryPolicy::default(),
            Some(metrics.clone()),
        );
        let item = store.claim_ready(INST, 10, 100).unwrap().pop().unwrap();
        assert_eq!(worker.process_item(&item, 100).await, ItemState::Completed);
        assert_eq!(metrics.calls.load(AtomicOrdering::SeqCst), 1);
    }

    // ---- P3 UNS event emission (the engine-wiring slice) ----------------------------------------

    use crate::events::test_support::recording_events;

    #[tokio::test]
    async fn events_success_delete_emits_started_completed_deleted() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let store = store();
        enqueue_file(&store, src.path(), "a/b.txt", b"payload");
        let (fake, events) = recording_events();
        let worker = local_worker(
            store.clone(),
            src.path(),
            dst.path(),
            completion(OnSuccess::Delete),
            RetryPolicy::default(),
        )
        .with_events(events);

        let item = store.claim_ready(INST, 10, 100).unwrap().pop().unwrap();
        assert_eq!(worker.process_item(&item, 100).await, ItemState::Completed);

        // ReplicationStarted on the right per-instance topic, with size/attempt/destination.
        let started = fake.events_named("ReplicationStarted");
        assert_eq!(started.len(), 1);
        assert_eq!(started[0].body["path"], serde_json::json!("a/b.txt"));
        assert_eq!(started[0].body["attempt"], serde_json::json!(1));
        assert_eq!(started[0].body["destination"], serde_json::json!("local"));
        assert!(fake
            .topics()
            .contains(&"gw-01/file-replicator/evt/instances/w/ReplicationStarted".to_string()));

        // ReplicationCompleted carries the delivered byte count, then FileDeleted (onSuccess=delete).
        let done = fake.events_named("ReplicationCompleted");
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].body["bytes"], serde_json::json!(7));
        let deleted = fake.events_named("FileDeleted");
        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted[0].body["path"], serde_json::json!("a/b.txt"));
        assert!(fake.events_named("FileArchived").is_empty(), "delete, not archive");
    }

    #[tokio::test]
    async fn events_success_archive_emits_file_archived_with_path() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let archive = tempfile::tempdir().unwrap();
        let store = store();
        enqueue_file(&store, src.path(), "r.csv", b"1,2,3");
        let mut comp = completion(OnSuccess::Archive);
        comp.archive_dir = Some(archive.path().to_path_buf());
        let (fake, events) = recording_events();
        let worker = local_worker(store.clone(), src.path(), dst.path(), comp, RetryPolicy::default())
            .with_events(events);

        let item = store.claim_ready(INST, 10, 100).unwrap().pop().unwrap();
        assert_eq!(worker.process_item(&item, 100).await, ItemState::Completed);

        let archived = fake.events_named("FileArchived");
        assert_eq!(archived.len(), 1);
        assert_eq!(archived[0].body["path"], serde_json::json!("r.csv"));
        let ap = archived[0].body["archivePath"].as_str().unwrap();
        assert!(ap.ends_with("r.csv"), "archivePath points at the moved file: {ap}");
        assert!(fake.events_named("FileDeleted").is_empty(), "archive, not delete");
    }

    #[tokio::test]
    async fn events_transient_failure_emits_replication_failed_with_next_attempt() {
        let src = tempfile::tempdir().unwrap();
        let store = store();
        store.upsert_ready(INST, "f", 10, 0, 1).unwrap();
        let dest: SharedDestination = Arc::new(FailingDest {
            err: || ReplError::Transient("network".into()),
        });
        let (fake, events) = recording_events();
        let worker = Worker::new(
            INST.into(),
            store.clone(),
            dest,
            completion(OnSuccess::Delete),
            src.path().to_path_buf(),
            Bandwidth::unlimited(),
            RetryPolicy::default(),
            None,
        )
        .with_events(events);

        let item = store.claim_ready(INST, 10, 100).unwrap().pop().unwrap();
        assert_eq!(worker.process_item(&item, 100).await, ItemState::Failed);

        let failed = fake.events_named("ReplicationFailed");
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].body["willRetry"], serde_json::json!(true));
        assert_eq!(failed[0].body["attempt"], serde_json::json!(1));
        assert!(failed[0].body["error"].as_str().unwrap().contains("network"));
        assert!(failed[0].body.get("nextAttemptAt").is_some(), "retry has a scheduled next attempt");
        assert!(fake.events_named("RetriesExhausted").is_empty());
    }

    #[tokio::test]
    async fn events_exhaustion_emits_retries_exhausted_then_quarantined() {
        let src = tempfile::tempdir().unwrap();
        let failed = tempfile::tempdir().unwrap();
        let store = store();
        std::fs::write(src.path().join("bad.dat"), b"oops").unwrap();
        store.upsert_ready(INST, "bad.dat", 4, 0, 1).unwrap();
        let mut comp = completion(OnSuccess::Delete);
        comp.on_exhausted = OnExhausted::Quarantine;
        comp.failed_dir = Some(failed.path().to_path_buf());
        let dest: SharedDestination = Arc::new(FailingDest {
            err: || ReplError::Permanent("denied".into()),
        });
        let (fake, events) = recording_events();
        let worker = Worker::new(
            INST.into(),
            store.clone(),
            dest,
            comp,
            src.path().to_path_buf(),
            Bandwidth::unlimited(),
            RetryPolicy::default(),
            None,
        )
        .with_events(events);

        let item = store.claim_ready(INST, 10, 100).unwrap().pop().unwrap();
        assert_eq!(worker.process_item(&item, 100).await, ItemState::Quarantined);

        let exhausted = fake.events_named("RetriesExhausted");
        assert_eq!(exhausted.len(), 1);
        assert_eq!(exhausted[0].body["attempts"], serde_json::json!(1));
        assert!(exhausted[0].body["lastError"].as_str().unwrap().contains("denied"));

        let quarantined = fake.events_named("FileQuarantined");
        assert_eq!(quarantined.len(), 1);
        assert_eq!(quarantined[0].body["path"], serde_json::json!("bad.dat"));
        assert!(quarantined[0].body["quarantinePath"].as_str().unwrap().ends_with("bad.dat"));
        // A permanent failure never rides the retry path.
        assert!(fake.events_named("ReplicationFailed").is_empty());
    }

    #[tokio::test]
    async fn events_progress_emitted_for_a_large_transfer() {
        // A file larger than PROGRESS_STEP_BYTES (4 MiB) drives at least one throttled progress
        // checkpoint through the drainer → a ReplicationProgress event.
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let store = store();
        let big = vec![7u8; (PROGRESS_STEP_BYTES as usize) * 2 + 1024];
        enqueue_file(&store, src.path(), "big.bin", &big);
        let (fake, events) = recording_events();
        let worker = local_worker(
            store.clone(),
            src.path(),
            dst.path(),
            completion(OnSuccess::Delete),
            RetryPolicy::default(),
        )
        .with_events(events);

        let item = store.claim_ready(INST, 10, 100).unwrap().pop().unwrap();
        assert_eq!(worker.process_item(&item, 100).await, ItemState::Completed);

        let progress = fake.events_named("ReplicationProgress");
        assert!(!progress.is_empty(), "a multi-checkpoint transfer emits progress");
        let p0 = &progress[0].body;
        assert_eq!(p0["path"], serde_json::json!("big.bin"));
        assert_eq!(p0["destination"], serde_json::json!("local"));
        assert!(p0["bytesDone"].as_u64().unwrap() > 0);
        assert!(p0["percent"].as_f64().unwrap() >= 0.0);
    }

    #[tokio::test]
    async fn events_progress_honors_zero_and_hundred_percent_endpoints() {
        // FR-EVT-2 "0%/100% always": the first observation and the terminal report (done == size →
        // 100%) must both reach the throttle end-to-end, even though the sub-4-MiB head/tail never
        // crosses the 4 MiB persist gate. Regression for the review finding that the 100%-always
        // guarantee was unreachable in the real pipeline. Sized >13 MiB so the first 64 KiB chunk
        // rounds to a genuine 0% (a smaller file's first observation is already >0%).
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let store = store();
        let big = vec![9u8; 13 * 1024 * 1024];
        enqueue_file(&store, src.path(), "big.bin", &big);
        let (fake, events) = recording_events();
        let worker = local_worker(
            store.clone(),
            src.path(),
            dst.path(),
            completion(OnSuccess::Delete),
            RetryPolicy::default(),
        )
        .with_events(events);

        let item = store.claim_ready(INST, 10, 100).unwrap().pop().unwrap();
        assert_eq!(worker.process_item(&item, 100).await, ItemState::Completed);

        let progress = fake.events_named("ReplicationProgress");
        assert!(progress.len() >= 2, "at least the 0% and 100% endpoints, got {}", progress.len());
        assert!(progress.len() <= 50, "still throttled, not one per chunk (got {})", progress.len());
        let percents: Vec<f64> =
            progress.iter().filter_map(|m| m.body["percent"].as_f64()).collect();
        assert!(percents.contains(&0.0), "the 0% endpoint is emitted (got {percents:?})");
        assert!(percents.contains(&100.0), "the 100% endpoint is emitted (got {percents:?})");
        let last = progress.last().unwrap();
        assert_eq!(last.body["percent"], serde_json::json!(100.0), "final progress is 100%");
        assert_eq!(last.body["bytesDone"].as_u64().unwrap(), big.len() as u64);
    }

    #[tokio::test]
    async fn events_disabled_worker_publishes_nothing() {
        // The default (no messaging) worker runs the identical pipeline with zero events.
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let store = store();
        enqueue_file(&store, src.path(), "x.txt", b"data");
        let worker = local_worker(
            store.clone(),
            src.path(),
            dst.path(),
            completion(OnSuccess::Delete),
            RetryPolicy::default(),
        ); // no .with_events(...)
        let item = store.claim_ready(INST, 10, 100).unwrap().pop().unwrap();
        assert_eq!(worker.process_item(&item, 100).await, ItemState::Completed);
        // Nothing to assert on a fake — the point is it neither panics nor spawns a drainer.
    }

    // ---- P6 multi-destination fan-out (DESIGN §20-B) ---------------------------------------------

    fn local_dest(dst_root: &Path) -> SharedDestination {
        Arc::new(LocalDest::new(
            &LocalEgress {
                path: dst_root.to_path_buf(),
                fsync: false,
            },
            Algorithm::Crc32c,
        ))
    }

    /// A [`LocalDest`] wrapper that counts `deliver` calls — used to prove a destination is (or is
    /// NOT) re-delivered to. `kind` is configurable so a fan-out test can pin distinct destination
    /// labels deterministically.
    struct CountingDest {
        kind: &'static str,
        inner: LocalDest,
        deliver_calls: Arc<AtomicU32>,
    }
    impl CountingDest {
        fn new(dst_root: &Path) -> Self {
            Self::named("counting", dst_root)
        }
        fn named(kind: &'static str, dst_root: &Path) -> Self {
            CountingDest {
                kind,
                inner: LocalDest::new(
                    &LocalEgress {
                        path: dst_root.to_path_buf(),
                        fsync: false,
                    },
                    Algorithm::Crc32c,
                ),
                deliver_calls: Arc::new(AtomicU32::new(0)),
            }
        }
    }
    #[async_trait]
    impl crate::dest::Destination for CountingDest {
        fn kind(&self) -> &'static str {
            self.kind
        }
        fn supports_resume(&self) -> bool {
            self.inner.supports_resume()
        }
        async fn deliver(
            &self,
            item: &WorkItem,
            resume: Option<ResumeState>,
            progress: &ProgressSink,
            bw: &Bandwidth,
        ) -> ReplResult<crate::domain::Delivered> {
            self.deliver_calls.fetch_add(1, AtomicOrdering::SeqCst);
            self.inner.deliver(item, resume, progress, bw).await
        }
        async fn verify(
            &self,
            item: &WorkItem,
            delivered: &crate::domain::Delivered,
            policy: Verify,
        ) -> ReplResult<()> {
            self.inner.verify(item, delivered, policy).await
        }
        async fn abort(&self, item: &WorkItem, resume: &crate::domain::ResumeState) -> ReplResult<()> {
            self.inner.abort(item, resume).await
        }
    }

    #[tokio::test]
    async fn fan_out_completes_once_after_all_destinations_verify() {
        let src = tempfile::tempdir().unwrap();
        let dst_a = tempfile::tempdir().unwrap();
        let dst_b = tempfile::tempdir().unwrap();
        let store = store();
        enqueue_file(&store, src.path(), "f.txt", b"fan-out payload");

        let worker = Worker::new_multi(
            INST.to_string(),
            store.clone(),
            vec![local_dest(dst_a.path()), local_dest(dst_b.path())],
            completion(OnSuccess::Delete),
            src.path().to_path_buf(),
            Bandwidth::unlimited(),
            RetryPolicy::default(),
            None,
        );

        let item = store.claim_ready(INST, 10, 100).unwrap().pop().unwrap();
        let state = worker.process_item(&item, 100).await;

        assert_eq!(state, ItemState::Completed);
        assert_eq!(std::fs::read(dst_a.path().join("f.txt")).unwrap(), b"fan-out payload");
        assert_eq!(std::fs::read(dst_b.path().join("f.txt")).unwrap(), b"fan-out payload");
        assert!(!src.path().join("f.txt").exists(), "source deleted only after BOTH destinations verify");
        assert_eq!(store.get(INST, "f.txt").unwrap().unwrap().state, ItemState::Completed);
        // The completion action (source delete + the `replicated` counter) fires EXACTLY ONCE — not
        // once per destination.
        assert_eq!(store.stats(INST).unwrap().replicated, 1);
        assert!(
            store.dest_states(INST, "f.txt").unwrap().is_empty(),
            "per-destination bookkeeping is cleared on completion"
        );
    }

    #[tokio::test]
    async fn fan_out_one_destination_failing_keeps_item_incomplete_and_source_retained() {
        let src = tempfile::tempdir().unwrap();
        let dst_a = tempfile::tempdir().unwrap();
        let store = store();
        enqueue_file(&store, src.path(), "f.txt", b"payload");

        let failing: SharedDestination = Arc::new(FailingDest {
            err: || ReplError::Permanent("no such bucket".into()),
        });
        let worker = Worker::new_multi(
            INST.to_string(),
            store.clone(),
            vec![local_dest(dst_a.path()), failing],
            completion(OnSuccess::Delete), // retainInPlace on_exhausted (default)
            src.path().to_path_buf(),
            Bandwidth::unlimited(),
            RetryPolicy::default(),
            None,
        );

        let item = store.claim_ready(INST, 10, 100).unwrap().pop().unwrap();
        let state = worker.process_item(&item, 100).await;

        assert_eq!(
            state,
            ItemState::Retained,
            "one destination permanently exhausted -> the item can never complete"
        );
        assert!(src.path().join("f.txt").exists(), "source retained, not deleted");
        assert_eq!(
            std::fs::read(dst_a.path().join("f.txt")).unwrap(),
            b"payload",
            "the destination that DID succeed keeps its delivered data"
        );
        assert_eq!(store.get(INST, "f.txt").unwrap().unwrap().state, ItemState::Retained);
        assert_eq!(store.stats(INST).unwrap().failed, 1);
        assert_eq!(store.stats(INST).unwrap().replicated, 0);
        let states = store.dest_states(INST, "f.txt").unwrap();
        let local = states.iter().find(|d| d.dest == "local").expect("local dest row exists");
        assert_eq!(local.phase, DestPhase::Verified, "the succeeded destination stays Verified");
        let failed = states.iter().find(|d| d.dest == "failing").expect("failing dest row exists");
        assert_eq!(failed.phase, DestPhase::Exhausted);
    }

    #[tokio::test]
    async fn fan_out_recovery_redelivers_only_the_unverified_destination_and_completes_once() {
        // Simulate a crash AFTER destination "local" was delivered + verified (its write-ahead
        // DestPhase::Verified + resume checkpoint already persisted) but BEFORE destination "counting"
        // ever ran (no dest_state row for it at all) — the item is left `InProgress`.
        let src = tempfile::tempdir().unwrap();
        let dst_a = tempfile::tempdir().unwrap();
        let dst_b = tempfile::tempdir().unwrap();
        let store = store();
        std::fs::write(src.path().join("f.txt"), b"payload").unwrap();

        let dest_a_backend = LocalDest::new(
            &LocalEgress {
                path: dst_a.path().to_path_buf(),
                fsync: false,
            },
            Algorithm::Crc32c,
        );
        let seed_item = WorkItem {
            instance: INST.into(),
            relpath: "f.txt".into(),
            abs_source: src.path().join("f.txt"),
            state: ItemState::InProgress,
            size: 7,
            discovered_at: 0,
            attempts: 0,
            next_attempt_at: 0,
            last_error: None,
            bytes_done: 0,
            updated_at: 0,
        };
        let delivered_a = dest_a_backend
            .deliver(&seed_item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .unwrap();

        store.upsert_ready(INST, "f.txt", 7, 0, 1).unwrap();
        store.set_state(INST, "f.txt", ItemState::InProgress, 2).unwrap();
        store
            .save_resume(
                INST,
                "f.txt",
                "local",
                &ResumeState {
                    bytes_committed: delivered_a.bytes,
                    token: serde_json::json!({ "verified": serde_json::to_value(&delivered_a).unwrap() }),
                },
            )
            .unwrap();
        store.set_dest_phase(INST, "f.txt", "local", DestPhase::Verified, 2).unwrap();
        // No dest_state row at all for "counting" — implicitly Pending.

        let counting = CountingDest::new(dst_b.path());
        let delivers_b = counting.deliver_calls.clone();
        let worker = Worker::new_multi(
            INST.to_string(),
            store.clone(),
            vec![Arc::new(dest_a_backend), Arc::new(counting)],
            completion(OnSuccess::Delete),
            src.path().to_path_buf(),
            Bandwidth::unlimited(),
            RetryPolicy::default(),
            None,
        );

        // Crash recovery: the aggregate item was InProgress → reverts to Ready. Per-destination
        // bookkeeping (the "local" DestPhase::Verified row) is untouched by an InProgress recovery.
        worker.recover(100).await.unwrap();
        assert_eq!(store.get(INST, "f.txt").unwrap().unwrap().state, ItemState::Ready);

        // Re-claim and process: must re-deliver ONLY to "counting" (the unverified destination) — the
        // already-`Verified` "local" destination is skipped entirely (no re-delivery).
        let item = store.claim_ready(INST, 10, 200).unwrap().pop().unwrap();
        let state = worker.process_item(&item, 200).await;

        assert_eq!(state, ItemState::Completed);
        assert_eq!(
            delivers_b.load(AtomicOrdering::SeqCst),
            1,
            "only the unverified destination is (re-)delivered"
        );
        assert_eq!(std::fs::read(dst_b.path().join("f.txt")).unwrap(), b"payload");
        assert!(!src.path().join("f.txt").exists());
        assert_eq!(store.stats(INST).unwrap().replicated, 1, "completes exactly once");
    }

    #[tokio::test]
    async fn fan_out_honors_each_destinations_independent_backoff_gate() {
        // Two not-yet-verified destinations with DIFFERENT backoff gates: only the destination whose own
        // clock is due is (re-)attempted this round; the one still in backoff is left untouched, and the
        // item re-claims at the earliest still-pending gate — proving retries are per-destination
        // independent (the fix for "a slow destination retried at its fastest sibling's cadence, its
        // attempts inflated toward premature exhaustion"), FR-EGR-3 / DESIGN §20-B.
        let src = tempfile::tempdir().unwrap();
        let dst_a = tempfile::tempdir().unwrap();
        let dst_b = tempfile::tempdir().unwrap();
        let store = store();
        std::fs::write(src.path().join("f.txt"), b"payload").unwrap();

        let a = CountingDest::named("ay", dst_a.path());
        let b = CountingDest::named("bee", dst_b.path());
        let a_calls = a.deliver_calls.clone();
        let b_calls = b.deliver_calls.clone();
        let worker = Worker::new_multi(
            INST.to_string(),
            store.clone(),
            vec![Arc::new(a), Arc::new(b)],
            completion(OnSuccess::Delete),
            src.path().to_path_buf(),
            Bandwidth::unlimited(),
            RetryPolicy::default(),
            None,
        );

        // Seed a partially-fanned-out item: both destinations Pending, but A is due (gate 0) while B is
        // parked on a long backoff (gate 5_000). The item is claimed (InProgress).
        store.upsert_ready(INST, "f.txt", 7, 0, 1).unwrap();
        store
            .record_dest_attempt(INST, "f.txt", "ay", "earlier", DestPhase::Pending, 0, 1)
            .unwrap();
        store
            .record_dest_attempt(INST, "f.txt", "bee", "earlier", DestPhase::Pending, 5_000, 1)
            .unwrap();
        store.set_state(INST, "f.txt", ItemState::InProgress, 1).unwrap();

        let item = store.get(INST, "f.txt").unwrap().unwrap();
        assert_eq!(worker.process_item(&item, 1_000).await, ItemState::Failed);

        assert_eq!(a_calls.load(AtomicOrdering::SeqCst), 1, "the due destination is attempted");
        assert_eq!(
            b_calls.load(AtomicOrdering::SeqCst),
            0,
            "the destination still in backoff is NOT re-attempted at its sibling's cadence"
        );
        let it = store.get(INST, "f.txt").unwrap().unwrap();
        assert_eq!(
            it.next_attempt_at, 5_000,
            "item re-claims at the earliest still-pending destination's own gate"
        );
        let states = store.dest_states(INST, "f.txt").unwrap();
        let bee = states.iter().find(|d| d.dest == "bee").unwrap();
        assert_eq!(bee.phase, DestPhase::Pending);
        assert_eq!(bee.next_attempt_at, 5_000, "not-due destination's backoff clock untouched");
        assert_eq!(bee.attempts, 1, "not-due destination's attempt count not inflated");
        let ay = states.iter().find(|d| d.dest == "ay").unwrap();
        assert_eq!(ay.phase, DestPhase::Verified, "the attempted destination delivered + verified");

        // Once B's clock elapses it is attempted and the item completes exactly once; A (already
        // Verified) is never re-delivered.
        store.set_state(INST, "f.txt", ItemState::InProgress, 6_000).unwrap();
        let item = store.get(INST, "f.txt").unwrap().unwrap();
        assert_eq!(worker.process_item(&item, 6_000).await, ItemState::Completed);
        assert_eq!(a_calls.load(AtomicOrdering::SeqCst), 1, "already-Verified destination never re-delivered");
        assert_eq!(b_calls.load(AtomicOrdering::SeqCst), 1, "the now-due destination delivered");
        assert_eq!(store.stats(INST).unwrap().replicated, 1);
        assert!(!src.path().join("f.txt").exists(), "source released only after BOTH verify");
    }

    #[tokio::test]
    async fn crash_recovery_reverifies_prior_session_verified_destination_no_data_loss() {
        // FR-CMP-3/4 fan-out data-loss guard: an item left `InProgress` with destination A already
        // `DestPhase::Verified` from a PRIOR session, whose object was then CORRUPTED during downtime,
        // must NOT complete (delete the source) trusting the stale row. Recovery re-verifies A, catches
        // the corruption by re-hash, demotes it to Pending, and the next claim re-delivers A (healing its
        // copy) as well as the still-pending B — completing only after BOTH truly verify. Without the
        // re-verify, run_pipeline would skip A on trust and the source would be deleted with A corrupt.
        let src = tempfile::tempdir().unwrap();
        let dst_a = tempfile::tempdir().unwrap();
        let dst_b = tempfile::tempdir().unwrap();
        let store = store();
        std::fs::write(src.path().join("f.txt"), b"payload").unwrap();

        // Deliver to A for real to capture a genuine Verified checkpoint (checksum), then corrupt A's
        // object in place (same length, different bytes — only a checksum re-verify catches it).
        let a_backend = LocalDest::new(
            &LocalEgress { path: dst_a.path().to_path_buf(), fsync: false },
            Algorithm::Crc32c,
        );
        let seed_item = WorkItem {
            instance: INST.into(),
            relpath: "f.txt".into(),
            abs_source: src.path().join("f.txt"),
            state: ItemState::InProgress,
            size: 7,
            discovered_at: 0,
            attempts: 0,
            next_attempt_at: 0,
            last_error: None,
            bytes_done: 0,
            updated_at: 0,
        };
        let delivered_a = a_backend
            .deliver(&seed_item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .unwrap();
        std::fs::write(dst_a.path().join("f.txt"), b"CORRUPT").unwrap(); // 7 bytes, wrong content

        store.upsert_ready(INST, "f.txt", 7, 0, 1).unwrap();
        store.set_state(INST, "f.txt", ItemState::InProgress, 2).unwrap();
        store
            .save_resume(
                INST,
                "f.txt",
                "ay",
                &ResumeState {
                    bytes_committed: delivered_a.bytes,
                    token: serde_json::json!({ "verified": serde_json::to_value(&delivered_a).unwrap() }),
                },
            )
            .unwrap();
        store.set_dest_phase(INST, "f.txt", "ay", DestPhase::Verified, 2).unwrap();
        // No dest_state row for "bee" — implicitly Pending.

        let a = CountingDest::named("ay", dst_a.path());
        let b = CountingDest::named("bee", dst_b.path());
        let a_calls = a.deliver_calls.clone();
        let b_calls = b.deliver_calls.clone();
        let worker = Worker::new_multi(
            INST.to_string(),
            store.clone(),
            vec![Arc::new(a), Arc::new(b)],
            completion(OnSuccess::Delete),
            src.path().to_path_buf(),
            Bandwidth::unlimited(),
            RetryPolicy::default(),
            None,
        );

        // Recovery: InProgress → re-verify A's prior-session Verified phase. The corrupted object fails
        // re-verification → A is demoted to Pending, its stale checkpoint cleared, item re-readied.
        worker.recover(100).await.unwrap();
        assert_eq!(store.get(INST, "f.txt").unwrap().unwrap().state, ItemState::Ready);
        let ay = store
            .dest_states(INST, "f.txt")
            .unwrap()
            .into_iter()
            .find(|d| d.dest == "ay")
            .unwrap();
        assert_eq!(ay.phase, DestPhase::Pending, "corrupted prior-session Verified destination demoted");
        assert!(store.load_resume(INST, "f.txt", "ay").unwrap().is_none(), "stale checkpoint cleared");
        assert_eq!(a_calls.load(AtomicOrdering::SeqCst), 0, "recovery re-verifies, never re-delivers");

        // Next claim re-delivers BOTH (A because it was demoted): completes only after both truly verify.
        let item = store.claim_ready(INST, 10, 200).unwrap().pop().unwrap();
        assert_eq!(worker.process_item(&item, 200).await, ItemState::Completed);
        assert_eq!(a_calls.load(AtomicOrdering::SeqCst), 1, "the demoted destination is re-delivered (copy healed)");
        assert_eq!(b_calls.load(AtomicOrdering::SeqCst), 1, "the pending destination delivered");
        assert_eq!(
            std::fs::read(dst_a.path().join("f.txt")).unwrap(),
            b"payload",
            "A's corrupted object replaced with the correct bytes — no silent data loss"
        );
        assert!(!src.path().join("f.txt").exists(), "source released only after both truly verified");
        assert_eq!(store.stats(INST).unwrap().replicated, 1);
    }

    #[tokio::test]
    async fn recover_verified_multi_dest_requeues_only_the_failed_destination() {
        // The N>1 partial-re-verify branch of recover_verified: an aggregate-`Verified` item where dest A
        // re-verifies clean but dest B's object was lost during downtime. Only B is demoted (checkpoint
        // cleared) and the item reverts to Ready; A stays Verified and is NOT re-delivered on the next
        // claim, which re-delivers only B and completes exactly once (FR-CMP-3/4, DESIGN §20-B).
        let src = tempfile::tempdir().unwrap();
        let dst_a = tempfile::tempdir().unwrap();
        let dst_b = tempfile::tempdir().unwrap();
        let store = store();
        std::fs::write(src.path().join("f.txt"), b"payload").unwrap();

        // A: deliver for real → genuine object + checkpoint. B: seed a Verified checkpoint but leave its
        // object ABSENT (a wiped mount), so B's re-verify fails while A's passes.
        let a_backend = LocalDest::new(
            &LocalEgress { path: dst_a.path().to_path_buf(), fsync: false },
            Algorithm::Crc32c,
        );
        let seed_item = WorkItem {
            instance: INST.into(),
            relpath: "f.txt".into(),
            abs_source: src.path().join("f.txt"),
            state: ItemState::InProgress,
            size: 7,
            discovered_at: 0,
            attempts: 0,
            next_attempt_at: 0,
            last_error: None,
            bytes_done: 0,
            updated_at: 0,
        };
        let delivered_a = a_backend
            .deliver(&seed_item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .unwrap();
        let fake_b = Delivered {
            bytes: 7,
            checksum: delivered_a.checksum.clone(),
            handle: serde_json::Value::Null,
        };

        store.upsert_ready(INST, "f.txt", 7, 0, 1).unwrap();
        store.set_state(INST, "f.txt", ItemState::Verified, 2).unwrap();
        store
            .save_resume(
                INST,
                "f.txt",
                "ay",
                &ResumeState {
                    bytes_committed: delivered_a.bytes,
                    token: serde_json::json!({ "verified": serde_json::to_value(&delivered_a).unwrap() }),
                },
            )
            .unwrap();
        store.set_dest_phase(INST, "f.txt", "ay", DestPhase::Verified, 2).unwrap();
        store
            .save_resume(
                INST,
                "f.txt",
                "bee",
                &ResumeState {
                    bytes_committed: 7,
                    token: serde_json::json!({ "verified": serde_json::to_value(&fake_b).unwrap() }),
                },
            )
            .unwrap();
        store.set_dest_phase(INST, "f.txt", "bee", DestPhase::Verified, 2).unwrap();

        let a = CountingDest::named("ay", dst_a.path());
        let b = CountingDest::named("bee", dst_b.path());
        let a_calls = a.deliver_calls.clone();
        let b_calls = b.deliver_calls.clone();
        let worker = Worker::new_multi(
            INST.to_string(),
            store.clone(),
            vec![Arc::new(a), Arc::new(b)],
            completion(OnSuccess::Delete),
            src.path().to_path_buf(),
            Bandwidth::unlimited(),
            RetryPolicy::default(),
            None,
        );

        worker.recover(100).await.unwrap();

        // A re-verified clean (stays Verified, checkpoint intact); B failed → demoted, checkpoint cleared;
        // the aggregate item reverted to Ready. Neither destination was re-delivered during recovery.
        assert_eq!(store.get(INST, "f.txt").unwrap().unwrap().state, ItemState::Ready);
        let states = store.dest_states(INST, "f.txt").unwrap();
        let ay = states.iter().find(|d| d.dest == "ay").unwrap();
        assert_eq!(ay.phase, DestPhase::Verified, "the intact destination stays Verified (not re-delivered)");
        let bee = states.iter().find(|d| d.dest == "bee").unwrap();
        assert_eq!(bee.phase, DestPhase::Pending, "the lost destination is demoted");
        assert!(store.load_resume(INST, "f.txt", "ay").unwrap().is_some(), "A's checkpoint preserved");
        assert!(store.load_resume(INST, "f.txt", "bee").unwrap().is_none(), "B's stale checkpoint cleared");
        assert_eq!(a_calls.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(b_calls.load(AtomicOrdering::SeqCst), 0);

        // The next claim re-delivers ONLY B (A is skipped) and completes exactly once.
        let item = store.claim_ready(INST, 10, 200).unwrap().pop().unwrap();
        assert_eq!(worker.process_item(&item, 200).await, ItemState::Completed);
        assert_eq!(a_calls.load(AtomicOrdering::SeqCst), 0, "already-Verified destination never re-delivered");
        assert_eq!(b_calls.load(AtomicOrdering::SeqCst), 1, "only the requeued destination is re-delivered");
        assert_eq!(std::fs::read(dst_b.path().join("f.txt")).unwrap(), b"payload");
        assert!(!src.path().join("f.txt").exists());
        assert_eq!(store.stats(INST).unwrap().replicated, 1, "completes exactly once");
    }
}

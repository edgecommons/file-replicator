//! # file-replicator — per-file worker (DESIGN §8.1/§13)
//!
//! Drives one claimed [`WorkItem`] through the crash-safe pipeline and owns the retry/backoff and
//! completion-action policy:
//!
//! ```text
//! deliver → verify → persist Verified (write-ahead) → completion (delete|archive) → persist Completed
//!                 └─ error → record attempt + backoff → Failed (retry) | Exhausted → quarantine|retain
//! ```
//!
//! The **write-ahead** ordering is load-bearing (DESIGN §13.2): `Verified` is persisted *before* the
//! source side effect and `Completed` *after*, so a crash at any point is recovered idempotently by
//! [`recover`](Worker::recover) — the destination object already matches (stable key → idempotent
//! overwrite), so completion simply re-runs; a file is never re-uploaded and never lost.
//!
//! Failures are classified by [`ReplError`](crate::error::ReplError): *permanent* errors fail fast to
//! `Exhausted`; *transient*/*integrity* errors back off with **full-jitter exponential** delay capped
//! at `maxDelayMs`, retrying until the **time-based** `giveUpAfter` budget (default 7 days) or an
//! optional `maxAttempts` cap — built for hours-to-days offline (DESIGN §13.4/§13.6).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use ggcommons::metrics::MetricService;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::config::{Collision, CompletionCfg, OnExhausted, OnSuccess, RetryCfg, Verify};
use crate::dest::SharedDestination;
use crate::domain::{Checksum, Delivered, ItemState, ProgressSink, ResumeState, WorkItem};
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

/// The per-file driver for one instance. Shared (`Arc<Worker>`) across the instance's worker tasks.
pub struct Worker {
    instance: String,
    store: Arc<dyn StateStore>,
    dest: SharedDestination,
    completion: CompletionCfg,
    /// Source root — used to rebuild each item's absolute path (the store keeps only `relpath`).
    ingress_root: PathBuf,
    bw: Bandwidth,
    retry: RetryPolicy,
    /// Backoff jitter source (seedable for deterministic tests).
    rng: Mutex<StdRng>,
    /// Optional metric emitter (`filesReplicated`/`bytesReplicated`/`filesFailed`).
    metrics: Option<Arc<dyn MetricService>>,
    /// UNS event emitter for the per-file lifecycle events (`ReplicationStarted`/`…Progress`/
    /// `…Completed`/`…Failed`/`FileDeleted`/`FileArchived`/`RetriesExhausted`/`FileQuarantined`,
    /// DESIGN §17). A no-op [`Events::disabled`] by default, so a worker built without messaging (or
    /// in a unit test) runs the exact P1/P2 pipeline with zero event overhead.
    events: Events,
}

impl Worker {
    /// Build a worker. `ingress_root` is the instance's source directory; `bw` is the per-instance ∩
    /// global bandwidth governor; `retry` is the resolved policy.
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
        Worker {
            instance,
            store,
            dest,
            completion,
            ingress_root,
            bw,
            retry,
            rng: Mutex::new(StdRng::from_entropy()),
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

        // The item was claimed (`Ready → InProgress`) by the queue before it reached us — emit the
        // start-of-transfer lifecycle event (DESIGN §17.1). `attempt` is 1-based (attempts so far + 1).
        self.events
            .instance_event(
                &self.instance,
                Event::ReplicationStarted {
                    path: item.relpath.clone(),
                    size: item.size,
                    destination: self.dest.kind().to_string(),
                    attempt: item.attempts + 1,
                },
                now,
            )
            .await;

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

    /// The deliver → verify → completion pipeline (write-ahead ordering per DESIGN §13.2). Returns the
    /// terminal state; `Err` is a durable-store fault only (transfer errors are handled internally).
    async fn run_pipeline(&self, item: &WorkItem, now: i64) -> Result<ItemState> {
        let resume = self
            .store
            .load_resume(&self.instance, &item.relpath, self.dest.kind())?;
        let (progress, drainer) = self.make_progress(item);

        let outcome = async {
            let delivered = self.dest.deliver(item, resume, &progress, &self.bw).await?;
            self.dest
                .verify(item, &delivered, self.completion.verify)
                .await?;
            Ok::<Delivered, ReplError>(delivered)
        }
        .await;

        // Drop our sink handle so the progress channel closes once every destination-held clone is
        // gone (deliver has returned), then flush any still-queued `ReplicationProgress` events.
        drop(progress);
        if let Some(d) = drainer {
            let _ = d.await;
        }

        match outcome {
            Ok(delivered) => {
                // Write-ahead the FULL expected result at the Verified point (DESIGN §13.2): persist
                // the delivered checksum + handle as the resume checkpoint BEFORE marking Verified and
                // BEFORE the source side effect, so crash recovery re-verifies the destination by
                // re-hash (not just size) before deleting/archiving the source. This overwrites any
                // mid-transfer resume token (e.g. an S3 uploadId) — the transfer is complete.
                self.save_verified_checkpoint(item, &delivered);
                self.store
                    .set_state(&self.instance, &item.relpath, ItemState::Verified, now)?;
                self.complete_verified(item, delivered.bytes, now).await
            }
            Err(e) => self.handle_failure(item, e, now).await,
        }
    }

    /// Persist the delivered result as the `Verified` write-ahead checkpoint (DESIGN §13.2). The
    /// whole [`Delivered`] rides in [`ResumeState::token`] under `"verified"`, reusing the resume row
    /// (single blob per `(instance, relpath, dest)`) across lifecycle phases — no schema change.
    /// Best-effort: a store fault here only costs recovery its checksum-precision (it falls back to a
    /// size check), so it must not fail the transfer that already succeeded.
    fn save_verified_checkpoint(&self, item: &WorkItem, delivered: &Delivered) {
        let token = match serde_json::to_value(delivered) {
            Ok(v) => serde_json::json!({ "verified": v }),
            Err(e) => {
                tracing::warn!(relpath = %item.relpath, error = %e, "serialize verified checkpoint failed");
                return;
            }
        };
        let checkpoint = ResumeState {
            bytes_committed: delivered.bytes,
            token,
        };
        if let Err(e) =
            self.store
                .save_resume(&self.instance, &item.relpath, self.dest.kind(), &checkpoint)
        {
            tracing::warn!(relpath = %item.relpath, error = %e, "persist verified checkpoint failed");
        }
    }

    /// On give-up (terminal), abort any in-flight partial transfer (S3 → `AbortMultipartUpload`;
    /// local → temp cleanup) so no orphaned upload lingers, then drop the resume checkpoint. The
    /// resume is deliberately **kept** on the retry path (so the next attempt resumes) and cleared
    /// only here at terminal give-up (DESIGN §11.3/§13.4). Idempotent + best-effort.
    async fn abort_and_clear_resume(&self, item: &WorkItem) {
        match self
            .store
            .load_resume(&self.instance, &item.relpath, self.dest.kind())
        {
            Ok(Some(resume)) => {
                if let Err(e) = self.dest.abort(item, &resume).await {
                    tracing::warn!(
                        relpath = %item.relpath, error = %e,
                        "aborting partial transfer on give-up failed"
                    );
                }
            }
            Ok(None) => {}
            Err(e) => tracing::warn!(relpath = %item.relpath, error = %e, "load resume for give-up abort failed"),
        }
        let _ = self
            .store
            .clear_resume(&self.instance, &item.relpath, self.dest.kind());
    }

    /// Run the success completion action (`delete` | `archive`) on the source, then persist
    /// `Completed`. Idempotent: a missing source (already completed before a crash) is fine. Filesystem
    /// errors are logged but still advance to `Completed` — the delivery succeeded and the durable
    /// `Completed` marker stops the file being re-discovered, so a stale source can never loop.
    ///
    /// The source-side filesystem work runs on the blocking pool: a cross-device archive falls back to
    /// a whole-file copy, which must not block a shared async worker thread (DESIGN §6.3).
    async fn complete_verified(&self, item: &WorkItem, bytes: u64, now: i64) -> Result<ItemState> {
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
        // Clear the resume checkpoint only AFTER Completed is durable, so the expected-checksum
        // checkpoint survives a crash in the Verified→Completed gap and recovery can still re-verify.
        let _ = self
            .store
            .clear_resume(&self.instance, &item.relpath, self.dest.kind());
        self.store.bump_stats(
            &self.instance,
            StatsDelta {
                replicated: 1,
                failed: 0,
                bytes: bytes as i64,
            },
        )?;

        // Lifecycle events (DESIGN §17.1): the completion, then the source side effect that ran.
        self.events
            .instance_event(
                &self.instance,
                Event::ReplicationCompleted {
                    path: item.relpath.clone(),
                    size: item.size,
                    destination: self.dest.kind().to_string(),
                    bytes,
                },
                now,
            )
            .await;
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

    /// A transfer error: record the attempt with backoff, or (on give-up / a permanent error) move to
    /// `Exhausted` and run the `onExhausted` action.
    async fn handle_failure(&self, item: &WorkItem, err: ReplError, now: i64) -> Result<ItemState> {
        let permanent = err.is_permanent();
        let err_str = err.to_string();
        let decision = {
            let mut rng = self.rng.lock().expect("rng mutex");
            self.retry.decide(item, permanent, now, &mut *rng)
        };
        match decision {
            RetryDecision::Retry { next_attempt_at } => {
                self.store.record_attempt(
                    &self.instance,
                    &item.relpath,
                    &err_str,
                    ItemState::Failed,
                    next_attempt_at,
                    now,
                )?;
                tracing::warn!(
                    instance = %self.instance, relpath = %item.relpath,
                    attempts = item.attempts + 1, retry_at = next_attempt_at, error = %err_str,
                    "transfer failed; scheduled for retry"
                );
                self.events
                    .instance_event(
                        &self.instance,
                        Event::ReplicationFailed {
                            path: item.relpath.clone(),
                            destination: self.dest.kind().to_string(),
                            attempt: item.attempts + 1,
                            error: err_str,
                            next_attempt_at_ms: Some(next_attempt_at),
                        },
                        now,
                    )
                    .await;
                Ok(ItemState::Failed)
            }
            RetryDecision::GiveUp => {
                // Write-ahead Exhausted, then run the terminal onExhausted action.
                self.store.record_attempt(
                    &self.instance,
                    &item.relpath,
                    &err_str,
                    ItemState::Exhausted,
                    now,
                    now,
                )?;
                self.store.bump_stats(
                    &self.instance,
                    StatsDelta {
                        replicated: 0,
                        failed: 1,
                        bytes: 0,
                    },
                )?;
                // Terminal give-up: release any in-flight partial upload + drop its resume checkpoint.
                self.abort_and_clear_resume(item).await;
                let attempts = item.attempts.saturating_add(1);
                tracing::error!(
                    instance = %self.instance, relpath = %item.relpath, attempts, error = %err_str,
                    "retries exhausted"
                );
                self.events
                    .instance_event(
                        &self.instance,
                        Event::RetriesExhausted {
                            path: item.relpath.clone(),
                            destination: self.dest.kind().to_string(),
                            attempts,
                            last_error: err_str.clone(),
                        },
                        now,
                    )
                    .await;
                self.finish_exhausted(item, &err_str, attempts, now).await
            }
        }
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
                    destination: self.dest.kind(),
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
    async fn recover_verified(&self, it: &WorkItem, now: i64) -> Result<()> {
        let resume = self
            .store
            .load_resume(&self.instance, &it.relpath, self.dest.kind())?;
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

        match self.dest.verify(it, &expected, policy).await {
            Ok(()) => {
                tracing::info!(instance = %self.instance, relpath = %it.relpath, "recovering Verified → re-verified, completing");
                let _ = self.complete_verified(it, expected.bytes, now).await?;
            }
            Err(e) => {
                tracing::warn!(
                    instance = %self.instance, relpath = %it.relpath, error = %e,
                    "Verified item failed destination re-verification on recovery; requeuing for re-delivery"
                );
                self.store
                    .set_state(&self.instance, &it.relpath, ItemState::Ready, now)?;
                let _ = self
                    .store
                    .clear_resume(&self.instance, &it.relpath, self.dest.kind());
            }
        }
        Ok(())
    }

    /// Build the [`ProgressSink`] the destination streams byte-progress into, plus (only when the UNS
    /// event stream is live) a drainer task that turns those checkpoints into throttled
    /// `ReplicationProgress` events (DESIGN §17.2 / FR-EVT-2).
    ///
    /// The sink persists `bytes_done` to the durable store at most once per [`PROGRESS_STEP_BYTES`]
    /// (for `get-status`/resume) and, when events are enabled, forwards a checkpoint over an unbounded
    /// channel — a non-blocking send from the sync callback, so the transfer never awaits the broker.
    /// The drainer applies the percent-step/interval [`ProgressThrottle`] and publishes, ending when
    /// the transfer drops every clone of the sink (closing the channel). When events are disabled the
    /// sink is exactly the P1/P2 store-persist sink and no task is spawned (zero overhead, identical
    /// behavior).
    ///
    /// To honor the FR-EVT-2 "0%/100% always" guarantee, the events-enabled sink forwards the **first**
    /// observation and the **terminal** (`done == size`) report unconditionally — not just the ≥4 MiB
    /// persist checkpoints. Otherwise the sub-4 MiB head/tail is swallowed by the persist gate and the
    /// throttle never sees percent 0 or 100 (small files would emit no progress at all).
    fn make_progress(&self, item: &WorkItem) -> (ProgressSink, Option<tokio::task::JoinHandle<()>>) {
        let store = self.store.clone();
        let instance = self.instance.clone();
        let relpath = item.relpath.clone();
        let last = Arc::new(AtomicU64::new(0));

        if !self.events.is_enabled() {
            let sink = ProgressSink::new(move |done| {
                persist_progress(&store, &instance, &relpath, &last, done);
            });
            return (sink, None);
        }

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<u64>();
        let events = self.events.clone();
        let id = self.instance.clone();
        let path = item.relpath.clone();
        let size = item.size;
        let destination = self.dest.kind().to_string();
        let attempt = item.attempts + 1;
        let drainer = tokio::spawn(async move {
            let mut throttle = ProgressThrottle::with_defaults();
            while let Some(done) = rx.recv().await {
                let pct = percent_int(done, size);
                let now = now_ms();
                if throttle.should_emit(pct, now) {
                    events
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

        let size_bytes = item.size;
        let first = Arc::new(AtomicBool::new(true));
        let sink = ProgressSink::new(move |done| {
            let checkpoint = persist_progress(&store, &instance, &relpath, &last, done);
            // Always forward the first observation (→ the throttle's guaranteed 0%) and the terminal
            // `done == size` report (→ the guaranteed 100%), plus every ≥4 MiB persist checkpoint in
            // between. The drainer's [`ProgressThrottle`] then decides what actually publishes.
            let is_first = first.swap(false, Ordering::Relaxed);
            let is_terminal = size_bytes > 0 && done >= size_bytes;
            if checkpoint || is_first || is_terminal {
                let _ = tx.send(done);
            }
        });
        (sink, Some(drainer))
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
    destination: &'static str,
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
}

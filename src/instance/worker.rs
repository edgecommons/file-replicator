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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use ggcommons::metrics::MetricService;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::config::{Collision, CompletionCfg, OnExhausted, OnSuccess, RetryCfg, Verify};
use crate::dest::SharedDestination;
use crate::domain::{Checksum, Delivered, ItemState, ProgressSink, WorkItem};
use crate::error::{ReplError, Result};
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
        }
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
        let progress = self.progress_sink(&item.relpath);

        let outcome = async {
            let delivered = self.dest.deliver(item, resume, &progress, &self.bw).await?;
            self.dest
                .verify(item, &delivered, self.completion.verify)
                .await?;
            Ok::<u64, ReplError>(delivered.bytes)
        }
        .await;

        match outcome {
            Ok(bytes) => {
                // Write-ahead: persist Verified BEFORE touching the source (crash-safe).
                self.store
                    .set_state(&self.instance, &item.relpath, ItemState::Verified, now)?;
                let _ = self
                    .store
                    .clear_resume(&self.instance, &item.relpath, self.dest.kind());
                self.complete_verified(item, bytes, now).await
            }
            Err(e) => self.handle_failure(item, e, now).await,
        }
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
        self.store.bump_stats(
            &self.instance,
            StatsDelta {
                replicated: 1,
                failed: 0,
                bytes: bytes as i64,
            },
        )?;
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
                let attempts = item.attempts.saturating_add(1);
                tracing::error!(
                    instance = %self.instance, relpath = %item.relpath, attempts, error = %err_str,
                    "retries exhausted"
                );
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
    /// NAS mount, a bucket-lifecycle deletion). Completing (deleting/archiving the source) without
    /// checking would destroy the only copy. So **re-verify the destination first**: a size check
    /// against the persisted expected size (the original streaming checksum is no longer held). If it
    /// still matches, complete; if it is missing/truncated, requeue to `Ready` for idempotent
    /// re-delivery rather than risk data loss (FR-CMP-3).
    async fn recover_verified(&self, it: &WorkItem, now: i64) -> Result<()> {
        let expected = Delivered {
            bytes: it.size,
            checksum: Checksum::None,
            handle: serde_json::Value::Null,
        };
        match self.dest.verify(it, &expected, Verify::Size).await {
            Ok(()) => {
                tracing::info!(instance = %self.instance, relpath = %it.relpath, "recovering Verified → re-verified, completing");
                let _ = self.complete_verified(it, it.size, now).await?;
            }
            Err(e) => {
                tracing::warn!(
                    instance = %self.instance, relpath = %it.relpath, error = %e,
                    "Verified item failed destination re-verification on recovery; requeuing for re-delivery"
                );
                self.store
                    .set_state(&self.instance, &it.relpath, ItemState::Ready, now)?;
            }
        }
        Ok(())
    }

    /// A [`ProgressSink`] that persists `bytes_done` at most once per [`PROGRESS_STEP_BYTES`].
    fn progress_sink(&self, relpath: &str) -> ProgressSink {
        let store = self.store.clone();
        let instance = self.instance.clone();
        let relpath = relpath.to_string();
        let last = Arc::new(AtomicU64::new(0));
        ProgressSink::new(move |done| {
            let prev = last.load(Ordering::Relaxed);
            if done.saturating_sub(prev) >= PROGRESS_STEP_BYTES {
                last.store(done, Ordering::Relaxed);
                if let Err(e) = store.set_bytes_done(&instance, &relpath, done, now_ms()) {
                    tracing::debug!(relpath = %relpath, error = %e, "persist progress failed");
                }
            }
        })
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
    use crate::dest::LocalDest;
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
}

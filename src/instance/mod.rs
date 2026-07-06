//! # file-replicator — the per-instance runtime (DESIGN §6.3/§8.2)
//!
//! One [`Instance`] per `component.instances[]` entry, assembled by [`Instance::build`] and driven by
//! [`Instance::run`]: it owns the [`Watcher`] (discovery + readiness gate), the [`Queue`] (durable
//! work-front + concurrency slots), the [`Worker`] (per-file deliver→verify→complete + retry), and the
//! persisted **activation** flag. Instances are isolated — a slow or failed destination on one never
//! stalls another (independent tasks; only the global concurrency semaphore + global bandwidth bucket
//! are shared).
//!
//! ## Testable seam
//! [`Instance::run`] is a thin loop: it recovers durable state, then on every reconciliation-rescan
//! tick (and every OS-watch nudge) calls [`Instance::tick`], which does the real work
//! (discover → enqueue → claim → process a bounded batch). Tests drive [`tick`](Instance::tick)
//! directly with an explicit `now`, so behavior never depends on filesystem-event or timer timing.
//!
//! ## Scheduling (P4, DESIGN §12)
//! Every tick evaluates the instance's [`Schedule`] against `now` to decide whether ready work may be
//! claimed (immediate = always; cron = release all ready work at each fire; window = flow only while
//! open). Ready work that becomes available while the gate is closed simply accumulates in the durable
//! `Ready` backlog (discovery/enqueue always runs) until the next admission. A transfer still running
//! when a window closes obeys `onWindowClose`: `pauseResume` cancels it (best-effort, cooperative —
//! [`JoinSet::abort_all`]) and reverts it to `Ready` so its already-persisted resume checkpoint is
//! picked back up next open, **iff** the destination reports [`Destination::supports_resume`]; otherwise
//! (and always for `finishCurrent`) the transfer is left to finish. See [`Instance::run_batch_windowed`].

pub mod queue;
pub mod watcher;
pub mod worker;

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, TimeZone, Utc};
use edgecommons::metrics::MetricService;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::admission::PriorityGate;
use crate::config::{EgressCfg, GlobalCfg, InstanceCfg, ScheduleCfg, WindowClose};
use crate::control::InstanceControl;
use crate::dest::{build_destination, DestDeps, SharedDestination};
use crate::domain::{ItemState, WorkItem};
use crate::error::Result;
use crate::events::{Event, Events};
use crate::permission::{PermissionLog, Role};
use crate::ratelimit::{parse_byte_rate, Bandwidth, Clock, SystemClock, TokenBucket};
use crate::readiness::RealProbe;
use crate::schedule::{Admission, Schedule};
use crate::state::StateStore;

use queue::Queue;
use watcher::{ScanIssue, Watcher};
use worker::{RetryPolicy, Worker};

/// Default per-instance concurrency when neither the instance nor `component.global` sets a cap.
const DEFAULT_PER_INSTANCE_CONCURRENCY: usize = 4;
/// Default reconciliation-rescan interval when `ingress.rescanSecs` is unset.
const DEFAULT_RESCAN_SECS: u64 = 30;

/// Current wall-clock time in Unix milliseconds (the durable store's time base).
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// A single watched-directory replication instance.
pub struct Instance {
    id: String,
    /// The persisted activation flag (runtime state wins over config `enabled`, DESIGN §7.5).
    active: AtomicBool,
    /// Config `enabled` — the value [`clear_activation`](Self::clear_activation) reverts to, and the
    /// `configuredEnabled` field reported by the P3 control plane's `get-status`.
    config_enabled: bool,
    /// Destination label(s) surfaced in `get-status` `inProgress` rows via
    /// [`crate::control::InstanceControl::dest_label`] — a single `dest.kind()` (e.g. `"local"`) for
    /// one destination, or every configured destination's label joined with `"+"` for a fan-out
    /// instance (e.g. `"local+s3"`, DESIGN §20-B).
    dest_kind: String,
    /// The configured schedule mode (`"immediate"`/`"cron"`/`"window"`) reported by `get-status`
    /// (DESIGN §16), driven live by [`schedule`](Self::schedule).
    schedule_mode: &'static str,
    /// The parsed schedule (DESIGN §12); [`Schedule::admission`] gates every tick's claim.
    schedule: Schedule,
    /// The cron watermark / window open-state [`schedule`](Self::schedule) evaluation needs across
    /// ticks (fire dedup, open/close transition detection for `WindowOpened`/`WindowClosed`). Guarded
    /// separately from `tick_lock` because it is read+written synchronously (no `.await` while held).
    sched_track: Mutex<ScheduleTrack>,
    /// Whether the destination reports [`crate::dest::Destination::supports_resume`] — decides whether
    /// a window close cancels an in-flight transfer (`pauseResume`) or always falls back to
    /// `finishCurrent` (DESIGN §12.4). Captured once at build time (destinations don't change kind).
    dest_supports_resume: bool,
    rescan: Duration,
    /// Serializes [`tick`](Self::tick) so the periodic run-loop tick and a control-triggered tick (both
    /// on the same `Arc<Instance>`, from different tasks) never overlap — the new-file detection
    /// (`store.get` then `enqueue`) is not atomic, so overlapping ticks could double-emit
    /// `FileReady`/`ScanComplete` and double-count `discovered` for one file.
    tick_lock: tokio::sync::Mutex<()>,
    /// Holds the readiness state (stability tracker), so discovery locks it briefly per tick. Behind
    /// an `Arc` so the blocking directory walk can run in `spawn_blocking` off the async runtime.
    watcher: Arc<Mutex<Watcher>>,
    queue: Queue,
    worker: Arc<Worker>,
    /// Held for the activation write-side ([`set_activation`](Self::set_activation) /
    /// [`clear_activation`](Self::clear_activation)) and the P3 `get-status` store queries.
    store: Arc<dyn StateStore>,
    /// UNS event emitter for this instance's discovery events (`FileReady`/`ScanComplete`) and
    /// lifecycle events, bound once to this instance's own `gg.instance(id).events()` facade (see
    /// `crate::app`/`crate::events` module docs). A no-op [`Events::disabled`] when messaging is
    /// absent (or in unit tests), so the P1/P2 engine runs unchanged. The worker holds a clone for the
    /// per-file lifecycle events.
    events: Events,
    /// Feature A dedup-log state for **ingress** permission/access errors on this instance's watched
    /// directory (`src/permission.rs`) — the "log once, not per rescan" gate for [`report_scan_issues`].
    /// This is a SEPARATE `PermissionLog` from the worker's egress one ([`Worker::perm_log`]): the two
    /// have different key spaces (ingress directory paths vs. destination labels) and lifecycles (this
    /// one evicts recovered paths every tick via [`PermissionLog::recovered`]; the egress one is bounded
    /// by the fixed destination count), so sharing a single log would let a per-tick ingress recovery
    /// pass wrongly evict egress entries and re-arm egress spam.
    perm_log: Arc<PermissionLog>,
}

/// Cross-tick schedule evaluation state (DESIGN §12), synchronous-only (no `.await` while a lock on
/// this is held — event emission happens after the lock is dropped).
#[derive(Default)]
struct ScheduleTrack {
    /// The cron watermark [`Schedule::admission`] uses to detect a *fresh* fire since the last call.
    last_fire: Option<DateTime<Utc>>,
    /// Whether the window was open as of the last evaluation (detects the open/close edges that emit
    /// `WindowOpened`/`WindowClosed`). Always `false` for immediate/cron schedules (never read there).
    window_open: bool,
}

/// What a tick's schedule evaluation implies should be announced this tick (computed once, emitted
/// after the [`ScheduleTrack`] lock is released).
#[derive(Default)]
struct ScheduleTransition {
    /// A cron fire happened since the last tick.
    triggered: bool,
    /// The window just opened (was closed last evaluation).
    window_opened: bool,
    /// The window closed **between ticks** (its batch, if any, fully drained before `close_at` — the
    /// common case). A close detected *mid*-batch is instead handled synchronously inside
    /// [`Instance::run_batch_windowed`], which flips [`ScheduleTrack::window_open`] itself so this
    /// flag is never also set on the following tick (no double `WindowClosed`).
    window_closed: bool,
}

/// Convert the engine's Unix-ms clock base to the schedule model's `DateTime<Utc>`. Falls back to the
/// Unix epoch on an out-of-range value (astronomically unlikely — `now` is always a real clock read).
fn ms_to_utc(now_ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(now_ms).single().unwrap_or_else(|| Utc.timestamp_opt(0, 0).unwrap())
}

impl Instance {
    /// Assemble an instance from its config plus the process-wide shared governors (the
    /// priority-aware global concurrency admission gate — Feature B, `src/admission.rs` — and the
    /// global bandwidth bucket) and the shared durable store.
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        cfg: InstanceCfg,
        global: &GlobalCfg,
        store: Arc<dyn StateStore>,
        global_gate: Arc<PriorityGate>,
        global_bw: Arc<TokenBucket>,
        metrics: Option<Arc<dyn MetricService>>,
        deps: &DestDeps,
        events: Events,
    ) -> anyhow::Result<Instance> {
        anyhow::ensure!(
            !cfg.egress.is_empty(),
            "instance '{}': at least one egress destination is required",
            cfg.id
        );
        // The backend may need the shared store (S3 resume checkpoints); always thread THIS instance's
        // store in (the credential handle rides in from `App::run`).
        let deps = deps.clone().with_store(store.clone());
        let dests: Vec<SharedDestination> = cfg
            .egress
            .iter()
            .map(|e| {
                build_destination(e, &deps).map_err(|err| anyhow::anyhow!("instance '{}': {err}", cfg.id))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Self::build_with_dests(cfg, global, store, global_gate, global_bw, metrics, dests, events)
    }

    /// Assemble an instance with a **caller-supplied** [`SharedDestination`], bypassing the egress
    /// factory. This is the injection seam used by the end-to-end integration tests (a fake/failing
    /// destination for the error paths) and by future programmatic wiring; the normal path is
    /// [`build`](Self::build), which resolves the destination(s) from `cfg.egress`. Unlike `build`, this
    /// does not require `cfg.egress` to be present — the destination is explicit. A thin wrapper over
    /// [`build_with_dests`](Self::build_with_dests) with a single-element destination list — by
    /// construction this is exactly the N=1 case of the fan-out engine (DESIGN §20-B).
    #[allow(clippy::too_many_arguments)]
    pub fn build_with_dest(
        cfg: InstanceCfg,
        global: &GlobalCfg,
        store: Arc<dyn StateStore>,
        global_gate: Arc<PriorityGate>,
        global_bw: Arc<TokenBucket>,
        metrics: Option<Arc<dyn MetricService>>,
        dest: SharedDestination,
        events: Events,
    ) -> anyhow::Result<Instance> {
        Self::build_with_dests(cfg, global, store, global_gate, global_bw, metrics, vec![dest], events)
    }

    /// Assemble an instance that fans a claimed item out to every destination in `dests` (P6, DESIGN
    /// §20-B): each retries independently, and the source is only released once ALL of them verify. The
    /// injection-seam counterpart of [`build_with_dest`](Self::build_with_dest) for N>1 destinations —
    /// used by [`build`](Self::build) (the normal, config-driven path) and directly by tests exercising
    /// fan-out with injected/fake destinations. `dests` must be non-empty.
    #[allow(clippy::too_many_arguments)]
    pub fn build_with_dests(
        cfg: InstanceCfg,
        global: &GlobalCfg,
        store: Arc<dyn StateStore>,
        global_gate: Arc<PriorityGate>,
        global_bw: Arc<TokenBucket>,
        metrics: Option<Arc<dyn MetricService>>,
        dests: Vec<SharedDestination>,
        events: Events,
    ) -> anyhow::Result<Instance> {
        anyhow::ensure!(
            !dests.is_empty(),
            "instance '{}': at least one destination is required",
            cfg.id
        );
        let schedule_mode = match cfg.schedule {
            ScheduleCfg::Immediate => "immediate",
            ScheduleCfg::Cron(_) => "cron",
            ScheduleCfg::Window(_) => "window",
        };
        let default_tz = global.defaults.as_ref().and_then(|d| d.timezone.as_deref());
        let schedule = Schedule::from_cfg(&cfg.schedule, default_tz)
            .map_err(|e| anyhow::anyhow!("instance '{}': invalid schedule: {e}", cfg.id))?;

        // Prune any in-tree archive/failed directory from discovery so completed/quarantined files are
        // never re-discovered and re-replicated.
        let mut skip: Vec<PathBuf> = Vec::new();
        if let Some(d) = &cfg.completion.archive_dir {
            skip.push(d.clone());
        }
        if let Some(d) = &cfg.completion.failed_dir {
            skip.push(d.clone());
        }
        // FR-ING-8 loop prevention: a LOCAL egress whose directory lives under (or at) the watch root
        // would otherwise have the replicator re-discover its own output and replicate it again —
        // `/data` → `/data/out/a.txt` → `/data/out/out/a.txt` → … (and with onSuccess=delete the
        // source is removed each pass, so the delivered copy is rediscovered and re-nested forever).
        // Prune the destination directory from discovery. (S3/other egress kinds are off-filesystem.)
        for e in &cfg.egress {
            if let EgressCfg::Local(l) = e {
                if l.path.starts_with(&cfg.ingress.path) {
                    tracing::debug!(
                        instance = %cfg.id, dest = %l.path.display(),
                        "local egress is under the watch root; pruning it from discovery"
                    );
                    skip.push(l.path.clone());
                }
            }
        }

        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let watcher = Watcher::from_cfg(&cfg.ingress, clock, skip)
            .map_err(|e| anyhow::anyhow!("instance '{}': invalid ingress glob: {e}", cfg.id))?;

        let per_limit = cfg
            .limits
            .as_ref()
            .and_then(|l| l.max_concurrent_files)
            .or_else(|| global.limits.as_ref().and_then(|l| l.max_concurrent_files))
            .unwrap_or(DEFAULT_PER_INSTANCE_CONCURRENCY)
            .max(1);

        // Per-instance bandwidth bucket (0 = unlimited); every transfer also passes the global bucket.
        let per_rate = cfg
            .limits
            .as_ref()
            .and_then(|l| l.max_bandwidth.as_deref())
            .map(parse_byte_rate)
            .transpose()
            .map_err(|e| anyhow::anyhow!("instance '{}': maxBandwidth: {e}", cfg.id))?
            .unwrap_or(0);
        let per_bucket = Arc::new(TokenBucket::new(per_rate, Arc::new(SystemClock)));
        let bw = Bandwidth::new(per_bucket, global_bw);

        let global_retry = global.defaults.as_ref().and_then(|d| d.retry.as_ref());
        let retry = RetryPolicy::resolve(cfg.retry.as_ref(), global_retry);

        // Multi-destination fan-out (DESIGN §20-B): each destination retries independently, so the
        // window-close `pauseResume` behavior is only safe when EVERY destination supports resume
        // (cancelling a destination that can't resume would restart it from byte 0 next window — the
        // documented `finishCurrent` fallback handles that single-destination case already, and the
        // same fallback now covers "at least one destination can't resume"). `dest_kind` is every
        // configured destination's label, joined (`"local"` for N=1, `"local+s3"` for N=2, …) — the
        // short label surfaced in `get-status`.
        let dest_kind = dests
            .iter()
            .map(|d| d.kind())
            .collect::<Vec<_>>()
            .join("+");
        let dest_supports_resume = dests.iter().all(|d| d.supports_resume());
        // Feature B (`src/admission.rs`): this instance's cross-instance GLOBAL admission priority
        // (lower = higher priority) rides in with it so `Queue::acquire_slot` can hand it to
        // `PriorityGate::acquire` — the per-instance semaphore below is unaffected by priority.
        let queue = Queue::new(cfg.id.clone(), store.clone(), per_limit, global_gate, cfg.priority);
        // Feature A (`src/permission.rs`): two dedup-logs, not one — the ingress one has a natural
        // "still erroring" set every tick (this scan's issues) so it evicts recovered paths and stays
        // tightly bounded (keyed by ingress directory path). The worker's egress errors have no
        // equivalent whole-set signal per attempt, so it gets its OWN log, keyed by destination LABEL —
        // a broken-permission destination is logged/emitted once for the destination (not once per
        // file), which bounds that map to the small, fixed number of configured destinations (no
        // per-relpath growth) and is dropped entirely at instance stop when the `Arc` is dropped. The
        // two must stay separate: sharing one would let the ingress per-tick `recovered()` pass (whose
        // still-erroring set is ingress paths only) evict every egress entry each tick and re-arm the
        // egress spam this feature exists to prevent.
        let perm_log = Arc::new(PermissionLog::new());
        let worker_perm_log = Arc::new(PermissionLog::new());
        let worker = Arc::new(
            Worker::new_multi(
                cfg.id.clone(),
                store.clone(),
                dests,
                cfg.completion.clone(),
                cfg.ingress.path.clone(),
                bw,
                retry,
                metrics,
            )
            .with_events(events.clone())
            .with_permission_log(worker_perm_log),
        );

        // Activation precedence (DESIGN §7.5): persisted runtime state ▸ config `enabled` ▸ default.
        let effective = match store.load_activation(&cfg.id)? {
            Some(a) => a.active,
            None => cfg.enabled,
        };

        let rescan =
            Duration::from_secs(cfg.ingress.rescan_secs.unwrap_or(DEFAULT_RESCAN_SECS).max(1));

        Ok(Instance {
            id: cfg.id,
            active: AtomicBool::new(effective),
            config_enabled: cfg.enabled,
            dest_kind,
            schedule_mode,
            schedule,
            sched_track: Mutex::new(ScheduleTrack::default()),
            dest_supports_resume,
            rescan,
            tick_lock: tokio::sync::Mutex::new(()),
            watcher: Arc::new(Mutex::new(watcher)),
            queue,
            worker,
            store,
            events,
            perm_log,
        })
    }

    /// Whether the instance is currently active (its effective activation state).
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    /// Set (and persist) the activation override so it survives restarts and config reloads
    /// (DESIGN §7.5). `source` records who set it (e.g. `"control"`). Driven live by the P3
    /// `set-activation` control message via [`crate::control::InstanceControl::apply_activation`].
    pub fn set_activation(&self, active: bool, source: &str, now: i64) -> Result<()> {
        self.store.set_activation(&self.id, active, source, now)?;
        self.active.store(active, Ordering::Relaxed);
        Ok(())
    }

    /// Clear the persisted activation override, reverting to config `enabled` (DESIGN §7.5 reset path).
    /// Driven by the P3 `set-activation { reset: true }` control message.
    pub fn clear_activation(&self, _now: i64) -> Result<()> {
        self.store.clear_activation(&self.id)?;
        self.active.store(self.config_enabled, Ordering::Relaxed);
        Ok(())
    }

    /// Feature A: seed the ingress dedup-log with startup permission [`Violation`](crate::permission::Violation)s
    /// so a `retain` instance — one started *despite* a startup violation — does NOT re-emit a duplicate
    /// `PermissionDenied` on its first rescan. The startup path ([`crate::app`]) already emitted exactly
    /// one event per violation (that is the "once"); marking each ingress violation as already-logged at
    /// `now` keeps the first tick, still inside the quiet window, silent for the same (path, kind) — so
    /// the "emitted once per distinct (path, error kind)" guarantee holds across the startup→runtime
    /// boundary. Only ingress-role violations matter: the runtime per-rescan re-emit path is
    /// [`report_scan_issues`](Self::report_scan_issues) (ingress only); egress/archive/failed roles have
    /// no per-rescan runtime re-emit to suppress. If the ingress path has actually recovered by the first
    /// tick, [`PermissionLog::recovered`] evicts the seed and logs the usual INFO "access restored".
    pub fn seed_permission_log(&self, violations: &[crate::permission::Violation], now: i64) {
        for v in violations {
            if v.role == Role::Ingress {
                let _ = self.perm_log.should_log(&v.path.display().to_string(), v.kind, now);
            }
        }
    }

    /// One engine iteration (the testable unit): discover ready files → durably enqueue → claim a
    /// bounded batch → process it concurrently (bounded by the per-instance ∩ global slots), gated by
    /// the schedule (DESIGN §12). A deactivated instance does nothing. `now` is the single Unix-ms
    /// clock read for this tick.
    pub async fn tick(&self, now: i64) {
        self.run_tick(now, false).await;
    }

    /// The tick body. `force = true` is a control-plane `trigger { ignoreWindow: true }` (FR-CTL-3):
    /// it still discovers/enqueues and promotes retries as usual, but then **bypasses the schedule
    /// gate** and drains ALL ready work now regardless of cron/window state — an explicit operator
    /// override. A forced tick does not disturb the cron watermark or window open-state and emits no
    /// gate-transition events (the control plane emits its own `ScheduleTriggered`), so it never
    /// perturbs the automatic schedule.
    async fn run_tick(&self, now: i64, force: bool) {
        if !self.is_active() {
            return;
        }

        // Serialize ticks per instance: [`run`](Self::run) ticks on the periodic/OS-watch schedule
        // while a control `trigger` ticks the same `Arc<Instance>` from the dispatcher's task. The
        // new-file detection below (`store.get` then `enqueue`) is not atomic, so without this guard
        // two overlapping ticks could each see a brand-new file as new and both emit `FileReady` /
        // `ScanComplete` and double-count `discovered` (durable state stays correct — `claim_ready` is
        // transactional — so this guard is purely event-stream/stat accuracy). Held for the whole tick.
        let _tick = self.tick_lock.lock().await;

        // Discovery walk (blocking `read_dir` + per-file `metadata`, potentially over a 100k-file
        // spool) runs on the blocking pool, not a shared async worker thread (DESIGN §6.3 / the
        // state.rs contract that bulk scans go through spawn_blocking). It mutates the readiness
        // tracker behind the shared `Arc<Mutex<Watcher>>`, so state persists across ticks.
        let watcher = self.watcher.clone();
        let (ready, issues) = match tokio::task::spawn_blocking(move || {
            let mut w = watcher.lock().expect("watcher mutex");
            w.discover(&RealProbe)
        })
        .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(instance = %self.id, error = %e, "discovery task failed");
                (Vec::new(), Vec::new())
            }
        };
        // Feature A: dedup-log + emit `PermissionDenied` for any directory the scan couldn't read,
        // instead of the old silent per-rescan skip (`src/permission.rs`).
        self.report_scan_issues(&issues, now).await;
        let mut discovered: u64 = 0;
        for c in ready {
            // A brand-new relpath (no prior durable row) is a genuine `FileReady` transition; a row
            // that already exists (still Ready/InProgress/terminal) is a re-scan of a known file, so
            // it is not re-announced (avoids per-rescan event spam for a backlog or a retained file).
            let is_new = matches!(self.store.get(&self.id, &c.relpath), Ok(None));
            if let Err(e) = self.queue.enqueue(&c.relpath, c.size, c.mtime_ms, now) {
                tracing::error!(instance = %self.id, relpath = %c.relpath, error = %e, "enqueue failed");
                continue;
            }
            if is_new {
                discovered += 1;
                self.events
                    .emit(Event::FileReady {
                        path: c.relpath.clone(),
                        size: c.size,
                    })
                    .await;
            }
        }
        // Announce the scan only when it actually enqueued something new (a heartbeat every rescan
        // would be noise on the evt stream); `awaiting` is the current ready backlog.
        if discovered > 0 {
            let awaiting = self
                .store
                .list_by_state(&self.id, ItemState::Ready)
                .map(|v| v.len() as u64)
                .unwrap_or(0);
            self.events.emit(Event::ScanComplete { discovered, awaiting }).await;
        }

        // Retry manager: promote any Failed items whose backoff gate has elapsed back to Ready. This
        // scans every Failed row, so it too runs off the async runtime (a clone of the queue shares
        // the same store + semaphores).
        let queue = self.queue.clone();
        match tokio::task::spawn_blocking(move || queue.promote_due(now)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => tracing::error!(instance = %self.id, error = %e, "retry promotion failed"),
            Err(e) => tracing::error!(instance = %self.id, error = %e, "retry promotion task failed"),
        }

        // Scheduling gate (DESIGN §12): decide whether/how ready work may be claimed this tick.
        // Discovery/enqueue above always ran regardless — only the CLAIM is gated, so newly-ready work
        // simply accumulates in the durable `Ready` backlog while the gate is closed. A forced trigger
        // (FR-CTL-3 `ignoreWindow`) bypasses the gate entirely and drains everything now.
        let (admission, transition) = if force {
            (Admission::All { drain: true }, ScheduleTransition::default())
        } else {
            self.evaluate_schedule(ms_to_utc(now))
        };
        if transition.triggered {
            self.events.emit(Event::ScheduleTriggered { scope: self.id.clone() }).await;
        }
        if transition.window_opened {
            self.events.emit(Event::WindowOpened { window: self.window_label() }).await;
        }
        if transition.window_closed {
            // The common case: the previous windowed batch (if any) fully drained before `close_at`,
            // so the close is only discovered on this later tick — no transfer to pause here (a close
            // caught mid-transfer is instead handled synchronously in `run_batch_windowed`).
            self.events.emit(Event::WindowClosed { window: self.window_label() }).await;
            self.events.emit(Event::ScheduleComplete { mode: "window".to_string() }).await;
        }

        // The return value used to also gate a retained-state republish (dropped — see events.rs's
        // module docs: `state` is now a reserved, library-owned UNS class); the calls themselves still
        // run for their side effects (persisting/delivering the admitted batch).
        match admission {
            Admission::All { drain } => {
                self.run_admitted(drain, now).await;
            }
            Admission::None => {}
            Admission::Windowed { close_at, on_close } => {
                self.run_windowed(close_at, on_close, now).await;
            }
        }
        if transition.triggered {
            self.events.emit(Event::ScheduleComplete { mode: "cron".to_string() }).await;
        }
    }

    /// Feature A (`src/permission.rs`): turn this tick's ingress [`ScanIssue`]s into the ALWAYS
    /// log-once + `PermissionDenied` event contract, instead of the watcher's old silent per-rescan
    /// `debug!`. Every issue is deduplicated per (instance, path, error-kind) via the shared
    /// [`PermissionLog`] — a persistently-inaccessible ingress dir is logged/emitted once, not on
    /// every reconciliation rescan. Paths that recovered since the last tick (no longer erroring) are
    /// evicted from the dedup state and get a one-line INFO recovery log (also a state change worth
    /// re-arming logging for, per the "log once, re-log on state change" contract).
    async fn report_scan_issues(&self, issues: &[ScanIssue], now: i64) {
        let still_erroring: HashSet<String> = issues
            .iter()
            .map(|i| i.path.display().to_string())
            .collect();
        for issue in issues {
            let path = issue.path.display().to_string();
            if !self.perm_log.should_log(&path, issue.kind, now) {
                continue;
            }
            let msg = format!("{:?}", issue.kind);
            if issue.kind == std::io::ErrorKind::PermissionDenied {
                tracing::warn!(
                    instance = %self.id, path = %path, kind = ?issue.kind,
                    "ingress directory permission denied; will not spam this every rescan"
                );
                self.events
                    .emit(Event::PermissionDenied {
                        path: path.clone(),
                        role: Role::Ingress.as_str().to_string(),
                        error: msg,
                    })
                    .await;
            } else {
                // Other unreadable-directory kinds (e.g. NotFound — a watched subdir was removed) still
                // get the same dedup treatment so they don't spam either, but are not a *permission*
                // event.
                tracing::debug!(
                    instance = %self.id, path = %path, kind = ?issue.kind,
                    "ingress directory unreadable; will not spam this every rescan"
                );
            }
        }
        for recovered in self.perm_log.recovered(&still_erroring) {
            tracing::info!(instance = %self.id, path = %recovered, "ingress directory access restored");
        }
    }

    /// Evaluate the schedule at `now_utc` against the persisted [`ScheduleTrack`], returning the raw
    /// [`Admission`] plus which lifecycle events this tick should announce. Synchronous (no `.await`
    /// while the track lock is held) — the caller emits events after this returns.
    fn evaluate_schedule(&self, now_utc: DateTime<Utc>) -> (Admission, ScheduleTransition) {
        let mut track = self.sched_track.lock().expect("schedule track mutex");
        let admission = self.schedule.admission(now_utc, &mut track.last_fire);
        let mut transition = ScheduleTransition::default();
        match admission {
            Admission::All { drain: true } => transition.triggered = true,
            Admission::Windowed { .. } => {
                if !track.window_open {
                    transition.window_opened = true;
                }
                track.window_open = true;
            }
            Admission::All { drain: false } | Admission::None => {
                if track.window_open {
                    transition.window_closed = true;
                }
                track.window_open = false;
            }
        }
        (admission, transition)
    }

    /// The schedule's human-readable label for `WindowOpened`/`WindowClosed` event bodies (empty for a
    /// non-window schedule, which never reaches the call sites that use this).
    fn window_label(&self) -> String {
        match &self.schedule {
            Schedule::Window(w) => w.label().to_string(),
            Schedule::Immediate | Schedule::Cron(_) => String::new(),
        }
    }

    /// `immediate`/`cron` admission: claim + process. `drain = false` (immediate) claims one bounded
    /// batch, matching the unchanged P1 behavior; `drain = true` (a fresh cron fire) repeats the
    /// claim → process cycle until the ready backlog is exhausted (DESIGN §12.2: "release all ready
    /// work at each fire"). Returns whether anything was claimed.
    async fn run_admitted(&self, drain: bool, now: i64) -> bool {
        let mut processed = false;
        loop {
            let batch = match self.queue.claim(self.queue.per_limit(), now) {
                Ok(b) => b,
                Err(e) => {
                    tracing::error!(instance = %self.id, error = %e, "claim failed");
                    return processed;
                }
            };
            if batch.is_empty() {
                break;
            }
            processed = true;
            self.run_batch(batch, now).await;
            if !drain {
                break;
            }
        }
        processed
    }

    /// Claim + process one bounded batch, with no window-close preemption (immediate/cron path).
    async fn run_batch(&self, batch: Vec<WorkItem>, now: i64) {
        let mut set: JoinSet<()> = JoinSet::new();
        for item in batch {
            let slot = self.queue.acquire_slot().await; // backpressure on the global cap
            let worker = self.worker.clone();
            set.spawn(async move {
                let _slot = slot; // released when the transfer finishes
                worker.process_item(&item, now).await;
            });
        }
        while set.join_next().await.is_some() {}
    }

    /// `window` admission: **drain** the ready backlog while the window is open (continuous flow —
    /// DESIGN §12.2), claiming successive bounded batches until either the `Ready` backlog is empty or
    /// the window's `close_at` instant arrives. Each batch races the *same* shared close deadline (so
    /// the window conserves its close instant no matter how many batches it takes to drain), and a
    /// transfer still running when the window closes obeys `onWindowClose` (DESIGN §12.4). Returns
    /// whether anything was claimed.
    ///
    /// Unlike a cron fire (an instantaneous point trigger), an open window must keep more work moving
    /// than one `per_limit` batch per reconciliation-rescan tick — a nightly window over a large spool
    /// has to actually drain within its span, not trickle `per_limit` files every `rescan` seconds.
    async fn run_windowed(&self, close_at: DateTime<Utc>, on_close: WindowClose, now: i64) -> bool {
        // One absolute deadline for the whole open span, shared across every drained batch.
        let dur = (close_at - ms_to_utc(now)).to_std().unwrap_or(Duration::ZERO);
        let deadline = tokio::time::Instant::now() + dur;
        let mut processed = false;
        loop {
            // Stop admitting new batches once the window has actually closed between batches (real time
            // reached the deadline while draining). This clean between-batches close is announced by the
            // NEXT tick's `evaluate_schedule` (`window_open` is still `true`), so nothing to emit here —
            // only a close caught *mid*-batch (below) emits `WindowClosed`/`ScheduleComplete` in-tick.
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            let batch = match self.queue.claim(self.queue.per_limit(), now) {
                Ok(b) => b,
                Err(e) => {
                    tracing::error!(instance = %self.id, error = %e, "claim failed");
                    return processed;
                }
            };
            if batch.is_empty() {
                break;
            }
            processed = true;
            // If the close is caught mid-batch, `run_batch_windowed` handles `onWindowClose`, emits the
            // close events once, and returns `true` — stop draining then (admit no new work post-close).
            if self.run_batch_windowed(batch, now, deadline, on_close).await {
                break;
            }
        }
        processed
    }

    /// Drive one claimed batch to completion, racing the window's shared `deadline`. If the batch drains
    /// first, this is exactly [`run_batch`](Self::run_batch) and returns `false`. If `deadline` arrives
    /// first (a transfer is still in flight), returns `true` after handling the close:
    /// - emits `WindowClosed`;
    /// - `onWindowClose = pauseResume` **and** the destination supports resume: cancels every
    ///   still-running task ([`JoinSet::abort_all`] — cooperative; dropping a future mid-poll is safe
    ///   here because the write-ahead architecture never depends on explicit teardown), then re-arms the
    ///   batch via [`pause_revert`](Self::pause_revert) (still-`InProgress` items → `Ready` with their
    ///   resume checkpoint preserved; an item that raced into `Verified` is driven to completion, not
    ///   stranded) so the next window resumes it (DESIGN §12.4);
    /// - otherwise (`finishCurrent`, or `pauseResume` without resume support — the documented fallback):
    ///   lets every in-flight item finish normally, admitting nothing new (the caller stops draining).
    ///
    /// The mid-batch close emits `WindowClosed` + `ScheduleComplete` once here; the between-ticks /
    /// between-batches close (the common case) gets its own pair from [`tick`](Self::tick)'s
    /// `transition.window_closed`, so this function must not also emit them when the deadline never fires
    /// (avoids a double announcement of the same close). It also flips [`ScheduleTrack::window_open`] to
    /// `false` synchronously so the next tick does not re-announce the same close.
    async fn run_batch_windowed(
        &self,
        batch: Vec<WorkItem>,
        now: i64,
        deadline: tokio::time::Instant,
        on_close: WindowClose,
    ) -> bool {
        let relpaths: Vec<String> = batch.iter().map(|i| i.relpath.clone()).collect();
        let mut set: JoinSet<()> = JoinSet::new();
        for item in batch {
            let slot = self.queue.acquire_slot().await;
            let worker = self.worker.clone();
            set.spawn(async move {
                let _slot = slot;
                worker.process_item(&item, now).await;
            });
        }

        let sleep = tokio::time::sleep_until(deadline);
        tokio::pin!(sleep);
        let mut closed = false;
        loop {
            tokio::select! {
                res = set.join_next() => {
                    match res {
                        Some(_) => continue,
                        None => break,
                    }
                }
                _ = &mut sleep, if !closed => {
                    closed = true;
                    // Flip the shared track's `window_open` NOW (synchronously, in-tick) so the next
                    // tick's `evaluate_schedule` sees it already closed and does not also fire
                    // `transition.window_closed` for this same close.
                    self.sched_track.lock().expect("schedule track mutex").window_open = false;
                    self.events.emit(Event::WindowClosed { window: self.window_label() }).await;
                    let cancel = matches!(on_close, WindowClose::PauseResume) && self.dest_supports_resume;
                    if cancel {
                        set.abort_all();
                        while set.join_next().await.is_some() {}
                        self.pause_revert(&relpaths, now).await;
                        break;
                    }
                    // finishCurrent (or the no-resume-support fallback): keep looping to let the
                    // still-running tasks finish naturally; no more claims happen after this tick.
                }
            }
        }
        if closed {
            self.events.emit(Event::ScheduleComplete { mode: "window".to_string() }).await;
        }
        closed
    }

    /// Re-arm a `pauseResume`-aborted batch for the next window (DESIGN §12.4). An item still
    /// `InProgress` reverts to `Ready` (its resume checkpoint is preserved — never cleared except at
    /// terminal give-up — so the next claim resumes rather than restarts it). An item that raced into
    /// `Verified` (delivery + verify already succeeded, aborted in the narrow `Verified → Completed`
    /// gap) is driven to completion via [`Worker::recover_verified`], not left dangling non-terminal
    /// until the next process restart — otherwise its `replicated` stat and `ReplicationCompleted`/
    /// completion events would be lost and `get-status` would misreport it (a detached `spawn_blocking`
    /// may already have run the source delete/archive).
    async fn pause_revert(&self, relpaths: &[String], now: i64) {
        for rp in relpaths {
            match self.store.get(&self.id, rp) {
                Ok(Some(row)) => match row.state {
                    ItemState::InProgress => {
                        if let Err(e) = self.store.set_state(&self.id, rp, ItemState::Ready, now) {
                            tracing::error!(
                                instance = %self.id, relpath = %rp, error = %e,
                                "reverting paused item to Ready failed"
                            );
                        }
                    }
                    ItemState::Verified => {
                        if let Err(e) = self.worker.recover_verified(&row, now).await {
                            tracing::error!(
                                instance = %self.id, relpath = %rp, error = %e,
                                "completing verified item after window-close pause failed"
                            );
                        }
                    }
                    _ => {}
                },
                Ok(None) => {}
                Err(e) => tracing::error!(
                    instance = %self.id, relpath = %rp, error = %e,
                    "loading paused item for revert failed"
                ),
            }
        }
    }

    /// Run until `cancel` fires: recover durable state, attach the OS file watcher, then tick on every
    /// reconciliation-rescan interval and every OS-watch nudge. Durable state is written ahead of every
    /// side effect, so cancellation needs no explicit checkpoint — a clean stop and a crash recover the
    /// same way (DESIGN §13.2).
    pub async fn run(&self, cancel: CancellationToken) {
        if let Err(e) = self.worker.recover(now_ms()).await {
            tracing::error!(instance = %self.id, error = %e, "crash recovery failed");
        }

        // OS file watch nudges a tick; the periodic rescan is the belt-and-suspenders fallback, so a
        // missed/failed watch only delays discovery to the next rescan, never loses it.
        let (nudge_tx, mut nudge_rx) = tokio::sync::mpsc::channel::<()>(8);
        let _os_watch = self.spawn_os_watch(nudge_tx);

        let mut ticker = tokio::time::interval(self.rescan);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        self.tick(now_ms()).await; // initial sweep

        loop {
            // If any file is mid-stability-window, self-schedule a near-term re-observation rather than
            // waiting out the full reconciliation rescan. A finished file emits NO further OS-watch
            // nudge, so its stability-confirming (2nd) observation would otherwise only land on the next
            // periodic rescan — collapsing discovery latency to `rescanSecs` and rendering the OS watch
            // useless for the default `stability` strategy. Clamped ≥100ms so a file right at its
            // boundary can't spin the loop, and ≤`rescan` so it never delays past the fallback.
            let recheck = self
                .watcher
                .lock()
                .expect("watcher mutex")
                .pending_recheck()
                .map(|d| d.clamp(Duration::from_millis(100), self.rescan));
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = ticker.tick() => self.tick(now_ms()).await,
                Some(_) = nudge_rx.recv() => self.tick(now_ms()).await,
                // A cron/window schedule needs a prompt wake at its next edge (a fire, or a window
                // open/close) rather than waiting out the full reconciliation-rescan interval — e.g. a
                // window closing mid-rescan-period should be noticed close to on time, not up to
                // `rescan` seconds late. Immediate mode's `next_wake` is always `None`, so this branch
                // degrades to the harmless 24h fallback and never fires meaningfully.
                _ = tokio::time::sleep(self.sched_wake_delay()) => self.tick(now_ms()).await,
                // The stability re-check (above): only armed while a file is mid-window.
                _ = tokio::time::sleep(recheck.unwrap_or(self.rescan)), if recheck.is_some() => self.tick(now_ms()).await,
            }
        }
        tracing::info!(instance = %self.id, "instance stopped (durable state checkpointed)");
    }

    /// Real-time delay until the schedule's next edge ([`Schedule::next_wake`]), for
    /// [`run`](Self::run)'s wake selection — capped at 24h so an immediate schedule (`next_wake` always
    /// `None`) still yields a bounded, harmless fallback rather than an unrepresentable "forever".
    fn sched_wake_delay(&self) -> Duration {
        const FALLBACK: Duration = Duration::from_secs(24 * 60 * 60);
        match self.schedule.next_wake(Utc::now()) {
            Some(at) => (at - Utc::now())
                .to_std()
                .unwrap_or(Duration::from_millis(1))
                .clamp(Duration::from_millis(1), FALLBACK),
            None => FALLBACK,
        }
    }

    /// Attach an OS file watcher that nudges a tick on any change under the source root. Best-effort:
    /// on failure the caller falls back to the periodic rescan only. The returned watcher must be kept
    /// alive for the duration of the run (dropping it stops watching).
    fn spawn_os_watch(
        &self,
        nudge_tx: tokio::sync::mpsc::Sender<()>,
    ) -> Option<notify::RecommendedWatcher> {
        use notify::{RecursiveMode, Watcher as _};

        let (root, recursive) = {
            let w = self.watcher.lock().expect("watcher mutex");
            (w.root().to_path_buf(), w.recursive())
        };
        let mode = if recursive {
            RecursiveMode::Recursive
        } else {
            RecursiveMode::NonRecursive
        };
        let watch_id = self.id.clone();
        let mut watcher = match notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
            match res {
                // Any change nudges a tick; coalesced (a full channel already has a pending nudge, so
                // a dropped send is harmless — the tick re-scans everything).
                Ok(_) => {
                    let _ = nudge_tx.try_send(());
                }
                // Surface watch errors (e.g. inotify queue overflow) instead of silently dropping them:
                // the periodic rescan still guarantees correctness, but an operator should see that the
                // low-latency watch has degraded.
                Err(e) => {
                    tracing::warn!(instance = %watch_id, error = %e, "OS file watch event error")
                }
            }
        }) {
            Ok(w) => w,
            Err(e) => {
                tracing::warn!(instance = %self.id, error = %e, "OS file watch unavailable; using periodic rescan only");
                return None;
            }
        };
        if let Err(e) = watcher.watch(&root, mode) {
            tracing::warn!(instance = %self.id, error = %e, "OS file watch failed; using periodic rescan only");
            return None;
        }
        tracing::info!(instance = %self.id, path = %root.display(), "OS file watch active");
        Some(watcher)
    }
}

/// The P3 control-plane seam (DESIGN §16): the dispatcher drives each instance's activation,
/// trigger, and status-facing metadata through this trait so it never depends on the engine
/// internals. See [`crate::control::ControlPlane`].
#[async_trait::async_trait]
impl InstanceControl for Instance {
    fn id(&self) -> &str {
        &self.id
    }

    fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    fn configured_enabled(&self) -> bool {
        self.config_enabled
    }

    fn dest_label(&self) -> &str {
        &self.dest_kind
    }

    fn schedule_mode(&self) -> &str {
        self.schedule_mode
    }

    async fn trigger_scan(&self, now: i64, ignore_window: bool) {
        // Force one full reconciliation pass now (discover → enqueue → claim → process). With
        // `ignore_window` the tick bypasses the schedule gate so a cron/window instance replicates
        // now regardless of its schedule (FR-CTL-3); otherwise it is exactly the periodic-rescan tick
        // (gate-respecting). A deactivated instance's tick is a no-op.
        self.run_tick(now, ignore_window).await;
    }

    fn apply_activation(
        &self,
        active: bool,
        persist: bool,
        source: &str,
        now: i64,
    ) -> anyhow::Result<bool> {
        if persist {
            // Persisted override (survives restart); also flips the runtime flag.
            self.set_activation(active, source, now)?;
        } else {
            // Runtime-only flip (FR-STATE-3 non-persistent path): the persisted state is untouched,
            // so a restart reverts to it (or config `enabled`).
            self.active.store(active, Ordering::Relaxed);
        }
        Ok(active)
    }

    fn reset_activation(&self, now: i64) -> anyhow::Result<bool> {
        self.clear_activation(now)?;
        Ok(self.is_active())
    }

    async fn notify(&self, ev: Event) {
        self.events.emit(ev).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CompletionCfg, EgressCfg, GlobReadiness, IngressCfg, LocalEgress, OnSuccess, ReadinessCfg,
        ScheduleCfg,
    };
    use crate::domain::ItemState;
    use crate::state::{SqliteStore, StateStore};
    use std::path::Path;

    fn glob_ingress(root: &Path) -> IngressCfg {
        IngressCfg {
            path: root.to_path_buf(),
            recursive: true,
            include: vec![],
            exclude: vec![],
            rescan_secs: Some(1),
            // ready-immediately so a single tick discovers + processes (no timer dependency).
            readiness: ReadinessCfg::Glob(GlobReadiness { ready: vec![] }),
        }
    }

    fn instance_cfg(id: &str, src: &Path, dst: &Path, enabled: bool) -> InstanceCfg {
        InstanceCfg {
            id: id.to_string(),
            enabled,
            ingress: glob_ingress(src),
            egress: vec![EgressCfg::Local(LocalEgress {
                path: dst.to_path_buf(),
                fsync: false,
            })],
            schedule: ScheduleCfg::Immediate,
            completion: CompletionCfg {
                on_success: OnSuccess::Delete,
                ..Default::default()
            },
            retry: None,
            limits: None,
            on_permission_error: None,
            priority: 100,
        }
    }

    fn build(cfg: InstanceCfg, store: Arc<dyn StateStore>) -> Instance {
        Instance::build(
            cfg,
            &GlobalCfg::default(),
            store,
            Arc::new(PriorityGate::new(8)),
            Arc::new(TokenBucket::unlimited()),
            None,
            &DestDeps::default(),
            Events::disabled(),
        )
        .expect("instance builds")
    }

    #[tokio::test]
    async fn tick_discovers_enqueues_and_replicates() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("a.txt"), b"hello").unwrap();
        std::fs::create_dir_all(src.path().join("sub")).unwrap();
        std::fs::write(src.path().join("sub/b.txt"), b"world!").unwrap();

        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let inst = build(instance_cfg("i1", src.path(), dst.path(), true), store.clone());

        inst.tick(100).await;

        assert_eq!(std::fs::read(dst.path().join("a.txt")).unwrap(), b"hello");
        assert_eq!(std::fs::read(dst.path().join("sub/b.txt")).unwrap(), b"world!");
        assert!(!src.path().join("a.txt").exists(), "source deleted on success");
        assert_eq!(store.get("i1", "a.txt").unwrap().unwrap().state, ItemState::Completed);
        assert_eq!(store.get("i1", "sub/b.txt").unwrap().unwrap().state, ItemState::Completed);
        assert_eq!(store.stats("i1").unwrap().replicated, 2);
    }

    #[tokio::test]
    async fn tick_promotes_failed_items_past_their_backoff_gate() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("retry.txt"), b"later").unwrap();
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        // Pre-seed a Failed item with a backoff gate at t=500 (a prior attempt).
        store.upsert_ready("i5", "retry.txt", 5, 0, 1).unwrap();
        store
            .record_attempt("i5", "retry.txt", "earlier boom", ItemState::Failed, 500, 10)
            .unwrap();
        let inst = build(instance_cfg("i5", src.path(), dst.path(), true), store.clone());

        // A tick before the gate does not promote/process it.
        inst.tick(100).await;
        assert_eq!(store.get("i5", "retry.txt").unwrap().unwrap().state, ItemState::Failed);
        assert!(!dst.path().join("retry.txt").exists());

        // A tick at/after the gate promotes Failed → Ready, claims, and completes it.
        inst.tick(500).await;
        assert_eq!(store.get("i5", "retry.txt").unwrap().unwrap().state, ItemState::Completed);
        assert_eq!(std::fs::read(dst.path().join("retry.txt")).unwrap(), b"later");
    }

    #[tokio::test]
    async fn deactivated_instance_does_nothing() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("a.txt"), b"x").unwrap();
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let inst = build(instance_cfg("i2", src.path(), dst.path(), false), store.clone());

        assert!(!inst.is_active());
        inst.tick(100).await;
        assert!(src.path().join("a.txt").exists(), "nothing replicated while inactive");
        assert!(store.get("i2", "a.txt").unwrap().is_none());

        // Activate → the next tick processes it.
        inst.set_activation(true, "control", 200).unwrap();
        inst.tick(300).await;
        assert!(dst.path().join("a.txt").exists());
    }

    #[tokio::test]
    async fn persisted_activation_wins_over_config_enabled() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());

        // Operator deactivates a config-enabled instance; the override persists.
        store.set_activation("i3", false, "control", 1).unwrap();
        let inst = build(instance_cfg("i3", src.path(), dst.path(), true), store.clone());
        assert!(!inst.is_active(), "persisted deactivate wins over config enabled=true");

        // Reset reverts to config `enabled` (true here).
        inst.clear_activation(now_ms()).unwrap();
        assert!(inst.is_active());
        assert!(store.load_activation("i3").unwrap().is_none());
    }

    #[tokio::test]
    async fn run_replicates_then_stops_on_cancel() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("live.txt"), b"streamed").unwrap();
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let inst = Arc::new(build(instance_cfg("i4", src.path(), dst.path(), true), store.clone()));

        let cancel = CancellationToken::new();
        let c = cancel.clone();
        let handle = {
            let inst = inst.clone();
            tokio::spawn(async move { inst.run(c).await })
        };

        // The initial sweep in run() replicates the file; poll until it lands, then cancel.
        let out = dst.path().join("live.txt");
        for _ in 0..50 {
            if out.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(out.exists(), "run() initial sweep should replicate");
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("run loop exits promptly on cancel")
            .unwrap();
    }

    #[tokio::test]
    async fn local_egress_under_watch_root_does_not_loop() {
        // FR-ING-8: a destination nested under the source must be pruned from discovery, or the
        // replicator would re-ingest its own output forever (out/ → out/out/ → …).
        let root = tempfile::tempdir().unwrap();
        let src = root.path();
        let dst = src.join("out"); // egress lives UNDER the watch root
        std::fs::create_dir_all(&dst).unwrap();
        std::fs::write(src.join("a.txt"), b"payload").unwrap();

        let mut cfg = instance_cfg("loop", src, &dst, true);
        // Keep the source so re-discovery would be possible if the dir were not pruned.
        cfg.completion.on_success = OnSuccess::Archive;
        cfg.completion.archive_dir = Some(root.path().join("archive"));

        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let inst = build(cfg, store.clone());

        // Several ticks: if the dest weren't pruned, each pass would nest another level.
        inst.tick(100).await;
        inst.tick(200).await;
        inst.tick(300).await;

        assert!(dst.join("a.txt").exists(), "file replicated into the nested dest");
        assert!(!dst.join("out").exists(), "no self-nesting: out/out must not appear");
        assert!(!dst.join("a.txt/a.txt").exists());
        // Exactly one work item was ever tracked (the original) — the delivered copy was not ingested.
        assert!(store.get("loop", "out/a.txt").unwrap().is_none(), "dest copy never enqueued");
        assert_eq!(store.stats("loop").unwrap().replicated, 1, "replicated exactly once");
    }

    #[tokio::test]
    async fn instance_control_trait_drives_activation_and_trigger() {
        // Exercises the REAL `InstanceControl` impl (the P3 control-plane seam), not the fake.
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("t.txt"), b"data").unwrap();
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let inst = build(instance_cfg("ictl", src.path(), dst.path(), true), store.clone());

        // Metadata accessors.
        assert_eq!(InstanceControl::id(&inst), "ictl");
        assert!(inst.configured_enabled());
        assert_eq!(inst.dest_label(), "local");
        assert!(InstanceControl::is_active(&inst));

        // Ephemeral deactivate: the runtime flag flips but nothing is persisted.
        assert!(!inst.apply_activation(false, false, "control", 1).unwrap());
        assert!(!InstanceControl::is_active(&inst));
        assert!(
            store.load_activation("ictl").unwrap().is_none(),
            "a non-persistent flip leaves no durable override"
        );

        // Persistent deactivate: the override is recorded durably.
        assert!(!inst.apply_activation(false, true, "control", 2).unwrap());
        assert!(!store.load_activation("ictl").unwrap().unwrap().active);

        // Reset reverts to config `enabled` (true) and drops the override.
        assert!(inst.reset_activation(3).unwrap());
        assert!(InstanceControl::is_active(&inst));
        assert!(store.load_activation("ictl").unwrap().is_none());

        // trigger_scan forces a replication pass now (immediate mode: gate always open).
        inst.trigger_scan(now_ms(), false).await;
        assert!(dst.path().join("t.txt").exists(), "trigger replicated the file");
    }

    #[test]
    fn build_rejects_empty_egress() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let mut cfg = instance_cfg("bad", src.path(), dst.path(), true);
        cfg.egress.clear();
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        assert!(Instance::build(
            cfg,
            &GlobalCfg::default(),
            store,
            Arc::new(PriorityGate::new(8)),
            Arc::new(TokenBucket::unlimited()),
            None,
            &DestDeps::default(),
            Events::disabled(),
        )
        .is_err());
    }

    #[test]
    fn build_accepts_multi_egress_and_joins_dest_labels() {
        // P6, DESIGN §20-B: the v1 len==1 restriction is lifted — N>=1 egress entries now build fine.
        let src = tempfile::tempdir().unwrap();
        let dst1 = tempfile::tempdir().unwrap();
        let dst2 = tempfile::tempdir().unwrap();
        let mut cfg = instance_cfg("fanout", src.path(), dst1.path(), true);
        cfg.egress.push(EgressCfg::Local(LocalEgress {
            path: dst2.path().to_path_buf(),
            fsync: false,
        }));
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let inst = Instance::build(
            cfg,
            &GlobalCfg::default(),
            store,
            Arc::new(PriorityGate::new(8)),
            Arc::new(TokenBucket::unlimited()),
            None,
            &DestDeps::default(),
            Events::disabled(),
        )
        .expect("multi-egress instance builds");
        assert_eq!(inst.dest_label(), "local+local");
    }

    // ---- P3 UNS event emission (the engine-wiring slice) ----------------------------------------

    use crate::dest::{Destination, SharedDestination};
    use crate::domain::{Delivered, ProgressSink, ResumeState, WorkItem};
    use crate::error::{ReplError, Result as ReplResult};
    use crate::events::test_support::recording_events;

    /// A destination whose `deliver` always fails transiently — keeps the source file on disk (so a
    /// second scan re-discovers a *known* file) for the `FileReady` dedup test.
    struct AlwaysFail;
    #[async_trait::async_trait]
    impl Destination for AlwaysFail {
        fn kind(&self) -> &'static str {
            "failing"
        }
        fn supports_resume(&self) -> bool {
            false
        }
        async fn deliver(
            &self,
            _i: &WorkItem,
            _r: Option<ResumeState>,
            _p: &ProgressSink,
            _b: &Bandwidth,
        ) -> ReplResult<Delivered> {
            Err(ReplError::Transient("boom".into()))
        }
        async fn verify(
            &self,
            _i: &WorkItem,
            _d: &Delivered,
            _v: crate::config::Verify,
        ) -> ReplResult<()> {
            Ok(())
        }
        async fn abort(&self, _i: &WorkItem, _r: &ResumeState) -> ReplResult<()> {
            Ok(())
        }
    }

    // ---- Feature A: INGRESS runtime dedup + event wiring (`report_scan_issues`) ------------------
    //
    // The engine-level counterpart of `permission.rs`'s isolated `PermissionLog` unit tests and the
    // worker's egress dedup test: these drive `Instance::report_scan_issues` directly (a `ScanIssue`
    // with a deterministic `PermissionDenied` kind — dodging the platform-dependent kind a real
    // unreadable dir yields) to prove the exact "log once, not per rescan; re-arm on recovery" wiring
    // that replaces the watcher's old silent per-rescan `debug!`.

    fn build_with_events(cfg: InstanceCfg, store: Arc<dyn StateStore>, events: Events) -> Instance {
        Instance::build(
            cfg,
            &GlobalCfg::default(),
            store,
            Arc::new(PriorityGate::new(8)),
            Arc::new(TokenBucket::unlimited()),
            None,
            &DestDeps::default(),
            events,
        )
        .expect("instance builds")
    }

    #[tokio::test]
    async fn ingress_permission_denied_logs_once_and_re_arms_on_recovery() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let (fake, events) = recording_events();
        let inst = build_with_events(instance_cfg("iperm", src.path(), dst.path(), true), store, events);

        let bad = ScanIssue {
            path: src.path().join("locked-subdir"),
            kind: std::io::ErrorKind::PermissionDenied,
        };
        // Two rescans' worth of the SAME still-erroring ingress issue → exactly one event.
        inst.report_scan_issues(std::slice::from_ref(&bad), 1_000).await;
        inst.report_scan_issues(std::slice::from_ref(&bad), 2_000).await;
        assert_eq!(
            fake.events_named("PermissionDenied").len(),
            1,
            "ingress permission denial is emitted once, not once per rescan"
        );

        // A clean pass (the path no longer errors) evicts + re-arms the dedup; a later re-break emits
        // a fresh event (the "re-log on state change" half of the contract).
        inst.report_scan_issues(&[], 3_000).await;
        inst.report_scan_issues(std::slice::from_ref(&bad), 4_000).await;
        assert_eq!(
            fake.events_named("PermissionDenied").len(),
            2,
            "recovery re-arms the dedup, so a subsequent re-break emits again"
        );
    }

    #[tokio::test]
    async fn ingress_non_permission_scan_issue_dedups_without_emitting_an_event() {
        // A non-permission unreadable-dir kind (e.g. a watched subdir removed → NotFound) still gets the
        // dedup treatment (no per-rescan spam) but is NOT a PermissionDenied event.
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let (fake, events) = recording_events();
        let inst = build_with_events(instance_cfg("inf", src.path(), dst.path(), true), store, events);

        let gone = ScanIssue {
            path: src.path().join("vanished"),
            kind: std::io::ErrorKind::NotFound,
        };
        inst.report_scan_issues(std::slice::from_ref(&gone), 1_000).await;
        inst.report_scan_issues(std::slice::from_ref(&gone), 2_000).await;
        assert!(
            fake.events_named("PermissionDenied").is_empty(),
            "a NotFound scan issue is deduped but not a permission event"
        );
    }

    #[tokio::test]
    async fn seed_permission_log_suppresses_the_first_rescan_reemit_for_a_retain_instance() {
        // Mirrors app.rs: for a `retain` instance the startup path already emitted one PermissionDenied
        // per violation; seeding the runtime ingress dedup-log makes the first rescan stay quiet for the
        // same (path, kind), so the "emitted once per distinct (path, error kind)" guarantee holds across
        // the startup→runtime boundary.
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let (fake, events) = recording_events();
        let inst = build_with_events(instance_cfg("iseed", src.path(), dst.path(), true), store, events);

        let ingress_path = src.path().join("locked");
        let violations = vec![crate::permission::Violation {
            path: ingress_path.clone(),
            role: Role::Ingress,
            kind: std::io::ErrorKind::PermissionDenied,
            msg: "denied".into(),
        }];
        inst.seed_permission_log(&violations, 1_000);

        let issue = ScanIssue { path: ingress_path, kind: std::io::ErrorKind::PermissionDenied };
        inst.report_scan_issues(std::slice::from_ref(&issue), 1_001).await;
        assert!(
            fake.events_named("PermissionDenied").is_empty(),
            "the seeded startup violation suppresses the first-rescan re-emit"
        );
    }

    #[tokio::test]
    async fn tick_emits_discovery_lifecycle_and_state_events() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("a.txt"), b"hello").unwrap();
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let (fake, events) = recording_events();
        let inst = Instance::build(
            instance_cfg("evt", src.path(), dst.path(), true),
            &GlobalCfg::default(),
            store.clone(),
            Arc::new(PriorityGate::new(8)),
            Arc::new(TokenBucket::unlimited()),
            None,
            &DestDeps::default(),
            events,
        )
        .expect("instance builds");

        inst.tick(1_000).await;

        // Instance-level discovery events (the facade-derived topic itself is edgecommons' own tested
        // concern — see the `crate::events` module docs' "Testability" note).
        let ready = fake.events_named("FileReady");
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].body["path"], serde_json::json!("a.txt"));
        let scan = fake.events_named("ScanComplete");
        assert_eq!(scan.len(), 1);
        assert_eq!(scan[0].body["discovered"], serde_json::json!(1));

        // The worker's per-file lifecycle events rode the same emitter (Instance→Worker wiring).
        assert_eq!(fake.events_named("ReplicationStarted").len(), 1);
        assert_eq!(fake.events_named("ReplicationCompleted").len(), 1);
        assert_eq!(fake.events_named("FileDeleted").len(), 1);

        // The retained state snapshot republish is gone (DESIGN: `state` is now a reserved,
        // library-owned UNS class — see `crate::events` module docs); the durable-store-backed
        // `get-status` document is exercised directly in `crate::control`'s tests.
    }

    #[tokio::test]
    async fn tick_does_not_reemit_file_ready_for_a_known_file() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("keep.txt"), b"stays").unwrap();
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let (fake, events) = recording_events();
        let dest: SharedDestination = Arc::new(AlwaysFail);
        let inst = Instance::build_with_dest(
            instance_cfg("dd", src.path(), dst.path(), true),
            &GlobalCfg::default(),
            store.clone(),
            Arc::new(PriorityGate::new(8)),
            Arc::new(TokenBucket::unlimited()),
            None,
            dest,
            events,
        )
        .expect("instance builds");

        // First tick: FileReady once; the transfer fails, so the source stays on disk as a Failed row.
        inst.tick(1_000).await;
        assert_eq!(fake.events_named("FileReady").len(), 1);
        assert_eq!(
            store.get("dd", "keep.txt").unwrap().unwrap().state,
            ItemState::Failed
        );

        // Second tick: the file is re-discovered on disk but is already a tracked row → no new
        // FileReady and no ScanComplete (nothing newly enqueued).
        inst.tick(2_000).await;
        assert_eq!(fake.events_named("FileReady").len(), 1, "known file not re-announced");
        assert_eq!(fake.events_named("ScanComplete").len(), 1, "no scan heartbeat without new work");
    }

    #[tokio::test]
    async fn concurrent_ticks_announce_a_new_file_exactly_once() {
        // The per-instance tick guard: a control `trigger` tick and the run-loop tick on the same
        // `Arc<Instance>` must not both announce a brand-new file. AlwaysFail keeps the source on disk
        // (Failed row) so both ticks discover it, but only the first (serialized) sees it as new.
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("c.txt"), b"data").unwrap();
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let (fake, events) = recording_events();
        let dest: SharedDestination = Arc::new(AlwaysFail);
        let inst = Arc::new(
            Instance::build_with_dest(
                instance_cfg("cc", src.path(), dst.path(), true),
                &GlobalCfg::default(),
                store.clone(),
                Arc::new(PriorityGate::new(8)),
                Arc::new(TokenBucket::unlimited()),
                None,
                dest,
                events,
            )
            .expect("instance builds"),
        );

        let a = {
            let i = inst.clone();
            tokio::spawn(async move { i.tick(1_000).await })
        };
        let b = {
            let i = inst.clone();
            tokio::spawn(async move { i.tick(1_000).await })
        };
        a.await.unwrap();
        b.await.unwrap();

        assert_eq!(
            fake.events_named("FileReady").len(),
            1,
            "a new file is announced exactly once across concurrent ticks"
        );
        assert_eq!(
            fake.events_named("ScanComplete").len(),
            1,
            "only the tick that enqueued new work emits a ScanComplete"
        );
    }

    #[tokio::test]
    async fn deactivated_instance_emits_no_events() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("a.txt"), b"x").unwrap();
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let (fake, events) = recording_events();
        let inst = Instance::build(
            instance_cfg("off", src.path(), dst.path(), false),
            &GlobalCfg::default(),
            store.clone(),
            Arc::new(PriorityGate::new(8)),
            Arc::new(TokenBucket::unlimited()),
            None,
            &DestDeps::default(),
            events,
        )
        .expect("instance builds");

        inst.tick(1_000).await; // inactive → tick returns before any discovery/emission
        assert!(fake.is_empty(), "an inactive instance publishes nothing");
    }

    // ---- P4 scheduling: engine gating (cron / window admission) ---------------------------------

    use crate::config::{CronSchedule, WindowSchedule};

    const MIN: i64 = 60_000;
    const HOUR: i64 = 60 * MIN;
    const DAY: i64 = 24 * HOUR;

    fn instance_cfg_scheduled(
        id: &str,
        src: &Path,
        dst: &Path,
        schedule: ScheduleCfg,
    ) -> InstanceCfg {
        let mut cfg = instance_cfg(id, src, dst, true);
        cfg.schedule = schedule;
        cfg
    }

    fn cron_schedule(expr: &str) -> ScheduleCfg {
        ScheduleCfg::Cron(CronSchedule {
            expression: expr.to_string(),
            timezone: None,
        })
    }

    fn window_schedule(open: &str, duration_mins: u64, on_close: WindowClose) -> ScheduleCfg {
        ScheduleCfg::Window(WindowSchedule {
            open: open.to_string(),
            close: None,
            duration_mins: Some(duration_mins),
            timezone: None,
            on_window_close: on_close,
        })
    }

    #[tokio::test]
    async fn cron_schedule_withholds_ready_work_then_releases_all_on_a_fire() {
        // "0 * * * *" fires on the hour. First tick (t=0) establishes the baseline (never replays
        // history); a tick before the next fire gates closed; the tick that crosses the hour boundary
        // drains everything accumulated in the interim. Seed MORE files than `per_limit` (default 4) so
        // the assertion actually pins DESIGN §12.2's "release ALL ready work at each fire" — a single
        // bounded batch (drain=false) would leave the overflow `Ready`, so this fails a hard-coded
        // non-draining regression.
        const N: usize = 9;
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        for i in 0..N {
            std::fs::write(src.path().join(format!("f{i}.txt")), b"hello").unwrap();
        }
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let inst = build(
            instance_cfg_scheduled("cronx", src.path(), dst.path(), cron_schedule("0 * * * *")),
            store.clone(),
        );

        inst.tick(0).await; // baseline — discovers + enqueues, but does not release (not a fire)
        for i in 0..N {
            assert_eq!(
                store.get("cronx", &format!("f{i}.txt")).unwrap().unwrap().state,
                ItemState::Ready
            );
        }

        inst.tick(30 * MIN).await; // still within the same hour — gate stays closed
        assert_eq!(store.list_by_state("cronx", ItemState::Ready).unwrap().len(), N);

        inst.tick(61 * MIN).await; // crossed the hour boundary — the fire drains the WHOLE backlog
        for i in 0..N {
            assert_eq!(
                store.get("cronx", &format!("f{i}.txt")).unwrap().unwrap().state,
                ItemState::Completed,
                "cron fire must drain every ready file, not just one per_limit batch"
            );
            assert!(dst.path().join(format!("f{i}.txt")).exists());
        }
    }

    #[tokio::test]
    async fn immediate_claims_one_bounded_batch_per_tick_leaving_overflow_ready() {
        // The counterpoint to the cron drain test: immediate mode (drain=false) claims exactly one
        // `per_limit` batch per tick, so a backlog larger than `per_limit` is NOT fully drained in a
        // single tick — proving the cron drain-loop is a distinct behavior, not the default.
        const N: usize = 9;
        let per_limit = DEFAULT_PER_INSTANCE_CONCURRENCY; // 4
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        for i in 0..N {
            std::fs::write(src.path().join(format!("f{i}.txt")), b"hi").unwrap();
        }
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        // Immediate schedule (default), same builder as the other tests.
        let inst = build(instance_cfg("imm", src.path(), dst.path(), true), store.clone());

        inst.tick(1_000).await;
        let completed = store.list_by_state("imm", ItemState::Completed).unwrap().len();
        let ready = store.list_by_state("imm", ItemState::Ready).unwrap().len();
        assert_eq!(completed, per_limit, "one bounded batch replicated");
        assert_eq!(ready, N - per_limit, "the overflow stays Ready for the next tick");
    }

    #[tokio::test]
    async fn cron_schedule_emits_triggered_and_complete_only_on_a_fire() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("a.txt"), b"hello").unwrap();
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let (fake, events) = recording_events();
        let inst = Instance::build(
            instance_cfg_scheduled("cronevt", src.path(), dst.path(), cron_schedule("0 * * * *")),
            &GlobalCfg::default(),
            store.clone(),
            Arc::new(PriorityGate::new(8)),
            Arc::new(TokenBucket::unlimited()),
            None,
            &DestDeps::default(),
            events,
        )
        .expect("instance builds");

        inst.tick(0).await;
        inst.tick(30 * MIN).await;
        assert!(fake.events_named("ScheduleTriggered").is_empty(), "no fire yet");
        assert!(fake.events_named("ScheduleComplete").is_empty());

        inst.tick(61 * MIN).await;
        assert_eq!(fake.events_named("ScheduleTriggered").len(), 1);
        let complete = fake.events_named("ScheduleComplete");
        assert_eq!(complete.len(), 1);
        assert_eq!(complete[0].body["mode"], serde_json::json!("cron"));
    }

    #[tokio::test]
    async fn window_schedule_withholds_ready_work_while_closed_then_releases_when_open() {
        // Window: open daily at 00:00 UTC for 60 minutes.
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("a.txt"), b"hello").unwrap();
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let inst = build(
            instance_cfg_scheduled(
                "winx",
                src.path(),
                dst.path(),
                window_schedule("0 0 * * *", 60, WindowClose::FinishCurrent),
            ),
            store.clone(),
        );

        // t = 2h: closed (the 00:00 window already closed at 01:00).
        inst.tick(2 * HOUR).await;
        assert_eq!(store.get("winx", "a.txt").unwrap().unwrap().state, ItemState::Ready);
        assert!(!dst.path().join("a.txt").exists(), "withheld while the window is closed");

        // t = 24h + 30min: inside the next day's window.
        inst.tick(DAY + 30 * MIN).await;
        assert_eq!(store.get("winx", "a.txt").unwrap().unwrap().state, ItemState::Completed);
        assert!(dst.path().join("a.txt").exists(), "released once the window opens");
    }

    #[tokio::test]
    async fn window_open_drains_the_whole_backlog_not_just_one_batch() {
        // DESIGN §12.2 "continuous flow": an open window must DRAIN a backlog larger than `per_limit`
        // (default 4) in a single open tick, not trickle one bounded batch per rescan. Seed N > per_limit
        // files that accumulated while closed; one open tick (with ample time before close) must
        // complete them all.
        const N: usize = 9;
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        for i in 0..N {
            std::fs::write(src.path().join(format!("f{i}.txt")), b"payload").unwrap();
        }
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let inst = build(
            instance_cfg_scheduled(
                "windrain",
                src.path(),
                dst.path(),
                // Open daily at 00:00 for 60 minutes.
                window_schedule("0 0 * * *", 60, WindowClose::FinishCurrent),
            ),
            store.clone(),
        );

        // Closed first (files pile up in the Ready backlog).
        inst.tick(2 * HOUR).await;
        assert_eq!(store.list_by_state("windrain", ItemState::Ready).unwrap().len(), N);

        // Next day, well inside the window (opens at 24h, closes at 24h+60m): drain everything.
        inst.tick(DAY + 30 * MIN).await;
        assert_eq!(
            store.list_by_state("windrain", ItemState::Completed).unwrap().len(),
            N,
            "an open window drains the whole backlog, not just one per_limit batch"
        );
        assert_eq!(store.list_by_state("windrain", ItemState::Ready).unwrap().len(), 0);
    }

    #[tokio::test]
    async fn window_schedule_emits_opened_and_closed_transitions() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let (fake, events) = recording_events();
        let inst = Instance::build(
            instance_cfg_scheduled(
                "winevt",
                src.path(),
                dst.path(),
                window_schedule("0 0 * * *", 60, WindowClose::FinishCurrent),
            ),
            &GlobalCfg::default(),
            store.clone(),
            Arc::new(PriorityGate::new(8)),
            Arc::new(TokenBucket::unlimited()),
            None,
            &DestDeps::default(),
            events,
        )
        .expect("instance builds");

        inst.tick(2 * HOUR).await; // closed from the start — no transition (never seen open)
        assert!(fake.events_named("WindowOpened").is_empty());
        assert!(fake.events_named("WindowClosed").is_empty());

        inst.tick(30 * MIN + DAY).await; // now open
        assert_eq!(fake.events_named("WindowOpened").len(), 1);
        assert_eq!(fake.events_named("WindowOpened")[0].body["window"], serde_json::json!("0 0 * * * for 60m"));

        inst.tick(2 * HOUR + DAY).await; // closed again (between ticks — no in-flight transfer)
        assert_eq!(fake.events_named("WindowClosed").len(), 1);
        let complete = fake.events_named("ScheduleComplete");
        assert_eq!(complete.len(), 1);
        assert_eq!(complete[0].body["mode"], serde_json::json!("window"));
    }

    /// A destination whose `deliver` completes instantly on every call **except** the first, which
    /// hangs for `hang_for` (simulating a transfer that's still streaming when a window closes). Used
    /// to prove the close-time race actually preempts a slow in-flight transfer under a paused tokio
    /// clock (no real sleeping): with time paused, the runtime auto-advances to the *nearer* of the two
    /// competing timers (the window's close deadline vs. this hang), so a `close_at` that arrives well
    /// before `hang_for` deterministically wins the race.
    ///
    /// For the `pauseResume` test it also (when `store` is set) persists a mid-transfer resume
    /// checkpoint on the first (hanging) attempt — the way a real resumable destination would with its
    /// in-flight token — and records on any LATER attempt whether the worker actually handed that
    /// checkpoint back (`second_got_resume`), pinning the "resume, not restart from byte 0" guarantee
    /// that distinguishes `pauseResume` from `finishCurrent`.
    struct HangOnceDest {
        hang_for: Duration,
        calls: std::sync::atomic::AtomicU32,
        resumable: bool,
        store: Option<Arc<dyn StateStore>>,
        instance: String,
        second_got_resume: Arc<AtomicBool>,
    }
    impl HangOnceDest {
        fn new(hang_for: Duration, resumable: bool) -> Self {
            Self {
                hang_for,
                calls: std::sync::atomic::AtomicU32::new(0),
                resumable,
                store: None,
                instance: String::new(),
                second_got_resume: Arc::new(AtomicBool::new(false)),
            }
        }
    }
    #[async_trait::async_trait]
    impl Destination for HangOnceDest {
        fn kind(&self) -> &'static str {
            "hang-once"
        }
        fn supports_resume(&self) -> bool {
            self.resumable
        }
        async fn deliver(
            &self,
            item: &WorkItem,
            resume: Option<ResumeState>,
            progress: &ProgressSink,
            _bw: &Bandwidth,
        ) -> ReplResult<Delivered> {
            progress.report(1);
            if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                // First attempt: persist a mid-transfer resume checkpoint (as S3 would with its
                // uploadId), then hang so the window close preempts us.
                if let Some(store) = &self.store {
                    let _ = store.save_resume(
                        &self.instance,
                        &item.relpath,
                        self.kind(),
                        &ResumeState {
                            bytes_committed: 8,
                            token: serde_json::json!({ "partial": true }),
                        },
                    );
                }
                tokio::time::sleep(self.hang_for).await;
            } else {
                // A resumed attempt: record whether the worker handed us the saved checkpoint.
                self.second_got_resume
                    .store(resume.is_some(), std::sync::atomic::Ordering::SeqCst);
            }
            Ok(Delivered {
                bytes: item.size,
                checksum: Checksum::None,
                handle: serde_json::Value::Null,
            })
        }
        async fn verify(&self, _i: &WorkItem, _d: &Delivered, _v: Verify) -> ReplResult<()> {
            Ok(())
        }
        async fn abort(&self, _i: &WorkItem, _r: &ResumeState) -> ReplResult<()> {
            Ok(())
        }
    }

    use crate::config::Verify;
    use crate::domain::Checksum;

    #[tokio::test(start_paused = true)]
    async fn window_close_mid_transfer_pauses_and_reverts_to_ready_when_resumable() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("big.bin"), vec![0u8; 16]).unwrap();
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let (fake, events) = recording_events();
        let hang = HangOnceDest {
            store: Some(store.clone()),
            instance: "pausewin".to_string(),
            ..HangOnceDest::new(Duration::from_secs(600), true)
        };
        let second_got_resume = hang.second_got_resume.clone();
        let dest: SharedDestination = Arc::new(hang);

        let cfg = instance_cfg_scheduled(
            "pausewin",
            src.path(),
            dst.path(),
            // Open at 00:00 for 1 minute, so a claim at t=30s races a close 30s away — far shorter
            // than the fake's 600s hang.
            window_schedule("0 0 * * *", 1, WindowClose::PauseResume),
        );
        let inst = Instance::build_with_dest(
            cfg,
            &GlobalCfg::default(),
            store.clone(),
            Arc::new(PriorityGate::new(8)),
            Arc::new(TokenBucket::unlimited()),
            None,
            dest,
            events,
        )
        .expect("instance builds");

        // t=30s: inside the window (closes at 60s). The claimed transfer hangs 600s — far longer than
        // the 30s left in the window — so tokio's paused clock auto-advances to the close deadline.
        inst.tick(30_000).await;

        let row = store.get("pausewin", "big.bin").unwrap().unwrap();
        assert_eq!(
            row.state,
            ItemState::Ready,
            "a transfer paused mid-flight reverts to Ready, not left stuck InProgress"
        );
        assert!(!dst.path().join("big.bin").exists(), "never delivered — it was cancelled");
        assert_eq!(fake.events_named("ReplicationStarted").len(), 1);
        assert!(fake.events_named("ReplicationCompleted").is_empty());
        assert_eq!(fake.events_named("WindowClosed").len(), 1);
        let complete = fake.events_named("ScheduleComplete");
        assert_eq!(complete.len(), 1);
        assert_eq!(complete[0].body["mode"], serde_json::json!("window"));
        // The resume checkpoint the (hung) first attempt persisted SURVIVES the pause — the revert must
        // not clear it, or the next window would restart from byte 0 (the whole point of pauseResume).
        assert!(
            store.load_resume("pausewin", "big.bin", "hang-once").unwrap().is_some(),
            "resume checkpoint preserved across the pause"
        );

        // The window was open at t=30s (first evaluation), so exactly one WindowOpened has fired.
        assert_eq!(fake.events_named("WindowOpened").len(), 1, "the initial open, announced once");

        // A follow-up tick still INSIDE the same closed span (90s; the window closed at 60s, reopens
        // at 24h): the synchronous `window_open = false` flip at close must prevent a SECOND
        // WindowClosed/ScheduleComplete, and there is no spurious WindowOpened while still closed.
        inst.tick(90_000).await;
        assert_eq!(fake.events_named("WindowClosed").len(), 1, "close announced exactly once");
        assert_eq!(fake.events_named("ScheduleComplete").len(), 1);
        assert_eq!(fake.events_named("WindowOpened").len(), 1, "no reopen while still closed");
        assert_eq!(
            store.get("pausewin", "big.bin").unwrap().unwrap().state,
            ItemState::Ready,
            "still gated closed — not replicated between windows"
        );

        // Next day's window: the paused item is claimed again; `HangOnceDest` completes instantly on
        // this second call (simulating a resumed transfer finishing fast off its checkpoint), and must
        // have been handed the preserved resume checkpoint (resume, not restart).
        inst.tick(DAY + 30_000).await;
        assert_eq!(
            store.get("pausewin", "big.bin").unwrap().unwrap().state,
            ItemState::Completed,
            "resumed and completed in the next window"
        );
        assert!(
            second_got_resume.load(std::sync::atomic::Ordering::SeqCst),
            "the resumed attempt received the persisted checkpoint (Some), not a fresh start"
        );
        assert_eq!(
            fake.events_named("WindowOpened").len(),
            2,
            "the initial open plus exactly one reopen in the next window"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn window_close_mid_transfer_finish_current_lets_it_complete() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("big.bin"), vec![0u8; 16]).unwrap();
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let (fake, events) = recording_events();
        // `hang_for` shorter than a real close-preempt test needs: finishCurrent never cancels, so the
        // task is simply awaited to completion regardless of how long it takes past the close instant.
        let dest: SharedDestination = Arc::new(HangOnceDest::new(Duration::from_secs(600), true));

        let cfg = instance_cfg_scheduled(
            "finishwin",
            src.path(),
            dst.path(),
            window_schedule("0 0 * * *", 1, WindowClose::FinishCurrent),
        );
        let inst = Instance::build_with_dest(
            cfg,
            &GlobalCfg::default(),
            store.clone(),
            Arc::new(PriorityGate::new(8)),
            Arc::new(TokenBucket::unlimited()),
            None,
            dest,
            events,
        )
        .expect("instance builds");

        inst.tick(30_000).await; // starts inside the window; close arrives mid-transfer at 60s

        // finishCurrent: the close is announced, but the in-flight transfer was left to run to
        // completion rather than cancelled.
        assert_eq!(
            store.get("finishwin", "big.bin").unwrap().unwrap().state,
            ItemState::Completed,
            "finishCurrent lets the in-flight transfer complete past the close"
        );
        assert_eq!(fake.events_named("WindowClosed").len(), 1);
        assert_eq!(fake.events_named("ReplicationCompleted").len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn window_close_pause_resume_falls_back_to_finish_current_without_resume_support() {
        // DESIGN §12.4: `pauseResume` degrades to `finishCurrent` when the destination doesn't support
        // resume — never discard partial progress.
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("big.bin"), vec![0u8; 16]).unwrap();
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let (fake, events) = recording_events();
        let dest: SharedDestination = Arc::new(HangOnceDest::new(Duration::from_secs(600), false));

        let cfg = instance_cfg_scheduled(
            "noresumewin",
            src.path(),
            dst.path(),
            window_schedule("0 0 * * *", 1, WindowClose::PauseResume),
        );
        let inst = Instance::build_with_dest(
            cfg,
            &GlobalCfg::default(),
            store.clone(),
            Arc::new(PriorityGate::new(8)),
            Arc::new(TokenBucket::unlimited()),
            None,
            dest,
            events,
        )
        .expect("instance builds");

        inst.tick(30_000).await;

        assert_eq!(
            store.get("noresumewin", "big.bin").unwrap().unwrap().state,
            ItemState::Completed,
            "no resume support -> falls back to finishCurrent, never cancelled"
        );
        assert_eq!(fake.events_named("WindowClosed").len(), 1);
    }

    #[tokio::test]
    async fn pause_revert_completes_verified_and_rearms_in_progress() {
        // The window-close pauseResume abort must not STRAND an item that raced into `Verified` (its
        // delivery+verify already succeeded, aborted in the narrow Verified→Completed gap): it is driven
        // to completion, not left dangling until a process restart. An `InProgress` item reverts to
        // `Ready`. Drives `pause_revert` directly (the exact race is otherwise nondeterministic).
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        // A Verified item: its object already lives at the destination (same size → Size re-verify ok).
        std::fs::write(src.path().join("v.txt"), b"hello").unwrap();
        std::fs::write(dst.path().join("v.txt"), b"hello").unwrap();
        // An InProgress item (was streaming when the window closed).
        std::fs::write(src.path().join("p.txt"), b"world").unwrap();

        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        store.upsert_ready("pr", "v.txt", 5, 0, 1).unwrap();
        store.set_state("pr", "v.txt", ItemState::Verified, 2).unwrap();
        store.upsert_ready("pr", "p.txt", 5, 0, 1).unwrap();
        store.set_state("pr", "p.txt", ItemState::InProgress, 2).unwrap();

        let inst = build(instance_cfg("pr", src.path(), dst.path(), true), store.clone());
        inst.pause_revert(&["v.txt".to_string(), "p.txt".to_string()], 100).await;

        // Verified → Completed (source deleted, stat bumped), not stranded non-terminal.
        assert_eq!(store.get("pr", "v.txt").unwrap().unwrap().state, ItemState::Completed);
        assert!(!src.path().join("v.txt").exists(), "verified item completed its source action");
        assert!(store.stats("pr").unwrap().replicated >= 1, "verified completion counted");
        // InProgress → Ready (re-armed for the next window; checkpoint preserved).
        assert_eq!(store.get("pr", "p.txt").unwrap().unwrap().state, ItemState::Ready);
    }

    #[tokio::test]
    async fn run_loop_wakes_on_the_schedule_edge_and_fires_a_cron() {
        // Covers the scheduled-wake path in run(): `sched_wake_delay`'s `Some(at)` branch AND the
        // `sleep(sched_wake_delay())` select arm — the other run() test uses an immediate schedule whose
        // `next_wake` is `None` (the fallback branch). A per-second cron with a large rescan means the
        // ONLY driver after the initial sweep is the schedule-edge wake, which crosses a fire ~1s in and
        // drains the file. Real-time (not paused) because the engine's `now`/cron eval read the real
        // clock; mirrors `run_replicates_then_stops_on_cancel`'s poll-then-cancel style.
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("tick.txt"), b"cron").unwrap();
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let mut cfg =
            instance_cfg_scheduled("cronrun", src.path(), dst.path(), cron_schedule("* * * * * *"));
        cfg.ingress.rescan_secs = Some(3600); // keep the periodic rescan out of the way
        let inst = Arc::new(build(cfg, store.clone()));

        let cancel = CancellationToken::new();
        let handle = {
            let inst = inst.clone();
            let c = cancel.clone();
            tokio::spawn(async move { inst.run(c).await })
        };

        // The initial sweep sets the cron baseline (no release). The next schedule-edge wake, ~1s later,
        // crosses a per-second fire and drains the file via the sched_wake select arm.
        let out = dst.path().join("tick.txt");
        let mut landed = false;
        for _ in 0..100 {
            if out.exists() {
                landed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("run loop exits on cancel")
            .unwrap();
        assert!(landed, "the schedule-edge wake fired a cron and replicated the file");
    }
}

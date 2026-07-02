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
//! ## Scheduling (P1)
//! Immediate mode only: an active instance releases ready work as soon as it is discovered. A
//! `cron`/`window` schedule is accepted but **treated as immediate** with a warning — the cron/window
//! scheduler is P4 (DESIGN §12/§21). See the `TODO(P4)` in [`Instance::build`].

pub mod queue;
pub mod watcher;
pub mod worker;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ggcommons::metrics::MetricService;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::{EgressCfg, GlobalCfg, InstanceCfg, ScheduleCfg};
use crate::control::{instance_state_snapshot, InstanceControl};
use crate::dest::{build_destination, DestDeps, SharedDestination};
use crate::domain::ItemState;
use crate::error::Result;
use crate::events::{Event, Events};
use crate::ratelimit::{parse_byte_rate, Bandwidth, Clock, SystemClock, TokenBucket};
use crate::readiness::RealProbe;
use crate::state::StateStore;

use queue::Queue;
use watcher::Watcher;
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
    /// Short destination label (`dest.kind()`, e.g. `"local"`/`"s3"`) surfaced in `get-status`
    /// `inProgress` rows via [`crate::control::InstanceControl::dest_label`].
    dest_kind: &'static str,
    /// The configured schedule mode (`"immediate"`/`"cron"`/`"window"`) reported by `get-status` and
    /// the retained state snapshot (DESIGN §16). P1 runs everything immediate (cron/window is P4), but
    /// reporting the *configured* mode never misrepresents an instance an operator set to a window.
    schedule_mode: &'static str,
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
    /// UNS event emitter for this instance's discovery events (`FileReady`/`ScanComplete`) and the
    /// retained per-instance `state/instances/{id}` republish (DESIGN §17). A no-op
    /// [`Events::disabled`] when messaging is absent (or in unit tests), so the P1/P2 engine runs
    /// unchanged. The worker holds a clone for the per-file lifecycle events.
    events: Events,
}

impl Instance {
    /// Assemble an instance from its config plus the process-wide shared governors (the global
    /// concurrency semaphore and the global bandwidth bucket) and the shared durable store.
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        cfg: InstanceCfg,
        global: &GlobalCfg,
        store: Arc<dyn StateStore>,
        global_sem: Arc<Semaphore>,
        global_bw: Arc<TokenBucket>,
        metrics: Option<Arc<dyn MetricService>>,
        deps: &DestDeps,
        events: Events,
    ) -> anyhow::Result<Instance> {
        anyhow::ensure!(
            cfg.egress.len() == 1,
            "instance '{}': exactly one egress destination is required (v1)",
            cfg.id
        );
        // The backend may need the shared store (S3 resume checkpoints); always thread THIS instance's
        // store in (the credential handle rides in from `App::run`).
        let deps = deps.clone().with_store(store.clone());
        let dest = build_destination(&cfg.egress[0], &deps)
            .map_err(|e| anyhow::anyhow!("instance '{}': {e}", cfg.id))?;
        Self::build_with_dest(cfg, global, store, global_sem, global_bw, metrics, dest, events)
    }

    /// Assemble an instance with a **caller-supplied** [`SharedDestination`], bypassing the egress
    /// factory. This is the injection seam used by the end-to-end integration tests (a fake/failing
    /// destination for the error paths) and by future programmatic wiring; the normal path is
    /// [`build`](Self::build), which resolves the destination from `cfg.egress`. Unlike `build`, this
    /// does not require `cfg.egress` to be present — the destination is explicit.
    #[allow(clippy::too_many_arguments)]
    pub fn build_with_dest(
        cfg: InstanceCfg,
        global: &GlobalCfg,
        store: Arc<dyn StateStore>,
        global_sem: Arc<Semaphore>,
        global_bw: Arc<TokenBucket>,
        metrics: Option<Arc<dyn MetricService>>,
        dest: SharedDestination,
        events: Events,
    ) -> anyhow::Result<Instance> {
        // TODO(P4): cron/window scheduling. P1 supports immediate only; a non-immediate schedule is
        // accepted and run as immediate so a P4-authored config still replicates (just not gated).
        // The *configured* mode is still reported by get-status/state (DESIGN §16) so an instance an
        // operator set to a nightly window is not misrepresented as `immediate`.
        let schedule_mode = match cfg.schedule {
            ScheduleCfg::Immediate => "immediate",
            ScheduleCfg::Cron(_) => "cron",
            ScheduleCfg::Window(_) => "window",
        };
        if !matches!(cfg.schedule, ScheduleCfg::Immediate) {
            tracing::warn!(
                instance = %cfg.id,
                mode = schedule_mode,
                "cron/window scheduling is not implemented (P4); treating as immediate"
            );
        }

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
                        "local egress is under the watch root; pruning it from discovery (FR-ING-8)"
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

        let dest_kind = dest.kind();
        let queue = Queue::new(cfg.id.clone(), store.clone(), per_limit, global_sem);
        let worker = Arc::new(
            Worker::new(
                cfg.id.clone(),
                store.clone(),
                dest,
                cfg.completion.clone(),
                cfg.ingress.path.clone(),
                bw,
                retry,
                metrics,
            )
            .with_events(events.clone()),
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
            rescan,
            tick_lock: tokio::sync::Mutex::new(()),
            watcher: Arc::new(Mutex::new(watcher)),
            queue,
            worker,
            store,
            events,
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

    /// One engine iteration (the testable unit): discover ready files → durably enqueue → claim a
    /// bounded batch → process it concurrently (bounded by the per-instance ∩ global slots). A
    /// deactivated instance does nothing. `now` is the single Unix-ms clock read for this tick.
    pub async fn tick(&self, now: i64) {
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
        let ready = match tokio::task::spawn_blocking(move || {
            let mut w = watcher.lock().expect("watcher mutex");
            w.discover(&RealProbe)
        })
        .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(instance = %self.id, error = %e, "discovery task failed");
                Vec::new()
            }
        };
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
                    .instance_event(
                        &self.id,
                        Event::FileReady {
                            path: c.relpath.clone(),
                            size: c.size,
                        },
                        now,
                    )
                    .await;
            }
        }
        // Announce the scan only when it actually enqueued something new (a heartbeat every rescan
        // would be noise on the non-retained event stream); `awaiting` is the current ready backlog.
        if discovered > 0 {
            let awaiting = self
                .store
                .list_by_state(&self.id, ItemState::Ready)
                .map(|v| v.len() as u64)
                .unwrap_or(0);
            self.events
                .instance_event(&self.id, Event::ScanComplete { discovered, awaiting }, now)
                .await;
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

        // Claim up to one per-instance batch and process it, bounded by the concurrency slots.
        let batch = match self.queue.claim(self.queue.per_limit(), now) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(instance = %self.id, error = %e, "claim failed");
                return;
            }
        };
        let processed = !batch.is_empty();
        if processed {
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

        // Retained current-state (DESIGN §17.2 / FR-EVT-4): after any transition this tick produced
        // (new discoveries and/or a processed batch, whose per-file terminal transitions have all
        // been awaited above), republish the compact per-instance snapshot so a late/reconnecting UNS
        // subscriber renders correctly. No-op when messaging is absent.
        if discovered > 0 || processed {
            self.publish_state(now).await;
        }
    }

    /// Republish this instance's compact current-state snapshot to the retained
    /// `state/instances/{id}` topic (DESIGN §17.2), built from the same durable-store source as the
    /// control plane's `get-status` reply (so a snapshot and a request/reply status match). No-op
    /// when messaging is absent.
    async fn publish_state(&self, now: i64) {
        if !self.events.is_enabled() {
            return;
        }
        let snapshot = instance_state_snapshot(
            self.store.as_ref(),
            &self.id,
            self.is_active(),
            self.config_enabled,
            self.schedule_mode,
            self.dest_kind,
            now,
        );
        self.events.publish_instance_state(&self.id, snapshot).await;
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
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = ticker.tick() => self.tick(now_ms()).await,
                Some(_) = nudge_rx.recv() => self.tick(now_ms()).await,
            }
        }
        tracing::info!(instance = %self.id, "instance stopped (durable state checkpointed)");
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
        let mut watcher = match notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
            if res.is_ok() {
                // Coalesce: a full channel already has a pending nudge, so a drop is harmless.
                let _ = nudge_tx.try_send(());
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
        self.dest_kind
    }

    fn schedule_mode(&self) -> &str {
        self.schedule_mode
    }

    async fn trigger_scan(&self, now: i64) {
        // Force one full reconciliation pass now (discover → enqueue → claim → process), exactly as
        // the periodic rescan / OS-watch nudge would. A deactivated instance's tick is a no-op.
        self.tick(now).await;
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
            topics: None,
        }
    }

    fn build(cfg: InstanceCfg, store: Arc<dyn StateStore>) -> Instance {
        Instance::build(
            cfg,
            &GlobalCfg::default(),
            store,
            Arc::new(Semaphore::new(8)),
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

        // trigger_scan forces a replication pass now.
        inst.trigger_scan(now_ms()).await;
        assert!(dst.path().join("t.txt").exists(), "trigger replicated the file");
    }

    #[test]
    fn build_rejects_multi_egress() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let mut cfg = instance_cfg("bad", src.path(), dst.path(), true);
        cfg.egress.push(EgressCfg::Local(LocalEgress {
            path: dst.path().to_path_buf(),
            fsync: false,
        }));
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
        assert!(Instance::build(
            cfg,
            &GlobalCfg::default(),
            store,
            Arc::new(Semaphore::new(8)),
            Arc::new(TokenBucket::unlimited()),
            None,
            &DestDeps::default(),
            Events::disabled(),
        )
        .is_err());
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
            Arc::new(Semaphore::new(8)),
            Arc::new(TokenBucket::unlimited()),
            None,
            &DestDeps::default(),
            events,
        )
        .expect("instance builds");

        inst.tick(1_000).await;

        // Instance-level discovery events on the per-instance topic.
        let ready = fake.events_named("FileReady");
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].body["path"], serde_json::json!("a.txt"));
        assert!(fake
            .topics()
            .contains(&"gw-01/file-replicator/evt/instances/evt/FileReady".to_string()));
        let scan = fake.events_named("ScanComplete");
        assert_eq!(scan.len(), 1);
        assert_eq!(scan[0].body["discovered"], serde_json::json!(1));

        // The worker's per-file lifecycle events rode the same emitter (Instance→Worker wiring).
        assert_eq!(fake.events_named("ReplicationStarted").len(), 1);
        assert_eq!(fake.events_named("ReplicationCompleted").len(), 1);
        assert_eq!(fake.events_named("FileDeleted").len(), 1);

        // The retained per-instance state snapshot was republished after the tick, and reflects the
        // completed transfer (replicated count = 1), matching the get-status document shape.
        let states = fake.state_snapshots();
        assert!(!states.is_empty(), "state republished after the tick");
        let (topic, msg) = states.last().unwrap();
        assert_eq!(topic, "gw-01/file-replicator/state/instances/evt");
        assert_eq!(msg.body["instance"], serde_json::json!("evt"));
        assert_eq!(msg.body["active"], serde_json::json!(true));
        assert_eq!(msg.body["replicated"]["count"], serde_json::json!(1));
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
            Arc::new(Semaphore::new(8)),
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
                Arc::new(Semaphore::new(8)),
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
            Arc::new(Semaphore::new(8)),
            Arc::new(TokenBucket::unlimited()),
            None,
            &DestDeps::default(),
            events,
        )
        .expect("instance builds");

        inst.tick(1_000).await; // inactive → tick returns before any discovery/emission
        assert!(fake.topics().is_empty(), "an inactive instance publishes nothing");
    }
}

//! # file-replicator — P1 end-to-end integration tests
//!
//! Drives the P1 engine (immediate mode, local destination) through its public library surface via a
//! small in-process harness: real [`Instance`]s built against `tempfile` directories, a real durable
//! [`SqliteStore`], and the deterministic `scan()` discovery path (glob readiness = "ready
//! immediately"), so nothing depends on filesystem-event or timer timing. Each test drives
//! [`Instance::tick`] with an explicit `now`, or [`Instance::run`] where crash-recovery-on-start is the
//! behavior under test.
//!
//! Coverage (DESIGN §8/§13):
//! - happy path — nested subtree replicated (subtree preserved), checksum-verified, then archived,
//!   source gone;
//! - `onSuccess = delete` variant;
//! - failure — an injected failing destination is retried then, on exhaustion, quarantined to
//!   `failedDir` with an `.error.json` sidecar, while a second instance is unaffected (isolation);
//! - crash safety (§13.2) — a store left `Verified` by a "crash" (engine dropped, DB reopened) is
//!   recovered idempotently on restart: the source is completed with **no re-delivery** and no loss;
//! - bandwidth cap — a per-instance rate cap forces the mandated minimum transfer time.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use file_replicator::config::{
    CompletionCfg, EgressCfg, GlobReadiness, GlobalCfg, IngressCfg, InstanceCfg, LimitsCfg,
    LocalEgress, OnExhausted, OnSuccess, ReadinessCfg, RetryCfg, ScheduleCfg, Verify,
};
use file_replicator::dest::{DestDeps, Destination, SharedDestination};
use file_replicator::domain::{
    Delivered, DestPhase, DestState, ItemState, ProgressSink, ResumeState, WorkItem,
};
use file_replicator::events::Events;
use file_replicator::error::{ReplError, Result as ReplResult};
use file_replicator::instance::Instance;
use file_replicator::ratelimit::{parse_byte_rate, Clock, ManualClock, SystemClock, TokenBucket};
use file_replicator::state::{Activation, SqliteStore, StateStore, Stats, StatsDelta};

// ---- harness helpers ----------------------------------------------------------------------------

/// Immediate-readiness ingress (glob strategy, empty `ready` list = every scanned file is ready at
/// once), so a single [`Instance::tick`] discovers + processes with no stability-window timing.
fn ingress_immediate(root: &Path) -> IngressCfg {
    IngressCfg {
        path: root.to_path_buf(),
        recursive: true,
        include: vec![],
        exclude: vec![],
        rescan_secs: Some(1),
        readiness: ReadinessCfg::Glob(GlobReadiness { ready: vec![] }),
    }
}

/// A local-egress instance config with the given source/destination and completion policy.
fn local_cfg(id: &str, src: &Path, dst: &Path, completion: CompletionCfg) -> InstanceCfg {
    InstanceCfg {
        id: id.to_string(),
        enabled: true,
        ingress: ingress_immediate(src),
        egress: vec![EgressCfg::Local(LocalEgress {
            path: dst.to_path_buf(),
            fsync: false,
        })],
        schedule: ScheduleCfg::Immediate,
        completion,
        retry: None,
        limits: None,
        topics: None,
    }
}

fn store_in_memory() -> Arc<dyn StateStore> {
    Arc::new(SqliteStore::open_in_memory().unwrap())
}

/// Build an instance with the real egress-derived destination, an unlimited global concurrency +
/// bandwidth governor, and no metrics.
fn build(cfg: InstanceCfg, store: Arc<dyn StateStore>) -> Instance {
    Instance::build(
        cfg,
        &GlobalCfg::default(),
        store,
        Arc::new(Semaphore::new(16)),
        Arc::new(TokenBucket::unlimited()),
        None,
        &DestDeps::default(),
        Events::disabled(),
    )
    .expect("instance builds")
}

fn write_file(path: &Path, bytes: &[u8]) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, bytes).unwrap();
}

/// A destination whose `deliver` always fails transiently (error-path tests). Records how many times
/// `deliver` was invoked so tests can assert "no re-delivery".
struct FailingDest {
    deliver_calls: Arc<std::sync::atomic::AtomicU32>,
}

impl FailingDest {
    fn new() -> Self {
        FailingDest {
            deliver_calls: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        }
    }
}

#[async_trait]
impl Destination for FailingDest {
    fn kind(&self) -> &'static str {
        "failing"
    }
    fn supports_resume(&self) -> bool {
        false
    }
    async fn deliver(
        &self,
        _item: &WorkItem,
        _resume: Option<ResumeState>,
        _progress: &ProgressSink,
        _bw: &file_replicator::ratelimit::Bandwidth,
    ) -> ReplResult<Delivered> {
        self.deliver_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(ReplError::Transient("network unreachable".into()))
    }
    async fn verify(&self, _item: &WorkItem, _d: &Delivered, _p: Verify) -> ReplResult<()> {
        Ok(())
    }
    async fn abort(&self, _item: &WorkItem, _resume: &ResumeState) -> ReplResult<()> {
        Ok(())
    }
}

/// A destination whose `deliver` **succeeds** (writing the object) but whose `verify` always fails
/// with an integrity error — the exact seam that guards FR-CMP-3/4: completion (source delete/archive)
/// must fire ONLY after a passing verify. Delivers to a real local dir so the "object exists but is
/// rejected" case is faithful; counts deliver calls to assert no runaway re-delivery.
struct VerifyFailingDest {
    inner: file_replicator::dest::LocalDest,
    deliver_calls: Arc<std::sync::atomic::AtomicU32>,
}

impl VerifyFailingDest {
    fn new(dst: &Path) -> Self {
        VerifyFailingDest {
            inner: file_replicator::dest::LocalDest::new(
                &LocalEgress { path: dst.to_path_buf(), fsync: false },
                file_replicator::integrity::Algorithm::Crc32c,
            ),
            deliver_calls: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        }
    }
}

#[async_trait]
impl Destination for VerifyFailingDest {
    fn kind(&self) -> &'static str {
        "verify-failing"
    }
    fn supports_resume(&self) -> bool {
        false
    }
    async fn deliver(
        &self,
        item: &WorkItem,
        resume: Option<ResumeState>,
        progress: &ProgressSink,
        bw: &file_replicator::ratelimit::Bandwidth,
    ) -> ReplResult<Delivered> {
        self.deliver_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.deliver(item, resume, progress, bw).await
    }
    async fn verify(&self, _item: &WorkItem, _d: &Delivered, _p: Verify) -> ReplResult<()> {
        Err(ReplError::Integrity("checksum mismatch (injected)".into()))
    }
    async fn abort(&self, _item: &WorkItem, _resume: &ResumeState) -> ReplResult<()> {
        Ok(())
    }
}

/// A [`StateStore`] wrapper that fails the FIRST `set_state(_, _, Completed, _)` exactly once (then
/// delegates), simulating a crash between the source side effect and the durable `Completed` write.
/// Everything else delegates to a real [`SqliteStore`], so crash recovery re-derives terminal state.
struct FailCompletedOnceStore {
    inner: SqliteStore,
    armed: std::sync::atomic::AtomicBool,
}

impl FailCompletedOnceStore {
    fn new(inner: SqliteStore) -> Self {
        FailCompletedOnceStore {
            inner,
            armed: std::sync::atomic::AtomicBool::new(true),
        }
    }
}

impl StateStore for FailCompletedOnceStore {
    fn upsert_ready(
        &self,
        instance: &str,
        relpath: &str,
        size: u64,
        mtime_ms: i64,
        now: i64,
    ) -> ReplResult<()> {
        self.inner.upsert_ready(instance, relpath, size, mtime_ms, now)
    }
    fn claim_ready(&self, instance: &str, limit: usize, now: i64) -> ReplResult<Vec<WorkItem>> {
        self.inner.claim_ready(instance, limit, now)
    }
    fn set_state(
        &self,
        instance: &str,
        relpath: &str,
        state: ItemState,
        now: i64,
    ) -> ReplResult<()> {
        if state == ItemState::Completed
            && self.armed.swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(ReplError::State("injected crash before Completed persist".into()));
        }
        self.inner.set_state(instance, relpath, state, now)
    }
    fn record_attempt(
        &self,
        instance: &str,
        relpath: &str,
        err: &str,
        next_state: ItemState,
        next_attempt_at: i64,
        now: i64,
    ) -> ReplResult<()> {
        self.inner
            .record_attempt(instance, relpath, err, next_state, next_attempt_at, now)
    }
    fn set_bytes_done(&self, instance: &str, relpath: &str, bytes: u64, now: i64) -> ReplResult<()> {
        self.inner.set_bytes_done(instance, relpath, bytes, now)
    }
    fn get(&self, instance: &str, relpath: &str) -> ReplResult<Option<WorkItem>> {
        self.inner.get(instance, relpath)
    }
    fn recover_incomplete(&self, instance: &str) -> ReplResult<Vec<WorkItem>> {
        self.inner.recover_incomplete(instance)
    }
    fn list_by_state(&self, instance: &str, state: ItemState) -> ReplResult<Vec<WorkItem>> {
        self.inner.list_by_state(instance, state)
    }
    fn save_resume(
        &self,
        instance: &str,
        relpath: &str,
        dest: &str,
        r: &ResumeState,
    ) -> ReplResult<()> {
        self.inner.save_resume(instance, relpath, dest, r)
    }
    fn load_resume(
        &self,
        instance: &str,
        relpath: &str,
        dest: &str,
    ) -> ReplResult<Option<ResumeState>> {
        self.inner.load_resume(instance, relpath, dest)
    }
    fn clear_resume(&self, instance: &str, relpath: &str, dest: &str) -> ReplResult<()> {
        self.inner.clear_resume(instance, relpath, dest)
    }
    fn clear_resume_all(&self, instance: &str, relpath: &str) -> ReplResult<()> {
        self.inner.clear_resume_all(instance, relpath)
    }
    fn dest_states(&self, instance: &str, relpath: &str) -> ReplResult<Vec<DestState>> {
        self.inner.dest_states(instance, relpath)
    }
    fn set_dest_phase(
        &self,
        instance: &str,
        relpath: &str,
        dest: &str,
        phase: DestPhase,
        now: i64,
    ) -> ReplResult<()> {
        self.inner.set_dest_phase(instance, relpath, dest, phase, now)
    }
    #[allow(clippy::too_many_arguments)]
    fn record_dest_attempt(
        &self,
        instance: &str,
        relpath: &str,
        dest: &str,
        err: &str,
        phase: DestPhase,
        next_attempt_at: i64,
        now: i64,
    ) -> ReplResult<()> {
        self.inner
            .record_dest_attempt(instance, relpath, dest, err, phase, next_attempt_at, now)
    }
    fn clear_dest_states(&self, instance: &str, relpath: &str) -> ReplResult<()> {
        self.inner.clear_dest_states(instance, relpath)
    }
    fn roll_up_item(
        &self,
        instance: &str,
        relpath: &str,
        state: ItemState,
        attempts: u32,
        next_attempt_at: i64,
        last_error: Option<&str>,
        now: i64,
    ) -> ReplResult<()> {
        self.inner
            .roll_up_item(instance, relpath, state, attempts, next_attempt_at, last_error, now)
    }
    fn load_activation(&self, instance: &str) -> ReplResult<Option<Activation>> {
        self.inner.load_activation(instance)
    }
    fn set_activation(&self, instance: &str, active: bool, source: &str, now: i64) -> ReplResult<()> {
        self.inner.set_activation(instance, active, source, now)
    }
    fn clear_activation(&self, instance: &str) -> ReplResult<()> {
        self.inner.clear_activation(instance)
    }
    fn bump_stats(&self, instance: &str, d: StatsDelta) -> ReplResult<()> {
        self.inner.bump_stats(instance, d)
    }
    fn stats(&self, instance: &str) -> ReplResult<Stats> {
        self.inner.stats(instance)
    }
}

// ---- 1. happy path: nested subtree → replicated (checksum-verified) → archived, source gone -------

#[tokio::test]
async fn happy_path_replicates_nested_subtree_then_archives() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let archive = tempfile::tempdir().unwrap();

    write_file(&src.path().join("top.txt"), b"top-level payload");
    write_file(&src.path().join("a/b/deep.csv"), b"col1,col2\n1,2\n");

    let completion = CompletionCfg {
        on_success: OnSuccess::Archive,
        archive_dir: Some(archive.path().to_path_buf()),
        verify: Verify::Checksum, // completion runs only after a passing re-hash of the delivered object
        ..Default::default()
    };
    let store = store_in_memory();
    let inst = build(local_cfg("hp", src.path(), dst.path(), completion), store.clone());

    inst.tick(1_000).await;

    // Replicated, subtree preserved.
    assert_eq!(std::fs::read(dst.path().join("top.txt")).unwrap(), b"top-level payload");
    assert_eq!(
        std::fs::read(dst.path().join("a/b/deep.csv")).unwrap(),
        b"col1,col2\n1,2\n"
    );
    // Archived (source moved out of the ingress tree).
    assert_eq!(std::fs::read(archive.path().join("top.txt")).unwrap(), b"top-level payload");
    assert!(archive.path().join("a/b/deep.csv").exists());
    assert!(!src.path().join("top.txt").exists(), "source archived away");
    assert!(!src.path().join("a/b/deep.csv").exists(), "source archived away");
    // Durable state + stats.
    assert_eq!(store.get("hp", "top.txt").unwrap().unwrap().state, ItemState::Completed);
    assert_eq!(store.get("hp", "a/b/deep.csv").unwrap().unwrap().state, ItemState::Completed);
    assert_eq!(store.stats("hp").unwrap().replicated, 2);
}

// ---- 2. delete-on-success variant ---------------------------------------------------------------

#[tokio::test]
async fn delete_on_success_removes_source() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    write_file(&src.path().join("x.txt"), b"delete me after copy");

    let completion = CompletionCfg {
        on_success: OnSuccess::Delete,
        ..Default::default()
    };
    let store = store_in_memory();
    let inst = build(local_cfg("del", src.path(), dst.path(), completion), store.clone());

    inst.tick(1_000).await;

    assert_eq!(std::fs::read(dst.path().join("x.txt")).unwrap(), b"delete me after copy");
    assert!(!src.path().join("x.txt").exists(), "source deleted on success");
    assert_eq!(store.get("del", "x.txt").unwrap().unwrap().state, ItemState::Completed);
    assert_eq!(store.stats("del").unwrap().replicated, 1);
}

// ---- 3. failure → retry → exhaustion → quarantine + sidecar; other instance unaffected -----------

#[tokio::test]
async fn failed_delivery_retries_then_quarantines_while_other_instance_unaffected() {
    let store = store_in_memory();

    // Instance A: an injected failing destination + quarantine-on-exhaustion, maxAttempts = 2.
    let src_a = tempfile::tempdir().unwrap();
    let failed_a = tempfile::tempdir().unwrap();
    write_file(&src_a.path().join("doomed.dat"), b"cannot be delivered");

    let mut cfg_a = InstanceCfg {
        id: "a".to_string(),
        enabled: true,
        ingress: ingress_immediate(src_a.path()),
        egress: vec![], // bypassed: build_with_dest supplies the destination
        schedule: ScheduleCfg::Immediate,
        completion: CompletionCfg {
            on_exhausted: OnExhausted::Quarantine,
            failed_dir: Some(failed_a.path().to_path_buf()),
            ..Default::default()
        },
        retry: Some(RetryCfg {
            base_delay_ms: Some(1),
            max_delay_ms: Some(1),
            give_up_after: None,
            max_attempts: Some(2),
        }),
        limits: None,
        topics: None,
    };
    cfg_a.completion.on_success = OnSuccess::Delete;

    let failing = FailingDest::new();
    let calls = failing.deliver_calls.clone();
    let dest_a: SharedDestination = Arc::new(failing);
    let inst_a = Instance::build_with_dest(
        cfg_a,
        &GlobalCfg::default(),
        store.clone(),
        Arc::new(Semaphore::new(16)),
        Arc::new(TokenBucket::unlimited()),
        None,
        dest_a,
        Events::disabled(),
    )
    .expect("instance A builds with injected dest");

    // Instance B: a healthy local destination sharing the same store — must be unaffected by A.
    let src_b = tempfile::tempdir().unwrap();
    let dst_b = tempfile::tempdir().unwrap();
    write_file(&src_b.path().join("fine.txt"), b"healthy payload");
    let inst_b = build(
        local_cfg(
            "b",
            src_b.path(),
            dst_b.path(),
            CompletionCfg {
                on_success: OnSuccess::Delete,
                ..Default::default()
            },
        ),
        store.clone(),
    );

    // A, tick 1: first attempt fails transiently → Failed (awaiting backoff), source untouched.
    inst_a.tick(1_000).await;
    let after1 = store.get("a", "doomed.dat").unwrap().unwrap();
    assert_eq!(after1.state, ItemState::Failed, "first failure schedules a retry");
    assert_eq!(after1.attempts, 1);
    assert!(src_a.path().join("doomed.dat").exists(), "Failed leaves the source in place");

    // A, tick 2 (past the backoff gate): promoted → re-attempted → maxAttempts hit → Quarantined.
    inst_a.tick(5_000).await;
    let after2 = store.get("a", "doomed.dat").unwrap().unwrap();
    assert_eq!(after2.state, ItemState::Quarantined);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2, "retried exactly twice");
    assert!(!src_a.path().join("doomed.dat").exists(), "quarantined (moved out of source)");
    assert_eq!(
        std::fs::read(failed_a.path().join("doomed.dat")).unwrap(),
        b"cannot be delivered"
    );

    // Error sidecar next to the quarantined file.
    let sidecar = failed_a.path().join("doomed.dat.error.json");
    let json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&sidecar).unwrap()).unwrap();
    assert_eq!(json["relpath"], "doomed.dat");
    assert_eq!(json["attempts"], 2);
    assert!(json["lastError"].as_str().unwrap().contains("transient"));
    assert_eq!(store.stats("a").unwrap().failed, 1);

    // Instance B is entirely unaffected: its file replicates cleanly.
    inst_b.tick(1_000).await;
    assert_eq!(std::fs::read(dst_b.path().join("fine.txt")).unwrap(), b"healthy payload");
    assert!(!src_b.path().join("fine.txt").exists());
    assert_eq!(store.get("b", "fine.txt").unwrap().unwrap().state, ItemState::Completed);
    assert_eq!(store.stats("b").unwrap().replicated, 1);
    // ...and A's failure did not touch B's stats.
    assert_eq!(store.stats("b").unwrap().failed, 0);
}

// ---- 4. crash safety: Verified-but-not-Completed recovers idempotently, with no re-delivery -------

#[tokio::test]
async fn crash_between_verified_and_completed_recovers_without_redelivery() {
    let workdir = tempfile::tempdir().unwrap();
    let db_path = workdir.path().join("state.db");
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

    // The bytes the (pre-crash) engine already delivered + verified at the destination key.
    const DELIVERED: &[u8] = b"the object delivered and verified before the crash";
    // Deliberately DIFFERENT current source bytes: a bug that re-delivered on recovery would
    // overwrite the destination with THESE, so an unchanged destination proves no re-delivery.
    const NEWER_SOURCE: &[u8] = b"NEWER source bytes that must never reach the destination";

    // --- Phase A: simulate the persisted "crashed at Verified" state, then drop the store. ---
    {
        let store = SqliteStore::open(&db_path).unwrap();
        write_file(&src.path().join("data.txt"), NEWER_SOURCE);
        write_file(&dst.path().join("data.txt"), DELIVERED); // object already live at its key
        store.upsert_ready("crash", "data.txt", DELIVERED.len() as u64, 0, 1).unwrap();
        // Write-ahead point of §13.2: Verified persisted before the source side effect.
        store.set_state("crash", "data.txt", ItemState::Verified, 2).unwrap();
        // `store` dropped here == the process crash (WAL-backed DB left on disk).
    }

    // --- Phase B: reopen the same DB with a fresh engine; recovery completes the item on start. ---
    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open(&db_path).unwrap());
    let inst = Arc::new(build(
        local_cfg(
            "crash",
            src.path(),
            dst.path(),
            CompletionCfg {
                on_success: OnSuccess::Delete,
                ..Default::default()
            },
        ),
        store.clone(),
    ));

    let cancel = CancellationToken::new();
    let handle = {
        let inst = inst.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move { inst.run(cancel).await })
    };

    // recover() runs before the first tick: it completes the Verified item (deletes the source).
    let mut recovered = false;
    for _ in 0..200 {
        if !src.path().join("data.txt").exists() {
            recovered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("run loop exits on cancel")
        .unwrap();

    assert!(recovered, "recovery should complete the Verified item on restart");
    // Idempotent completion: destination object is exactly what was delivered before the crash —
    // recovery did NOT re-deliver the (now different) source.
    assert_eq!(std::fs::read(dst.path().join("data.txt")).unwrap(), DELIVERED);
    assert!(!src.path().join("data.txt").exists(), "source completed (deleted)");
    assert_eq!(store.get("crash", "data.txt").unwrap().unwrap().state, ItemState::Completed);
    assert_eq!(store.stats("crash").unwrap().replicated, 1);
}

// ---- 5. bandwidth cap sanity --------------------------------------------------------------------

/// Deterministic token-bucket math (no real sleeps): a rate cap makes an over-burst request wait
/// exactly the deficit / rate.
#[test]
fn bandwidth_token_bucket_enforces_rate_deterministically() {
    assert_eq!(parse_byte_rate("10KB/s").unwrap(), 10_000);
    assert_eq!(parse_byte_rate("20MB/s").unwrap(), 20_000_000);

    let clock = Arc::new(ManualClock::new());
    let dyn_clock: Arc<dyn Clock> = clock.clone();
    let bucket = TokenBucket::new(parse_byte_rate("10KB/s").unwrap(), dyn_clock);

    // Full burst (capacity == rate) is free; the next 5 KB with no refill waits 0.5 s, carrying a
    // 5 KB token debt forward.
    assert_eq!(bucket.acquire(10_000), Duration::ZERO);
    assert_eq!(bucket.acquire(5_000), Duration::from_secs_f64(0.5));
    // A 1 s refill (+10 KB) repays the 5 KB debt and leaves 5 KB, so the next 5 KB is free.
    clock.advance(Duration::from_secs(1));
    assert_eq!(bucket.acquire(5_000), Duration::ZERO);
}

/// End-to-end: a per-instance bandwidth cap forces a real transfer to take at least the mandated
/// minimum time (a robust lower bound — a slower machine only waits longer).
#[tokio::test]
async fn per_instance_bandwidth_cap_enforces_minimum_transfer_time() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    // 3000 bytes at 2000 B/s: 2000 free from the initial burst, the remaining 1000 → ~0.5 s wait.
    write_file(&src.path().join("throttled.bin"), &vec![0x5au8; 3000]);

    let mut cfg = local_cfg(
        "bw",
        src.path(),
        dst.path(),
        CompletionCfg {
            on_success: OnSuccess::Delete,
            ..Default::default()
        },
    );
    cfg.limits = Some(LimitsCfg {
        max_concurrent_files: None,
        max_bandwidth: Some("2000".to_string()), // bare number = bytes/second
    });
    let store = store_in_memory();
    let inst = build(cfg, store.clone());

    let started = Instant::now();
    inst.tick(1_000).await;
    let elapsed = started.elapsed();

    assert!(
        elapsed >= Duration::from_millis(400),
        "bandwidth cap should force >= ~0.5s; took {elapsed:?}"
    );
    assert_eq!(std::fs::read(dst.path().join("throttled.bin")).unwrap(), vec![0x5au8; 3000]);
    assert_eq!(store.get("bw", "throttled.bin").unwrap().unwrap().state, ItemState::Completed);
}

// ---- 6. a global bandwidth cap is shared across instances (FR-REL-6) -----------------------------

/// Two instances share ONE global token bucket. After instance A drains the shared burst, instance
/// B's transfer must wait on the global deficit — proving the aggregate cap is genuinely shared
/// across instances, not merely per-instance.
#[tokio::test]
async fn global_bandwidth_cap_is_shared_across_instances() {
    let global_sem = Arc::new(Semaphore::new(16));
    // 2000 B/s shared; per-instance buckets are unlimited so only the global cap can throttle.
    let global_bw = Arc::new(TokenBucket::new(
        parse_byte_rate("2000").unwrap(),
        Arc::new(SystemClock),
    ));
    let store = store_in_memory();

    let mk = |id: &str| -> (Instance, tempfile::TempDir, tempfile::TempDir) {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        write_file(&src.path().join("f.bin"), &vec![0x11u8; 2000]);
        let cfg = local_cfg(
            id,
            src.path(),
            dst.path(),
            CompletionCfg { on_success: OnSuccess::Delete, ..Default::default() },
        );
        let inst = Instance::build(
            cfg,
            &GlobalCfg::default(),
            store.clone(),
            global_sem.clone(),
            global_bw.clone(),
            None,
            &DestDeps::default(),
            Events::disabled(),
        )
        .expect("instance builds with shared global bucket");
        (inst, src, dst)
    };

    let (inst_a, _sa, da) = mk("ga");
    let (inst_b, _sb, db) = mk("gb");

    // A drains the shared 2000-byte global burst (its 2000 bytes go through at ~no wait).
    inst_a.tick(1_000).await;
    // B now transfers 2000 bytes against a nearly-empty shared global bucket → ~1s deficit.
    let started = Instant::now();
    inst_b.tick(2_000).await;
    let elapsed = started.elapsed();

    assert!(
        elapsed >= Duration::from_millis(400),
        "shared global cap should make the 2nd instance wait; took {elapsed:?}"
    );
    assert_eq!(std::fs::read(da.path().join("f.bin")).unwrap(), vec![0x11u8; 2000]);
    assert_eq!(std::fs::read(db.path().join("f.bin")).unwrap(), vec![0x11u8; 2000]);
    assert_eq!(store.stats("ga").unwrap().replicated, 1);
    assert_eq!(store.stats("gb").unwrap().replicated, 1);
}

// ---- 7. completion fires ONLY after a passing verify (FR-CMP-3/4) --------------------------------

/// The core no-loss invariant at the pipeline level: `deliver` succeeds but `verify` FAILS. The
/// source must be preserved, the item retried (Failed, not Completed), and `stats.replicated` must
/// stay 0 — a regression that deleted the source regardless of the verify result would be caught here
/// (the dest-unit `verify_detects_checksum_mismatch` only checks `verify()`'s return, not the
/// worker's response to it).
#[tokio::test]
async fn completion_does_not_fire_when_verify_fails() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    write_file(&src.path().join("v.dat"), b"delivered but will fail verify");

    let cfg = InstanceCfg {
        id: "vf".to_string(),
        enabled: true,
        ingress: ingress_immediate(src.path()),
        egress: vec![], // injected dest
        schedule: ScheduleCfg::Immediate,
        completion: CompletionCfg {
            on_success: OnSuccess::Delete, // if completion wrongly fired, the source would vanish
            verify: Verify::Checksum,
            ..Default::default()
        },
        // Keep it in Failed (retry) rather than Exhausted so we observe the pre-completion state.
        retry: Some(RetryCfg {
            base_delay_ms: Some(60_000),
            max_delay_ms: Some(60_000),
            give_up_after: None,
            max_attempts: None,
        }),
        limits: None,
        topics: None,
    };
    let dest = Arc::new(VerifyFailingDest::new(dst.path()));
    let calls = dest.deliver_calls.clone();
    let dest: SharedDestination = dest;
    let store = store_in_memory();
    let inst = Instance::build_with_dest(
        cfg,
        &GlobalCfg::default(),
        store.clone(),
        Arc::new(Semaphore::new(16)),
        Arc::new(TokenBucket::unlimited()),
        None,
        dest,
        Events::disabled(),
    )
    .expect("instance builds with verify-failing dest");

    inst.tick(1_000).await;

    // Source preserved (NOT deleted) — completion never ran because verify failed.
    assert!(src.path().join("v.dat").exists(), "verify failure must preserve the source (no loss)");
    let item = store.get("vf", "v.dat").unwrap().unwrap();
    assert_eq!(item.state, ItemState::Failed, "verify failure → retry, not Completed");
    assert_eq!(item.attempts, 1);
    assert!(item.last_error.as_deref().unwrap().contains("integrity"));
    assert_eq!(store.stats("vf").unwrap().replicated, 0, "nothing counted as replicated");
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1, "delivered once, then held");
}

// ---- 8. write-ahead ordering: crash between the source side effect and the Completed write --------

/// Guards the load-bearing §13.2 ordering with a REAL interrupt (not synthetically-injected state): a
/// store that fails the `Completed` write exactly once simulates a crash *after* the source delete but
/// *before* the durable `Completed`. The item is left `Verified` (source already gone), and crash
/// recovery on restart re-derives the correct terminal `Completed` state with the object neither lost
/// nor re-delivered — exactly once. Flipping the persist-Verified / source-side-effect order would
/// leave the item unrecoverable and fail this test.
#[tokio::test]
async fn crash_after_source_delete_before_completed_recovers_exactly_once() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    const PAYLOAD: &[u8] = b"exactly-once across a crash between delete and Completed";
    write_file(&src.path().join("once.dat"), PAYLOAD);

    let store: Arc<dyn StateStore> =
        Arc::new(FailCompletedOnceStore::new(SqliteStore::open_in_memory().unwrap()));
    let inst = Arc::new(build(
        local_cfg(
            "once",
            src.path(),
            dst.path(),
            CompletionCfg { on_success: OnSuccess::Delete, ..Default::default() },
        ),
        store.clone(),
    ));

    // Phase A — one tick: deliver + verify succeed, the source is deleted, but persisting Completed
    // fails (the injected crash). The item is left Verified: source gone, dest written, not counted.
    inst.tick(1_000).await;
    assert_eq!(std::fs::read(dst.path().join("once.dat")).unwrap(), PAYLOAD, "delivered");
    assert!(!src.path().join("once.dat").exists(), "source side effect ran before Completed persist");
    assert_eq!(
        store.get("once", "once.dat").unwrap().unwrap().state,
        ItemState::Verified,
        "Verified persisted before the source side effect; Completed not yet written"
    );
    assert_eq!(store.stats("once").unwrap().replicated, 0, "not counted before Completed");

    // Phase B — restart: recovery re-verifies the destination (still present) and completes the item.
    let cancel = CancellationToken::new();
    let handle = {
        let inst = inst.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move { inst.run(cancel).await })
    };
    let mut completed = false;
    for _ in 0..200 {
        if store.get("once", "once.dat").unwrap().map(|i| i.state) == Some(ItemState::Completed) {
            completed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("run loop exits on cancel")
        .unwrap();

    assert!(completed, "recovery must re-derive Completed from the Verified crash point");
    // No re-delivery (dest content unchanged) and exactly-once accounting.
    assert_eq!(std::fs::read(dst.path().join("once.dat")).unwrap(), PAYLOAD);
    assert_eq!(store.stats("once").unwrap().replicated, 1, "counted exactly once across the crash");
}

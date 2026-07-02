//! # file-replicator — P6 multi-destination fan-out integration tests
//!
//! Drives the fan-out engine (DESIGN §20-B, FR-EGR-2/3) through its public library surface — real
//! [`Instance`]s over `tempfile` directories and a real durable [`SqliteStore`] — proving the three
//! load-bearing invariants that make multi-destination replication crash-safe and loss/duplication
//! free:
//!
//! 1. **fan-out + single completion** — a file replicated to two LOCAL destinations lands byte- and
//!    checksum-identical at both, and the source is archived (or deleted) **exactly once**, counted
//!    exactly once in stats;
//! 2. **partial failure blocks completion, but doesn't lose the succeeded copy** — with one destination
//!    permanently failing, the item never completes (source retained, not lost) even though the OTHER
//!    destination already has a verified copy; a later retry re-delivers only the still-failing
//!    destination — the already-`Verified` one is skipped (idempotent, no double delivery);
//! 3. **crash mid-fan-out recovers minimally** — a store left with ONE destination already
//!    `DestPhase::Verified` and the item claimed (`InProgress`) when the process died is recovered, on
//!    restart, by delivering to ONLY the still-pending destination — the already-verified one is never
//!    re-delivered — and the item completes exactly once.
//!
//! No external infra: every destination here is `local` (real temp-dir I/O) or an in-process fake: no
//! network, no S3/SFTP/etc. credentials required.

use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use file_replicator::config::{
    CompletionCfg, EgressCfg, GlobReadiness, GlobalCfg, IngressCfg, InstanceCfg, LocalEgress,
    OnSuccess, ReadinessCfg, ScheduleCfg, Verify,
};
use file_replicator::dest::{DestDeps, Destination, LocalDest, SharedDestination};
use file_replicator::domain::{
    Checksum, Delivered, DestPhase, ItemState, ProgressSink, ResumeState, WorkItem,
};
use file_replicator::error::{ReplError, Result as ReplResult};
use file_replicator::events::Events;
use file_replicator::instance::Instance;
use file_replicator::integrity::{hash_reader, Algorithm};
use file_replicator::ratelimit::{Bandwidth, TokenBucket};
use file_replicator::state::{SqliteStore, StateStore};

// ---- harness helpers (mirrors tests/p1_engine.rs) -----------------------------------------------

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

fn store_in_memory() -> Arc<dyn StateStore> {
    Arc::new(SqliteStore::open_in_memory().unwrap())
}

fn write_file(path: &Path, bytes: &[u8]) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, bytes).unwrap();
}

/// The CRC32C content checksum of a file on disk (the default transfer-integrity hash, DESIGN §13.1),
/// so a test can assert two destinations are not just byte-equal by luck but integrity-verified equal.
fn checksum_of(path: &Path) -> Checksum {
    let mut f = std::fs::File::open(path).unwrap();
    let (_, cksum) = hash_reader(&mut f, Algorithm::Crc32c).unwrap();
    cksum
}

/// A `local` destination wrapper that (a) reports a caller-chosen `kind()` — so a test can pin a
/// destination's fan-out **label** deterministically instead of relying on the `label_destinations`
/// disambiguation rule — and (b) counts `deliver` invocations, the load-bearing assertion for
/// "already-`Verified` destinations are never re-delivered".
struct NamedLocalDest {
    kind: &'static str,
    inner: LocalDest,
    deliver_calls: Arc<AtomicU32>,
}

impl NamedLocalDest {
    fn build(kind: &'static str, dst: &Path) -> (SharedDestination, Arc<AtomicU32>) {
        let calls = Arc::new(AtomicU32::new(0));
        let dest: SharedDestination = Arc::new(NamedLocalDest {
            kind,
            inner: LocalDest::new(
                &LocalEgress {
                    path: dst.to_path_buf(),
                    fsync: false,
                },
                Algorithm::Crc32c,
            ),
            deliver_calls: calls.clone(),
        });
        (dest, calls)
    }
}

#[async_trait]
impl Destination for NamedLocalDest {
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
    ) -> ReplResult<Delivered> {
        self.deliver_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.deliver(item, resume, progress, bw).await
    }
    async fn verify(&self, item: &WorkItem, d: &Delivered, p: Verify) -> ReplResult<()> {
        self.inner.verify(item, d, p).await
    }
    async fn abort(&self, item: &WorkItem, resume: &ResumeState) -> ReplResult<()> {
        self.inner.abort(item, resume).await
    }
}

/// A destination whose `deliver` always fails with a **permanent** error (access-denied-shaped, not
/// retriable) — the injected "one destination can never succeed" seam for the partial-failure test.
/// Counts calls so a later manual retry can assert it retried (unlike the already-`Verified` sibling).
struct AlwaysPermanentFail {
    deliver_calls: Arc<AtomicU32>,
}

impl AlwaysPermanentFail {
    fn build() -> (SharedDestination, Arc<AtomicU32>) {
        let calls = Arc::new(AtomicU32::new(0));
        let dest: SharedDestination = Arc::new(AlwaysPermanentFail {
            deliver_calls: calls.clone(),
        });
        (dest, calls)
    }
}

#[async_trait]
impl Destination for AlwaysPermanentFail {
    fn kind(&self) -> &'static str {
        "always-fail"
    }
    fn supports_resume(&self) -> bool {
        false
    }
    async fn deliver(
        &self,
        _item: &WorkItem,
        _resume: Option<ResumeState>,
        _progress: &ProgressSink,
        _bw: &Bandwidth,
    ) -> ReplResult<Delivered> {
        self.deliver_calls.fetch_add(1, Ordering::SeqCst);
        Err(ReplError::Permanent("access denied (injected)".into()))
    }
    async fn verify(&self, _item: &WorkItem, _d: &Delivered, _p: Verify) -> ReplResult<()> {
        Ok(())
    }
    async fn abort(&self, _item: &WorkItem, _resume: &ResumeState) -> ReplResult<()> {
        Ok(())
    }
}

// ---- 1. fan-out to two LOCAL destinations: both correct, source completes exactly once ------------

#[tokio::test]
async fn fanout_two_local_dests_both_receive_content_source_completes_once() {
    let src = tempfile::tempdir().unwrap();
    let dst1 = tempfile::tempdir().unwrap();
    let dst2 = tempfile::tempdir().unwrap();
    let archive = tempfile::tempdir().unwrap();

    const PAYLOAD: &[u8] = b"fan out to every destination before the source is released";
    write_file(&src.path().join("f.bin"), PAYLOAD);

    let cfg = InstanceCfg {
        id: "fanout".to_string(),
        enabled: true,
        ingress: ingress_immediate(src.path()),
        egress: vec![
            EgressCfg::Local(LocalEgress {
                path: dst1.path().to_path_buf(),
                fsync: false,
            }),
            EgressCfg::Local(LocalEgress {
                path: dst2.path().to_path_buf(),
                fsync: false,
            }),
        ],
        schedule: ScheduleCfg::Immediate,
        completion: CompletionCfg {
            on_success: OnSuccess::Archive,
            archive_dir: Some(archive.path().to_path_buf()),
            verify: Verify::Checksum,
            ..Default::default()
        },
        retry: None,
        limits: None,
        topics: None,
    };

    let store = store_in_memory();
    let inst = Instance::build(
        cfg,
        &GlobalCfg::default(),
        store.clone(),
        Arc::new(Semaphore::new(16)),
        Arc::new(TokenBucket::unlimited()),
        None,
        &DestDeps::default(),
        Events::disabled(),
    )
    .expect("N=2 fan-out instance builds (the v1 len==1 restriction is lifted, P6)");

    inst.tick(1_000).await;

    // Both destinations received the exact bytes...
    assert_eq!(std::fs::read(dst1.path().join("f.bin")).unwrap(), PAYLOAD);
    assert_eq!(std::fs::read(dst2.path().join("f.bin")).unwrap(), PAYLOAD);
    // ...and are checksum-identical to the archived source (not just byte-equal by luck: the
    // completion path only fires after a passing per-destination checksum verify, DESIGN §13.1).
    let expected = checksum_of(&archive.path().join("f.bin"));
    assert_ne!(expected, Checksum::None);
    assert_eq!(checksum_of(&dst1.path().join("f.bin")), expected);
    assert_eq!(checksum_of(&dst2.path().join("f.bin")), expected);

    // Source archived exactly once (gone from ingress, present in archive — not duplicated/lost).
    assert!(!src.path().join("f.bin").exists(), "source removed from ingress");
    assert_eq!(std::fs::read(archive.path().join("f.bin")).unwrap(), PAYLOAD);

    // Completed exactly once — the aggregate item, not per-destination — and per-destination
    // bookkeeping is cleared once the file is durably Completed (DESIGN §20-B).
    let item = store.get("fanout", "f.bin").unwrap().unwrap();
    assert_eq!(item.state, ItemState::Completed);
    assert_eq!(
        store.stats("fanout").unwrap().replicated,
        1,
        "counted exactly once, not once per destination"
    );
    assert!(
        store.dest_states("fanout", "f.bin").unwrap().is_empty(),
        "per-destination bookkeeping cleared on completion"
    );
}

// ---- 2. one destination permanently fails: no completion, the succeeded copy is not lost -----------

#[tokio::test]
async fn one_permanently_failing_dest_blocks_completion_but_preserves_the_other_copy() {
    let src = tempfile::tempdir().unwrap();
    let dst_good = tempfile::tempdir().unwrap();

    const PAYLOAD: &[u8] = b"one destination can never deliver this";
    write_file(&src.path().join("p.bin"), PAYLOAD);

    let cfg = InstanceCfg {
        id: "partial".to_string(),
        enabled: true,
        ingress: ingress_immediate(src.path()),
        egress: vec![], // both destinations injected below
        schedule: ScheduleCfg::Immediate,
        // Default completion: onExhausted = retainInPlace (source kept, item marked Retained) —
        // proves "does NOT complete" without conflating it with the (also-valid) quarantine outcome.
        completion: CompletionCfg {
            on_success: OnSuccess::Delete,
            ..Default::default()
        },
        retry: None,
        limits: None,
        topics: None,
    };

    let (dest_good, calls_good) = NamedLocalDest::build("good", dst_good.path());
    let (dest_bad, calls_bad) = AlwaysPermanentFail::build();

    let store = store_in_memory();
    let inst = Instance::build_with_dests(
        cfg,
        &GlobalCfg::default(),
        store.clone(),
        Arc::new(Semaphore::new(16)),
        Arc::new(TokenBucket::unlimited()),
        None,
        vec![dest_good, dest_bad],
        Events::disabled(),
    )
    .expect("fan-out instance with an injected failing destination builds");

    inst.tick(1_000).await;

    // The item can NEVER complete while one destination is permanently exhausted (FR-EGR-3(d)) — the
    // source must be preserved (retained), not deleted/lost, even though the OTHER destination already
    // has a verified copy.
    let item = store.get("partial", "p.bin").unwrap().unwrap();
    assert_eq!(item.state, ItemState::Retained, "never completes while a destination is exhausted");
    assert!(src.path().join("p.bin").exists(), "source retained, not lost, on partial failure");
    assert_eq!(store.stats("partial").unwrap().replicated, 0, "not counted as replicated");
    assert_eq!(store.stats("partial").unwrap().failed, 1);

    // The healthy destination's already-delivered, already-verified copy is left intact — partial
    // success is not thrown away just because a sibling destination can never succeed.
    assert_eq!(std::fs::read(dst_good.path().join("p.bin")).unwrap(), PAYLOAD);
    assert_eq!(calls_good.load(Ordering::SeqCst), 1, "delivered exactly once");
    assert_eq!(calls_bad.load(Ordering::SeqCst), 1, "the failing destination attempted exactly once");

    let mut states = store.dest_states("partial", "p.bin").unwrap();
    states.sort_by(|a, b| a.dest.cmp(&b.dest));
    assert_eq!(states.len(), 2);
    assert_eq!(states[0].dest, "always-fail");
    assert_eq!(states[0].phase, DestPhase::Exhausted);
    assert_eq!(states[1].dest, "good");
    assert_eq!(states[1].phase, DestPhase::Verified);

    // --- Idempotent re-delivery on a manual retry: the already-Verified destination must be SKIPPED
    // (never re-delivered), while the still-failing one is retried (and gives up again, permanently).
    store.set_state("partial", "p.bin", ItemState::Ready, 2_000).unwrap();
    inst.tick(2_000).await;

    assert_eq!(
        calls_good.load(Ordering::SeqCst),
        1,
        "already-Verified destination must NOT be re-delivered on retry"
    );
    assert_eq!(
        calls_bad.load(Ordering::SeqCst),
        2,
        "the still-failing destination is retried (a human might have fixed it)"
    );
    let item = store.get("partial", "p.bin").unwrap().unwrap();
    assert_eq!(item.state, ItemState::Retained, "still cannot complete");
    assert!(src.path().join("p.bin").exists(), "source still retained, not lost, after the retry");
    assert_eq!(std::fs::read(dst_good.path().join("p.bin")).unwrap(), PAYLOAD, "good copy unchanged");
    // The file's failure is counted EXACTLY ONCE across its lifetime — re-exhausting the same
    // already-Exhausted destination on a manual retry must not re-increment filesFailed.
    assert_eq!(
        store.stats("partial").unwrap().failed,
        1,
        "failed counted once, not re-counted when the retry re-exhausts the same destination"
    );
}

// ---- 3. crash mid-fan-out: recovery delivers ONLY the still-pending destination, once -------------

#[tokio::test]
async fn crash_mid_fanout_recovers_by_delivering_only_the_remaining_destination() {
    let workdir = tempfile::tempdir().unwrap();
    let db_path = workdir.path().join("state.db");
    let src = tempfile::tempdir().unwrap();
    let dst_a = tempfile::tempdir().unwrap(); // already delivered + verified before the "crash"
    let dst_b = tempfile::tempdir().unwrap(); // still pending when the process died

    const PAYLOAD: &[u8] = b"A got this before the crash; B is still owed it";
    write_file(&src.path().join("f.bin"), PAYLOAD);
    // Destination A's object is already live at its stable key (delivered + verified pre-crash).
    write_file(&dst_a.path().join("f.bin"), PAYLOAD);

    // --- Phase A: persist the "crashed mid-fan-out" state directly, then drop the store. ---
    // This is the exact write-ahead point DESIGN §20-B relies on: the claim (Ready → InProgress) and
    // destination A's independent DestPhase::Verified are BOTH durable; destination B's first attempt
    // never even started (no dest_state row at all — the implicit-Pending case, §14.2 comment).
    {
        let store = SqliteStore::open(&db_path).unwrap();
        store.upsert_ready("crash-fanout", "f.bin", PAYLOAD.len() as u64, 0, 1).unwrap();
        store.set_state("crash-fanout", "f.bin", ItemState::InProgress, 2).unwrap();
        store
            .set_dest_phase("crash-fanout", "f.bin", "a", DestPhase::Verified, 2)
            .unwrap();
        // `store` dropped here == the process crash (WAL-backed DB left on disk).
    }

    // --- Phase B: reopen the same DB with a fresh engine; recovery must deliver ONLY to B. ---
    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open(&db_path).unwrap());
    let (dest_a, calls_a) = NamedLocalDest::build("a", dst_a.path());
    let (dest_b, calls_b) = NamedLocalDest::build("b", dst_b.path());

    let cfg = InstanceCfg {
        id: "crash-fanout".to_string(),
        enabled: true,
        ingress: ingress_immediate(src.path()),
        egress: vec![], // dests injected below
        schedule: ScheduleCfg::Immediate,
        completion: CompletionCfg {
            on_success: OnSuccess::Delete,
            ..Default::default()
        },
        retry: None,
        limits: None,
        topics: None,
    };
    let inst = Arc::new(
        Instance::build_with_dests(
            cfg,
            &GlobalCfg::default(),
            store.clone(),
            Arc::new(Semaphore::new(16)),
            Arc::new(TokenBucket::unlimited()),
            None,
            vec![dest_a, dest_b],
            Events::disabled(),
        )
        .expect("recovered fan-out instance builds"),
    );

    let cancel = CancellationToken::new();
    let handle = {
        let inst = inst.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move { inst.run(cancel).await })
    };

    // recover() (InProgress → Ready) runs before the first tick; that tick's claim then re-runs the
    // fan-out pipeline, skipping the already-Verified destination A and delivering only to B.
    let mut completed = false;
    for _ in 0..200 {
        if store.get("crash-fanout", "f.bin").unwrap().map(|i| i.state) == Some(ItemState::Completed) {
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

    assert!(completed, "recovery should complete the item after delivering only to the pending destination");
    assert_eq!(
        calls_a.load(Ordering::SeqCst),
        0,
        "the already-Verified destination A must NOT be re-delivered (no double delivery)"
    );
    assert_eq!(calls_b.load(Ordering::SeqCst), 1, "the pending destination B delivered exactly once");
    assert_eq!(std::fs::read(dst_a.path().join("f.bin")).unwrap(), PAYLOAD, "A's object unchanged");
    assert_eq!(std::fs::read(dst_b.path().join("f.bin")).unwrap(), PAYLOAD, "B received the file");
    assert!(!src.path().join("f.bin").exists(), "source completed (deleted) exactly once");
    assert_eq!(store.stats("crash-fanout").unwrap().replicated, 1, "counted exactly once across the crash");
    assert!(
        store.dest_states("crash-fanout", "f.bin").unwrap().is_empty(),
        "per-destination bookkeeping cleared on completion"
    );
}

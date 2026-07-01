//! # file-replicator — S3 destination integration tests against floci (DESIGN §11, §21)
//!
//! Drives the real [`S3Dest`](file_replicator::dest::S3Dest) (through the public
//! [`Destination`](file_replicator::dest::Destination) trait + [`build_destination`] factory) against
//! the local floci AWS emulator at `http://localhost:4566` — path-style, region `us-east-1`, static
//! test creds, **Transfer Acceleration OFF** (a real-AWS-only feature). Every test **self-skips** when
//! floci is unreachable (a fast TCP probe → `eprintln!` + return), so `cargo test` stays green on CI
//! where floci is absent and the 90% coverage gate is met by the unit tests alone (the SDK-call seam in
//! `dest/s3/client.rs` is excluded from the gate; these live tests exercise it end-to-end when floci is
//! up, but never gate).
//!
//! Scope (the whole P2 floci-integration cut, one file — no duplicate harness):
//!   * `small_file_single_put_*`            — the small-file `PutObject` path: object present, bytes,
//!     and the flexible CRC32C checksum verified across all three `verify` policies.
//!   * `large_file_multipart_*`             — the multipart path (>threshold, several parts): content +
//!     checksum + the persisted `{uploadId, completed[]}` resume checkpoint.
//!   * `resume_skips_completed_parts`       — throttle a multipart upload, **interrupt** it after ≥1
//!     part checkpoints, **restart** from the checkpoint, and assert it completes correctly AND streams
//!     ONLY the missing parts (final progress == object size, not committed+size).
//!   * `resume_after_swept_mpu_*`           — the `uploadId` is aborted out-of-band (simulating the
//!     bucket's incomplete-MPU lifecycle sweeping it during a long outage); resume must NOT fail
//!     permanently — it detects `NoSuchUpload`, re-initiates a fresh MPU, and completes (DESIGN §13.4).
//!   * `idempotent_redelivery_overwrite`    — re-delivering to the same `relpath` overwrites (FR-REL-4).
//!   * `abort_is_idempotent_on_unknown_upload` — give-up abort tolerates a gone `uploadId`.
//!   * `recover_verified_s3_*`              — `Worker::recover()` re-verifies the live S3 object by the
//!     persisted checksum before the source side effect (intact → complete; same-size corruption →
//!     requeue, source preserved).
//!
//! floci capability notes (verified 2026-07-01 against floci 1.5.17 community):
//!   * bucket create, `PutObject` with a flexible CRC32C trailing checksum, and `HeadObject` echoing
//!     that checksum (`ChecksumMode=Enabled`) — **supported**, so the full checksum-verify path runs.
//!   * multipart (`CreateMultipartUpload`/`UploadPart` with per-part CRC32C/`CompleteMultipartUpload`/
//!     `AbortMultipartUpload`) — **supported**.
//!   * Transfer Acceleration — **not exercised** (real-AWS-only; forced off whenever `endpointUrl` is
//!     set, see `s3_client_params`). If a future floci build drops the flexible-checksum echo,
//!     `S3Dest::verify` downgrades to size+existence (logged) and the checksum comparison stays covered
//!     by the `dest/s3/mod.rs` unit tests.

#![cfg(feature = "dest-s3")]

use std::net::TcpStream;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use file_replicator::config::{
    Collision, CompletionCfg, EgressCfg, MultipartCfg, OnExhausted, OnSuccess, S3Egress, Verify,
};
use file_replicator::dest::{build_destination, DestDeps, SharedDestination};
use file_replicator::domain::{Delivered, ItemState, ProgressSink, ResumeState, WorkItem};
use file_replicator::instance::worker::{RetryPolicy, Worker};
use file_replicator::ratelimit::{Bandwidth, SystemClock, TokenBucket};
use file_replicator::state::{SqliteStore, StateStore};

const ENDPOINT: &str = "http://localhost:4566";
const REGION: &str = "us-east-1";
const MIB: usize = 1024 * 1024;
/// Instance id used by every test's `WorkItem` + store lookups.
const INSTANCE: &str = "s3it";

/// True when floci's S3 port answers a TCP connect within a short timeout.
fn floci_up() -> bool {
    let addr = "127.0.0.1:4566".parse().expect("addr");
    TcpStream::connect_timeout(&addr, Duration::from_millis(400)).is_ok()
}

/// Skip guard: run the test body only when floci is reachable, else print + return (test passes).
macro_rules! require_floci {
    () => {
        if !floci_up() {
            eprintln!("skipping S3 floci test: floci unreachable at {ENDPOINT}");
            return;
        }
    };
}

/// A fake credential service returning static floci test creds for any `{"$secret":"…"}` reference —
/// exercises the `S3Dest` static-credentials path (DESIGN §11.5) without mutating process env.
struct FlociCreds;
impl ggcommons::credentials::CredentialService for FlociCreds {
    fn get(&self, _n: &str) -> ggcommons::Result<Option<ggcommons::credentials::Secret>> {
        Ok(None)
    }
    fn get_version(
        &self,
        _n: &str,
        _v: &str,
    ) -> ggcommons::Result<Option<ggcommons::credentials::Secret>> {
        Ok(None)
    }
    fn exists(&self, _n: &str) -> ggcommons::Result<bool> {
        Ok(true)
    }
    fn list(&self, _p: &str) -> ggcommons::Result<Vec<ggcommons::credentials::SecretMeta>> {
        Ok(vec![])
    }
    fn versions(&self, _n: &str) -> ggcommons::Result<Vec<String>> {
        Ok(vec![])
    }
    fn put(
        &self,
        _n: &str,
        _v: &[u8],
        _o: ggcommons::credentials::PutOptions,
    ) -> ggcommons::Result<String> {
        Ok("v1".into())
    }
    fn delete(&self, _n: &str) -> ggcommons::Result<bool> {
        Ok(false)
    }
    fn get_aws_credentials(
        &self,
        _name: &str,
    ) -> ggcommons::Result<Option<ggcommons::credentials::AwsCredentials>> {
        Ok(Some(ggcommons::credentials::AwsCredentials {
            access_key_id: "test".into(),
            secret_access_key: "test".into(),
            session_token: None,
            expiry: None,
        }))
    }
}

/// An S3 egress pointed at floci: path-style + region resolved from `endpoint_url`; static creds via
/// the fake vault (`$secret`); Transfer Acceleration OFF (forced off whenever an endpoint is set).
fn floci_egress(bucket: &str, prefix: &str, multipart: MultipartCfg) -> S3Egress {
    S3Egress {
        bucket: bucket.into(),
        prefix: prefix.into(),
        region: Some(REGION.into()),
        endpoint_url: Some(ENDPOINT.into()),
        credentials: Some(serde_json::json!({ "$secret": "floci" })),
        storage_class: None,
        sse: None,
        kms_key_id: None,
        accelerate: false,
        unsigned_payload: false,
        checksum_algorithm: None, // CRC32C default
        multipart,
    }
}

/// Build the S3 destination via the real factory, optionally wired to a durable store (needed for the
/// multipart resume checkpoint).
fn build_s3(
    bucket: &str,
    prefix: &str,
    multipart: MultipartCfg,
    store: Option<Arc<dyn StateStore>>,
) -> SharedDestination {
    let egress = EgressCfg::S3(Box::new(floci_egress(bucket, prefix, multipart)));
    let mut deps = DestDeps::default().with_credentials(Some(Arc::new(FlociCreds)));
    if let Some(s) = store {
        deps = deps.with_store(s);
    }
    build_destination(&egress, &deps).expect("build s3 dest")
}

/// Run an AWS CLI subcommand against floci with static creds; returns stdout (panics on failure).
fn aws(args: &[&str]) -> Vec<u8> {
    let out = aws_output(args);
    assert!(
        out.status.success(),
        "aws {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

/// Run an AWS CLI subcommand against floci, returning the raw output (caller decides on the status) —
/// used where a non-success is acceptable (e.g. aborting an already-consumed multipart upload).
fn aws_output(args: &[&str]) -> std::process::Output {
    Command::new("aws")
        .args(["--endpoint-url", ENDPOINT, "--region", REGION])
        .args(args)
        .env("AWS_ACCESS_KEY_ID", "test")
        .env("AWS_SECRET_ACCESS_KEY", "test")
        .env("AWS_DEFAULT_REGION", REGION)
        .output()
        .expect("run aws cli")
}

/// Create a bucket (BucketAlreadyOwnedByYou is fine — names are per-run unique regardless).
fn make_bucket(name: &str) {
    let _ = aws_output(&["s3", "mb", &format!("s3://{name}")]);
}

/// Download an object's bytes via the CLI (independent read-back path from the SDK under test).
fn get_object(bucket: &str, key: &str) -> Vec<u8> {
    let tmp = std::env::temp_dir().join(format!("frs3-get-{}", key.replace('/', "_")));
    aws(&[
        "s3api",
        "get-object",
        "--bucket",
        bucket,
        "--key",
        key,
        tmp.to_str().unwrap(),
    ]);
    let bytes = std::fs::read(&tmp).expect("read downloaded object");
    let _ = std::fs::remove_file(&tmp);
    bytes
}

fn work_item(src_root: &Path, rel: &str) -> WorkItem {
    let abs = src_root.join(rel);
    let size = std::fs::metadata(&abs).map(|m| m.len()).unwrap_or(0);
    WorkItem {
        instance: INSTANCE.into(),
        relpath: rel.into(),
        abs_source: abs,
        state: ItemState::InProgress,
        size,
        discovered_at: 0,
        attempts: 0,
        next_attempt_at: 0,
        last_error: None,
        bytes_done: 0,
        updated_at: 0,
    }
}

/// A per-run unique suffix so parallel tests never collide on bucket/key names.
fn unique(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("frtest-{tag}-{nanos:x}")
}

/// Deterministic, non-trivial content of `n` bytes (so a corrupted/short object is detectable).
fn payload(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i % 251) as u8).collect()
}

/// A bandwidth governor capped at `mb_per_sec` MiB/s on the per-instance bucket (global unlimited) —
/// slows the first multipart pass so the resume tests can interrupt it mid-flight.
fn capped_bandwidth(mb_per_sec: u64) -> Bandwidth {
    Bandwidth::new(
        Arc::new(TokenBucket::new(mb_per_sec * MIB as u64, Arc::new(SystemClock))),
        Arc::new(TokenBucket::unlimited()),
    )
}

/// Count of completed parts recorded in a persisted S3 resume checkpoint.
fn completed_parts(store: &Arc<dyn StateStore>, rel: &str) -> Option<usize> {
    store
        .load_resume(INSTANCE, rel, "s3")
        .unwrap()
        .and_then(|rs| rs.token["s3"]["completed"].as_array().map(|a| a.len()))
}

/// Start a throttled multipart delivery in a task and interrupt it once 1..=3 parts have checkpointed;
/// returns the persisted resume checkpoint and how many parts were completed before the interrupt.
async fn interrupt_after_partial_parts(
    dest: &SharedDestination,
    item: &WorkItem,
    store: &Arc<dyn StateStore>,
    rel: &str,
) -> (ResumeState, usize) {
    let dest_c = dest.clone();
    let item_c = item.clone();
    let bw = capped_bandwidth(5); // ~1 s per 5 MiB part → the poll below reliably catches a part
    let handle = tokio::spawn(async move {
        let _ = dest_c
            .deliver(&item_c, None, &ProgressSink::noop(), &bw)
            .await;
    });

    let mut interrupted_after = 0usize;
    for _ in 0..800 {
        // up to ~20 s
        tokio::time::sleep(Duration::from_millis(25)).await;
        if let Some(n) = completed_parts(store, rel) {
            if (1..=3).contains(&n) {
                interrupted_after = n;
                break;
            }
        }
        if handle.is_finished() {
            break;
        }
    }
    handle.abort();
    let _ = handle.await; // join the cancelled task (JoinError::Cancelled expected)

    assert!(
        interrupted_after >= 1,
        "expected to interrupt after >=1 completed part (floci too fast? cap too high)"
    );
    let resume = store
        .load_resume(INSTANCE, rel, "s3")
        .unwrap()
        .expect("a partial multipart checkpoint was persisted before the interrupt");
    (resume, interrupted_after)
}

// ---- small file: single PutObject ---------------------------------------------------------------

#[tokio::test]
async fn small_file_single_put_delivers_verifies_and_round_trips() {
    require_floci!();
    let bucket = unique("single");
    make_bucket(&bucket);

    let src = tempfile::tempdir().unwrap();
    let data = payload(64 * 1024 + 7); // ~64 KiB, well under the 16 MiB multipart threshold
    std::fs::write(src.path().join("a.bin"), &data).unwrap();

    let dest = build_s3(&bucket, "in/", MultipartCfg::default(), None);
    let item = work_item(src.path(), "a.bin");
    let delivered = dest
        .deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
        .await
        .expect("deliver small");
    assert_eq!(delivered.bytes, data.len() as u64);

    // All three verify policies pass against the live object (checksum path exercised end-to-end).
    dest.verify(&item, &delivered, Verify::Checksum)
        .await
        .expect("verify checksum");
    dest.verify(&item, &delivered, Verify::Size)
        .await
        .expect("verify size");
    dest.verify(&item, &delivered, Verify::None)
        .await
        .expect("verify exists");

    // Subtree preserved under the prefix → key `in/a.bin`; byte-for-byte round trip.
    assert_eq!(get_object(&bucket, "in/a.bin"), data, "object bytes match source");
}

#[tokio::test]
async fn small_file_unsigned_payload_put_round_trips() {
    // Exercises the UNSIGNED-PAYLOAD single-PUT path live (`customize().disable_payload_signing()`,
    // DESIGN §11.2): the SigV4 payload-hash pass is skipped, integrity still guaranteed by the flexible
    // CRC32C checksum sent alongside. Proves the config knob is wired to real SDK behavior, not a no-op.
    require_floci!();
    let bucket = unique("unsigned");
    make_bucket(&bucket);

    let src = tempfile::tempdir().unwrap();
    let data = payload(48 * 1024 + 3);
    std::fs::write(src.path().join("u.bin"), &data).unwrap();

    let mut egress = floci_egress(&bucket, "in/", MultipartCfg::default());
    egress.unsigned_payload = true;
    let deps = DestDeps::default().with_credentials(Some(Arc::new(FlociCreds)));
    let dest = build_destination(&EgressCfg::S3(Box::new(egress)), &deps).expect("build s3 dest");
    let item = work_item(src.path(), "u.bin");

    let delivered = dest
        .deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
        .await
        .expect("unsigned-payload PUT delivers");
    dest.verify(&item, &delivered, Verify::Checksum)
        .await
        .expect("verify checksum (unsigned payload)");
    assert_eq!(
        get_object(&bucket, "in/u.bin"),
        data,
        "unsigned-payload object bytes match source"
    );
}

// ---- large file: multipart with multiple parts --------------------------------------------------

#[tokio::test]
async fn large_file_multipart_delivers_and_round_trips() {
    require_floci!();
    let bucket = unique("multi");
    make_bucket(&bucket);

    // Force multipart with the S3-minimum 5 MiB part size over a ~17 MiB file → 4 parts.
    let mp = MultipartCfg {
        threshold_bytes: Some(5 * MIB as u64),
        part_size_bytes: Some(5 * MIB as u64),
        max_concurrent_parts: Some(3),
    };
    let data = payload(17 * MIB);
    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("big.bin"), &data).unwrap();

    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
    let dest = build_s3(&bucket, "deep/nested", mp, Some(store.clone()));
    let item = work_item(src.path(), "big.bin");
    let delivered = dest
        .deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
        .await
        .expect("deliver multipart");
    assert_eq!(delivered.bytes, data.len() as u64);

    dest.verify(&item, &delivered, Verify::Checksum)
        .await
        .expect("verify checksum (multipart)");

    // Subtree preserved: key = deep/nested/big.bin. Whole-object round trip.
    assert_eq!(
        get_object(&bucket, "deep/nested/big.bin"),
        data,
        "multipart object bytes match"
    );

    // The multipart transfer persisted its resume checkpoint (uploadId + all completed parts).
    assert_eq!(
        completed_parts(&store, "big.bin"),
        Some(4),
        "all 4 parts recorded in the resume checkpoint"
    );
}

// ---- resume: interrupt after >=1 part, restart, do NOT re-upload completed parts ----------------

#[tokio::test]
async fn resume_skips_completed_parts() {
    require_floci!();
    let bucket = unique("resume");
    make_bucket(&bucket);

    // 5 MiB parts over a 17 MiB file → 4 parts, uploaded ONE at a time so the interrupt lands on a
    // clean part boundary.
    let mp = MultipartCfg {
        threshold_bytes: Some(5 * MIB as u64),
        part_size_bytes: Some(5 * MIB as u64),
        max_concurrent_parts: Some(1),
    };
    let data = payload(17 * MIB);
    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("r.bin"), &data).unwrap();

    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
    let dest = build_s3(&bucket, "r/", mp, Some(store.clone()));
    let item = work_item(src.path(), "r.bin");

    // --- Pass 1: throttle + interrupt once 1..=3 parts have checkpointed.
    let (resume, interrupted_after) =
        interrupt_after_partial_parts(&dest, &item, &store, "r.bin").await;
    let committed = resume.bytes_committed;
    assert_eq!(
        committed,
        interrupted_after as u64 * 5 * MIB as u64,
        "committed bytes == the completed 5 MiB parts"
    );
    assert!(
        committed > 0 && committed < data.len() as u64,
        "precondition: interrupted mid-transfer (committed {committed} of {})",
        data.len()
    );

    // --- Pass 2: RESTART from the persisted checkpoint. The progress accumulator starts at
    // `committed` and adds ONLY the bytes actually streamed. Track the MAX (final) cumulative total:
    //   * reading only the MISSING parts lands the final exactly at data.len();
    //   * re-reading a completed part would OVERSHOOT to committed + data.len().
    // Asserting `max == data.len()` therefore proves completed parts were skipped — the earlier
    // `min > committed` form was tautological (the accumulator can never dip below its committed seed).
    let max_seen = Arc::new(AtomicU64::new(0));
    let ms = max_seen.clone();
    let sink = ProgressSink::new(move |n| {
        ms.fetch_max(n, Ordering::SeqCst);
    });
    let delivered = dest
        .deliver(&item, Some(resume), &sink, &Bandwidth::unlimited())
        .await
        .expect("resume completes without re-uploading completed parts");
    assert_eq!(delivered.bytes, data.len() as u64);

    let max = max_seen.load(Ordering::SeqCst);
    assert_eq!(
        max,
        data.len() as u64,
        "resume must stream ONLY the missing parts: final progress {max} != object size {} \
         (a re-read completed part would overshoot to committed+size = {})",
        data.len(),
        committed + data.len() as u64
    );

    // The finished object is correct end-to-end (part 1 from floci's stored part + the resumed parts).
    dest.verify(&item, &delivered, Verify::Checksum)
        .await
        .expect("verify checksum after resume");
    assert_eq!(
        get_object(&bucket, "r/r.bin"),
        data,
        "resumed multipart object is byte-for-byte correct"
    );
}

// ---- resume after the uploadId was swept out-of-band → restart fresh (DESIGN §13.4) -------------

#[tokio::test]
async fn resume_after_swept_mpu_restarts_with_fresh_upload() {
    // A long outage can let the bucket's incomplete-MPU lifecycle abort the in-flight upload before we
    // resume. The persisted uploadId is then gone; UploadPart/Complete return NoSuchUpload. The backend
    // MUST NOT classify that as a permanent failure (which would quarantine a byte-correct-in-source
    // file) — it detects the gone upload, re-initiates a fresh CreateMultipartUpload, and completes.
    require_floci!();
    let bucket = unique("swept");
    make_bucket(&bucket);

    let mp = MultipartCfg {
        threshold_bytes: Some(5 * MIB as u64),
        part_size_bytes: Some(5 * MIB as u64),
        max_concurrent_parts: Some(1),
    };
    let data = payload(17 * MIB);
    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("s.bin"), &data).unwrap();

    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
    let dest = build_s3(&bucket, "s/", mp, Some(store.clone()));
    let item = work_item(src.path(), "s.bin");

    // Pass 1: throttle + interrupt after a partial upload, capturing the persisted uploadId.
    let (resume, _n) = interrupt_after_partial_parts(&dest, &item, &store, "s.bin").await;
    let upload_id = resume.token["s3"]["uploadId"]
        .as_str()
        .expect("uploadId persisted in the checkpoint")
        .to_string();

    // Simulate the lifecycle sweeping the incomplete MPU (tolerant: if floci already completed it, the
    // abort is a harmless no-op and pass 2 exercises the lost-ACK object-recovery branch instead).
    let _ = aws_output(&[
        "s3api",
        "abort-multipart-upload",
        "--bucket",
        &bucket,
        "--key",
        "s/s.bin",
        "--upload-id",
        &upload_id,
    ]);

    // Pass 2: resume against the now-gone uploadId → must restart fresh and complete, not fail.
    let delivered = dest
        .deliver(&item, Some(resume), &ProgressSink::noop(), &Bandwidth::unlimited())
        .await
        .expect("a swept uploadId must restart fresh (not fail permanently)");
    assert_eq!(delivered.bytes, data.len() as u64);
    dest.verify(&item, &delivered, Verify::Checksum)
        .await
        .expect("verify checksum after restart");
    assert_eq!(
        get_object(&bucket, "s/s.bin"),
        data,
        "restarted multipart object is byte-for-byte correct"
    );
}

// ---- idempotent re-delivery: stable key overwrite (FR-REL-4) ------------------------------------

#[tokio::test]
async fn idempotent_redelivery_overwrite() {
    require_floci!();
    let bucket = unique("idem");
    make_bucket(&bucket);

    let src = tempfile::tempdir().unwrap();
    let dest = build_s3(&bucket, "in/", MultipartCfg::default(), None);

    // First delivery of content v1 to relpath "dup.bin" → key in/dup.bin.
    let v1 = payload(20_000);
    std::fs::write(src.path().join("dup.bin"), &v1).unwrap();
    let item1 = work_item(src.path(), "dup.bin");
    let d1 = dest
        .deliver(&item1, None, &ProgressSink::noop(), &Bandwidth::unlimited())
        .await
        .expect("deliver v1");
    dest.verify(&item1, &d1, Verify::Checksum).await.expect("verify v1");
    assert_eq!(get_object(&bucket, "in/dup.bin"), v1);

    // Re-deliver DIFFERENT content (different length) to the SAME relpath → same key → overwrite.
    let v2 = payload(37_000);
    assert_ne!(v1, v2);
    std::fs::write(src.path().join("dup.bin"), &v2).unwrap();
    let item2 = work_item(src.path(), "dup.bin");
    let d2 = dest
        .deliver(&item2, None, &ProgressSink::noop(), &Bandwidth::unlimited())
        .await
        .expect("deliver v2 (overwrite)");
    assert_eq!(d2.bytes, v2.len() as u64);
    dest.verify(&item2, &d2, Verify::Checksum)
        .await
        .expect("verify v2");
    assert_eq!(
        get_object(&bucket, "in/dup.bin"),
        v2,
        "stable key overwrote identically with the new content"
    );
}

// ---- abort idempotence ---------------------------------------------------------------------------

#[tokio::test]
async fn abort_is_idempotent_on_unknown_upload() {
    require_floci!();
    let bucket = unique("abort");
    make_bucket(&bucket);

    let dest = build_s3(&bucket, "", MultipartCfg::default(), None);
    let item = work_item(std::env::temp_dir().as_path(), "nope.bin");
    // A resume token referencing a non-existent uploadId → AbortMultipartUpload returns NoSuchUpload,
    // which the S3 backend treats as idempotent success (DESIGN §11.3).
    let resume = ResumeState {
        bytes_committed: 0,
        token: serde_json::json!({
            "s3": { "uploadId": "does-not-exist", "key": "nope.bin", "partSize": 5242880, "completed": [] }
        }),
    };
    dest.abort(&item, &resume).await.expect("abort tolerates NoSuchUpload");

    // A resume without an S3 token is a no-op.
    dest.abort(&item, &ResumeState::default())
        .await
        .expect("abort no-op without token");
}

// ---- checksum-persist-recovery (DESIGN §13.2) for the S3 backend --------------------------------
//
// The worker persists the full `Delivered` (including the S3 object checksum) as the `Verified`
// write-ahead checkpoint, so crash recovery re-verifies the S3 object by CHECKSUM — not just size —
// before running the source side effect. These tests drive the real `Worker::recover()` against the
// live S3 object: the intact object completes (source deleted, counted once); a same-size CORRUPTED
// object is caught by the checksum and requeued to `Ready` (source preserved), where the interim
// size-only check would have silently completed and destroyed the only copy.

/// A `delete`-on-success completion that verifies by checksum (recovery uses `Verify::Checksum`).
fn recovery_completion() -> CompletionCfg {
    CompletionCfg {
        on_success: OnSuccess::Delete,
        archive_dir: None,
        on_exhausted: OnExhausted::RetainInPlace,
        failed_dir: None,
        on_collision: Collision::Overwrite,
        verify: Verify::Checksum,
    }
}

/// A worker wired to the S3 `dest` + the shared `store`, sourcing from `ingress_root`.
fn s3_worker(store: Arc<dyn StateStore>, dest: SharedDestination, ingress_root: &Path) -> Worker {
    Worker::new(
        INSTANCE.to_string(),
        store,
        dest,
        recovery_completion(),
        ingress_root.to_path_buf(),
        Bandwidth::unlimited(),
        RetryPolicy::default(),
        None,
    )
}

/// Seed the store as a crashed-at-`Verified` item carrying the full `Delivered` write-ahead
/// checkpoint under the `s3` resume key (exactly what `Worker::save_verified_checkpoint` persists).
fn seed_verified(store: &Arc<dyn StateStore>, rel: &str, delivered: &Delivered) {
    store.upsert_ready(INSTANCE, rel, delivered.bytes, 0, 1).unwrap();
    store.set_state(INSTANCE, rel, ItemState::Verified, 2).unwrap();
    store
        .save_resume(
            INSTANCE,
            rel,
            "s3",
            &ResumeState {
                bytes_committed: delivered.bytes,
                token: serde_json::json!({ "verified": serde_json::to_value(delivered).unwrap() }),
            },
        )
        .unwrap();
}

#[tokio::test]
async fn recover_verified_s3_completes_when_object_intact() {
    require_floci!();
    let bucket = unique("recov-ok");
    make_bucket(&bucket);

    let src = tempfile::tempdir().unwrap();
    let data = b"authentic S3 recovery payload - intact object".repeat(4);
    std::fs::write(src.path().join("r.txt"), &data).unwrap();

    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
    let dest = build_s3(&bucket, "in/", MultipartCfg::default(), Some(store.clone()));
    let item = work_item(src.path(), "r.txt");

    let delivered = dest
        .deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
        .await
        .expect("deliver");
    seed_verified(&store, "r.txt", &delivered);

    // Recovery re-verifies by checksum against the live (intact) object → completes.
    let worker = s3_worker(store.clone(), dest.clone(), src.path());
    worker.recover(100).await.unwrap();

    assert_eq!(
        store.get(INSTANCE, "r.txt").unwrap().unwrap().state,
        ItemState::Completed,
        "intact object re-verified by checksum → completes"
    );
    assert!(!src.path().join("r.txt").exists(), "source deleted on completed recovery");
    assert!(
        store.load_resume(INSTANCE, "r.txt", "s3").unwrap().is_none(),
        "checkpoint cleared after Completed"
    );
    assert_eq!(store.stats(INSTANCE).unwrap().replicated, 1, "counted exactly once");
}

#[tokio::test]
async fn recover_verified_s3_requeues_on_same_size_corruption() {
    // The whole point of persisting the checksum: an S3 object CORRUPTED during downtime — same
    // length, different bytes — must be caught by the object-checksum re-verify on recovery and
    // requeued, where the interim size-only check would have silently completed + deleted the source.
    require_floci!();
    let bucket = unique("recov-corrupt");
    make_bucket(&bucket);

    let src = tempfile::tempdir().unwrap();
    let data = b"the authentic S3 recovery bytes -- exactly!!".to_vec();
    std::fs::write(src.path().join("r.txt"), &data).unwrap();

    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
    let dest = build_s3(&bucket, "in/", MultipartCfg::default(), Some(store.clone()));
    let item = work_item(src.path(), "r.txt");

    let delivered = dest
        .deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
        .await
        .expect("deliver");
    // Sanity: the checksum verify passes against the freshly-delivered (uncorrupted) object.
    dest.verify(&item, &delivered, Verify::Checksum)
        .await
        .expect("verify passes before corruption");

    // Corrupt the object IN PLACE: identical length, different content, PUT with a CRC32C so
    // HeadObject echoes a checksum (a size check alone cannot tell it from the original).
    let corrupt = b"CORRUPTED S3 recovery bytes -- same length!!".to_vec();
    assert_eq!(
        corrupt.len(),
        data.len(),
        "corruption must preserve size so only the checksum catches it"
    );
    let cpath = std::env::temp_dir().join(unique("corruptbody"));
    std::fs::write(&cpath, &corrupt).unwrap();
    aws(&[
        "s3api",
        "put-object",
        "--bucket",
        &bucket,
        "--key",
        "in/r.txt",
        "--body",
        cpath.to_str().unwrap(),
        "--checksum-algorithm",
        "CRC32C",
    ]);
    let _ = std::fs::remove_file(&cpath);

    seed_verified(&store, "r.txt", &delivered);

    // Recovery: the checksum re-verify catches the same-size corruption → requeue, source preserved.
    let worker = s3_worker(store.clone(), dest.clone(), src.path());
    worker.recover(100).await.unwrap();

    assert_eq!(
        store.get(INSTANCE, "r.txt").unwrap().unwrap().state,
        ItemState::Ready,
        "same-size corruption caught by checksum → requeue, never complete"
    );
    assert!(src.path().join("r.txt").exists(), "source preserved — no data loss");
    assert!(
        store.load_resume(INSTANCE, "r.txt", "s3").unwrap().is_none(),
        "stale checkpoint cleared on requeue"
    );
    assert_eq!(store.stats(INSTANCE).unwrap().replicated, 0, "not counted as replicated");
}

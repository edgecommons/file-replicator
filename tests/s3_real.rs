//! # file-replicator — REAL S3 integration test (opt-in), covering the checksum-verify COMPARISON
//!
//! floci does not echo an S3 object checksum the way real AWS S3 does, so `tests/s3_floci.rs` never
//! actually exercises the `verify(Checksum)` comparison against a real S3-returned checksum. This test
//! fills that gap: it drives the real [`S3Dest`] against **real AWS S3** using the ambient AWS
//! credential chain (`~/.aws`, env, IMDS, …) and asserts the round-trip + checksum verify.
//!
//! It **self-skips** unless `FR_S3_REAL_BUCKET` is set, so `cargo test` (and CI) stay green without AWS.
//! Run (Windows/host, with working `aws` creds):
//!   FR_S3_REAL_BUCKET=my-bucket cargo test --features dest-s3 --test s3_real -- --nocapture
#![cfg(feature = "dest-s3")]

use std::path::Path;
use std::sync::Arc;

use file_replicator::config::{
    Collision, CompletionCfg, EgressCfg, MultipartCfg, OnExhausted, OnSuccess, S3Egress, Verify,
};
use file_replicator::dest::{build_destination, DestDeps, SharedDestination};
use file_replicator::domain::{ItemState, ProgressSink, WorkItem};
use file_replicator::instance::worker::{RetryPolicy, Worker};
use file_replicator::ratelimit::{Bandwidth, TokenBucket};
use file_replicator::state::{SqliteStore, StateStore};

fn real_egress(bucket: &str, prefix: &str, multipart: MultipartCfg) -> S3Egress {
    S3Egress {
        bucket: bucket.into(),
        prefix: prefix.into(),
        region: Some("us-east-1".into()),
        endpoint_url: None, // real AWS
        credentials: None,  // ambient default provider chain
        storage_class: None,
        sse: None,
        kms_key_id: None,
        accelerate: false,
        unsigned_payload: false,
        checksum_algorithm: None, // CRC32C default
        multipart,
    }
}

fn work_item(root: &Path, rel: &str) -> WorkItem {
    let abs = root.join(rel);
    let size = std::fs::metadata(&abs).map(|m| m.len()).unwrap_or(0);
    WorkItem {
        instance: "s3real".into(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_s3_checksum_verify_roundtrip() {
    let Ok(bucket) = std::env::var("FR_S3_REAL_BUCKET") else {
        eprintln!("skipping real-S3 test: set FR_S3_REAL_BUCKET to run");
        return;
    };
    let prefix =
        std::env::var("FR_S3_REAL_PREFIX").unwrap_or_else(|_| "file-replicator-verify-test/".into());

    let dir = tempfile::tempdir().unwrap();
    let rel = "verify-probe.bin";
    let content: Vec<u8> = (0..1_048_576u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(dir.path().join(rel), &content).unwrap();

    let dest: SharedDestination = build_destination(
        &EgressCfg::S3(Box::new(real_egress(&bucket, &prefix, MultipartCfg::default()))),
        &DestDeps::default(),
    )
    .expect("build real S3 dest");
    let item = work_item(dir.path(), rel);
    let bw = Bandwidth::new(
        Arc::new(TokenBucket::unlimited()),
        Arc::new(TokenBucket::unlimited()),
    );

    let delivered = dest
        .deliver(&item, None, &ProgressSink::noop(), &bw)
        .await
        .expect("deliver to real S3");
    eprintln!(
        "DELIVERED bytes={} handle={}",
        delivered.bytes, delivered.handle
    );

    let vr = dest.verify(&item, &delivered, Verify::Checksum).await;
    eprintln!("VERIFY(Checksum) => {vr:?}");
    vr.expect("verify(Checksum) against REAL S3 must pass");
    eprintln!("PASS: real-S3 checksum verify round-trip");
}

/// The full engine path the Greengrass device runs: Worker::process_item = deliver → verify(Checksum)
/// → complete(archive). Reproduces (or refutes) the on-device "S3 uploads but source never archives".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_s3_worker_archives_after_checksum_verify() {
    let Ok(bucket) = std::env::var("FR_S3_REAL_BUCKET") else {
        eprintln!("skipping real-S3 worker test: set FR_S3_REAL_BUCKET to run");
        return;
    };
    let prefix = std::env::var("FR_S3_REAL_PREFIX")
        .unwrap_or_else(|_| "file-replicator-verify-test/".into());

    let src = tempfile::tempdir().unwrap();
    let archive = tempfile::tempdir().unwrap();
    let rel = "worker-probe.bin";
    let content: Vec<u8> = (0..1_048_576u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(src.path().join(rel), &content).unwrap();

    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
    let dest = build_destination(
        &EgressCfg::S3(Box::new(real_egress(&bucket, &prefix, MultipartCfg::default()))),
        &DestDeps::default().with_store(store.clone()),
    )
    .expect("build real S3 dest");
    let completion = CompletionCfg {
        on_success: OnSuccess::Archive,
        archive_dir: Some(archive.path().to_path_buf()),
        on_exhausted: OnExhausted::RetainInPlace,
        failed_dir: None,
        on_collision: Collision::Suffix,
        verify: Verify::Checksum,
    };
    let worker = Worker::new(
        "s3real".into(),
        store.clone(),
        dest,
        completion,
        src.path().to_path_buf(),
        Bandwidth::unlimited(),
        RetryPolicy::default(),
        None,
    );

    let item = work_item(src.path(), rel);
    store.upsert_ready("s3real", rel, item.size, 0, 1).unwrap();
    let final_state = worker.process_item(&item, 1_000).await;
    let src_exists = src.path().join(rel).exists();
    let archived = archive.path().join(rel).exists();
    eprintln!("WORKER final_state={final_state:?} src_exists={src_exists} archived={archived}");

    assert_eq!(
        final_state,
        ItemState::Completed,
        "worker must complete via checksum-verify + archive against real S3"
    );
    assert!(!src_exists, "source must be moved out of ingress");
    assert!(archived, "source must land in the archive dir");
    eprintln!("PASS: worker archived after real-S3 checksum verify");
}

/// The MULTIPART path (>threshold, several parts) through the full Worker with `verify: checksum` +
/// archive, against real S3. S3 returns a COMPOSITE (`base64-N`) checksum for multipart objects that
/// does NOT equal the whole-file CRC32C, so this exercises the code's multipart-verify special-casing
/// — a path neither floci (no checksum echo) nor the single-PUT tests cover.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_s3_worker_multipart_archives_after_checksum_verify() {
    let Ok(bucket) = std::env::var("FR_S3_REAL_BUCKET") else {
        eprintln!("skipping real-S3 multipart test: set FR_S3_REAL_BUCKET to run");
        return;
    };
    let prefix = std::env::var("FR_S3_REAL_PREFIX")
        .unwrap_or_else(|_| "file-replicator-verify-test/".into());

    let src = tempfile::tempdir().unwrap();
    let archive = tempfile::tempdir().unwrap();
    let rel = "worker-mp-probe.bin";
    // 20 MiB with an 8 MiB threshold/part → 3-part multipart upload.
    let content: Vec<u8> = (0..(20 * 1024 * 1024u32)).map(|i| (i % 251) as u8).collect();
    std::fs::write(src.path().join(rel), &content).unwrap();
    let mp = MultipartCfg {
        threshold_bytes: Some(8 * 1024 * 1024),
        part_size_bytes: Some(8 * 1024 * 1024),
        max_concurrent_parts: Some(4),
    };

    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
    let dest = build_destination(
        &EgressCfg::S3(Box::new(real_egress(&bucket, &prefix, mp))),
        &DestDeps::default().with_store(store.clone()),
    )
    .expect("build real S3 dest");
    let completion = CompletionCfg {
        on_success: OnSuccess::Archive,
        archive_dir: Some(archive.path().to_path_buf()),
        on_exhausted: OnExhausted::RetainInPlace,
        failed_dir: None,
        on_collision: Collision::Suffix,
        verify: Verify::Checksum,
    };
    let worker = Worker::new(
        "s3real".into(),
        store.clone(),
        dest,
        completion,
        src.path().to_path_buf(),
        Bandwidth::unlimited(),
        RetryPolicy::default(),
        None,
    );

    let item = work_item(src.path(), rel);
    store.upsert_ready("s3real", rel, item.size, 0, 1).unwrap();
    let final_state = worker.process_item(&item, 2_000).await;
    let src_exists = src.path().join(rel).exists();
    let archived = archive.path().join(rel).exists();
    eprintln!("WORKER(multipart) final_state={final_state:?} src_exists={src_exists} archived={archived}");

    assert_eq!(
        final_state,
        ItemState::Completed,
        "multipart worker must complete via checksum-verify + archive against real S3"
    );
    assert!(!src_exists && archived, "multipart source must be archived");
    eprintln!("PASS: multipart worker archived after real-S3 checksum verify");
}

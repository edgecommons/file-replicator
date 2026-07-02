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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use file_replicator::config::{
    Collision, CompletionCfg, EgressCfg, MultipartCfg, OnExhausted, OnSuccess, S3Egress, Verify,
};
use file_replicator::dest::s3::{checksum_to_b64, handle_s3_checksum};
use file_replicator::dest::{build_destination, DestDeps, SharedDestination};
use file_replicator::domain::{ItemState, ProgressSink, WorkItem};
use file_replicator::integrity::{Algorithm, Hasher};
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

/// The base64 CRC32C that real S3 stores as the whole-object flexible checksum for a *single-PUT*
/// object of `content`, computed independently of the delivery path. Asserting the delivery handle
/// carries exactly this (rather than only that `verify()` returns `Ok`) is what makes these tests guard
/// the single-pass regression class: if the `PutObject` response ever stopped echoing a checksum,
/// `build_handle` would store `s3Checksum: None` and `verify(Checksum)` would silently downgrade to
/// size+existence and still pass — but this equality would fail.
fn expected_crc32c_b64(content: &[u8]) -> String {
    let mut h = Hasher::new(Algorithm::Crc32c);
    h.update(content);
    checksum_to_b64(&h.finish()).expect("crc32c digest encodes to base64")
}

/// Assert a *multipart* delivery handle carries S3's composite `<base64>-N` object checksum with the
/// expected part count and a non-empty base64 component (the multipart analogue of
/// [`expected_crc32c_b64`]: the composite is not the whole-file CRC32C, so we check its shape, not an
/// exact value). A silent `s3Checksum: None` would fail the initial `expect`.
fn assert_composite_checksum(handle: &serde_json::Value, expected_parts: u32) {
    let stored = handle_s3_checksum(handle)
        .expect("multipart delivery handle must carry the S3 composite object checksum");
    let (b64, n) = stored
        .rsplit_once('-')
        .unwrap_or_else(|| panic!("multipart checksum must be the composite '<base64>-N' shape: {stored}"));
    assert!(!b64.is_empty(), "composite checksum must carry a non-empty base64 part: {stored}");
    assert_eq!(
        n,
        expected_parts.to_string(),
        "composite checksum part count must match the plan ({expected_parts} parts): {stored}"
    );
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

/// Real S3 has strong read-after-write consistency, but running these four `multi_thread` S3 tests plus
/// the 25 ms RSS sampler thread all at once was observed to flake (a `HeadObject` 404 right after a
/// successful `CompleteMultipartUpload` — a parallel-run artifact, not a production defect). Serialize
/// the opt-in suite so each test has the endpoint to itself. The guard is acquired only *after* each
/// test's `FR_S3_REAL_BUCKET` skip check, so the no-bucket (CI) path never contends. `tokio::sync::Mutex`
/// (held across `.await`) rather than `std::sync::Mutex` so `clippy::await_holding_lock` stays clean.
fn real_s3_serial() -> &'static tokio::sync::Mutex<()> {
    static M: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    M.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_s3_checksum_verify_roundtrip() {
    let Ok(bucket) = std::env::var("FR_S3_REAL_BUCKET") else {
        eprintln!("skipping real-S3 test: set FR_S3_REAL_BUCKET to run");
        return;
    };
    let _serial = real_s3_serial().lock().await;
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

    // Guard the single-pass regression class directly: the handle must carry the actual S3 object
    // checksum, equal to the independently computed base64 CRC32C — not merely let verify() pass (which
    // would also pass on a silent `s3Checksum: None` downgrade to size+existence).
    let stored = handle_s3_checksum(&delivered.handle)
        .expect("single-PUT delivery handle must carry the S3 object checksum");
    assert_eq!(
        stored,
        expected_crc32c_b64(&content),
        "single-PUT stored object checksum must equal the independently computed base64 CRC32C"
    );

    let vr = dest.verify(&item, &delivered, Verify::Checksum).await;
    eprintln!("VERIFY(Checksum) => {vr:?}");
    vr.expect("verify(Checksum) against REAL S3 must pass");
    eprintln!("PASS: real-S3 checksum verify round-trip (handle checksum {stored})");
}

/// The single-pass **trailing** checksum's riskiest composition (DESIGN §11.2/§11.4): unsigned-PUT
/// (`disable_payload_signing`) combined with a flexible-checksum request over a *streaming* body, which
/// the SDK sends as `STREAMING-UNSIGNED-PAYLOAD-TRAILER` on a real `https://` endpoint. Confirms the
/// combination is not just "not rejected" but produces a checksum `verify(Checksum)` actually accepts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_s3_unsigned_payload_trailer_checksum_verify_roundtrip() {
    let Ok(bucket) = std::env::var("FR_S3_REAL_BUCKET") else {
        eprintln!("skipping real-S3 unsigned-trailer test: set FR_S3_REAL_BUCKET to run");
        return;
    };
    let _serial = real_s3_serial().lock().await;
    let prefix =
        std::env::var("FR_S3_REAL_PREFIX").unwrap_or_else(|_| "file-replicator-verify-test/".into());

    let dir = tempfile::tempdir().unwrap();
    let rel = "unsigned-trailer-probe.bin";
    let content: Vec<u8> = (0..1_048_576u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(dir.path().join(rel), &content).unwrap();

    let mut egress = real_egress(&bucket, &prefix, MultipartCfg::default());
    egress.unsigned_payload = true;
    let dest: SharedDestination = build_destination(
        &EgressCfg::S3(Box::new(egress)),
        &DestDeps::default(),
    )
    .expect("build real S3 dest (unsigned payload)");
    let item = work_item(dir.path(), rel);
    let bw = Bandwidth::new(
        Arc::new(TokenBucket::unlimited()),
        Arc::new(TokenBucket::unlimited()),
    );

    let delivered = dest
        .deliver(&item, None, &ProgressSink::noop(), &bw)
        .await
        .expect("unsigned-payload deliver to real S3");
    eprintln!(
        "DELIVERED (unsigned-payload trailer) bytes={} handle={}",
        delivered.bytes, delivered.handle
    );

    // Same regression guard for the unsigned-PUT (STREAMING-UNSIGNED-PAYLOAD-TRAILER) composition: the
    // trailing checksum the SDK sends over the streamed body must round-trip into the handle as the exact
    // base64 CRC32C of the source, not just satisfy a downgradeable verify().
    let stored = handle_s3_checksum(&delivered.handle)
        .expect("unsigned-payload single-PUT delivery handle must carry the S3 object checksum");
    assert_eq!(
        stored,
        expected_crc32c_b64(&content),
        "unsigned-payload single-PUT object checksum must equal the independently computed base64 CRC32C"
    );

    let vr = dest.verify(&item, &delivered, Verify::Checksum).await;
    eprintln!("VERIFY(Checksum) (unsigned-payload trailer) => {vr:?}");
    vr.expect("verify(Checksum) against REAL S3 must pass for unsigned-payload + trailing checksum");
    eprintln!("PASS: real-S3 unsigned-payload trailing-checksum round-trip (handle checksum {stored})");
}

/// The full engine path the Greengrass device runs: Worker::process_item = deliver → verify(Checksum)
/// → complete(archive). Reproduces (or refutes) the on-device "S3 uploads but source never archives".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_s3_worker_archives_after_checksum_verify() {
    let Ok(bucket) = std::env::var("FR_S3_REAL_BUCKET") else {
        eprintln!("skipping real-S3 worker test: set FR_S3_REAL_BUCKET to run");
        return;
    };
    let _serial = real_s3_serial().lock().await;
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
    let _serial = real_s3_serial().lock().await;
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

/// Peak-RSS proof that the multipart upload body streams bounded chunks off disk instead of
/// buffering a whole part (or the whole file) in memory (the P2-review finding: "read_range buffers
/// the whole part/small-file into memory" — measured ~500 MB peak RSS on-device for 2x1GiB @ 8x16MiB
/// parts). A background sampler thread polls *this process's* RSS via `sysinfo` while a large
/// multipart upload (several concurrent parts) runs against real S3, and asserts the observed peak
/// stays well under the old whole-part-buffering bound (`part_size * max_concurrent_parts`) — if the
/// body still materialized a whole part per in-flight task, peak RSS would sit at or above that bound;
/// bounded-chunk streaming keeps it a small fraction of it regardless of part size/file size.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_s3_multipart_upload_peak_rss_is_bounded_not_part_buffered() {
    let Ok(bucket) = std::env::var("FR_S3_REAL_BUCKET") else {
        eprintln!("skipping real-S3 peak-RSS test: set FR_S3_REAL_BUCKET to run");
        return;
    };
    let _serial = real_s3_serial().lock().await;
    let prefix =
        std::env::var("FR_S3_REAL_PREFIX").unwrap_or_else(|_| "file-replicator-verify-test/".into());

    // NOTE (measurement caveat): `sysinfo` reports Windows working-set RSS, which the OS can trim under
    // memory pressure — on a RAM-constrained host a buffering regression could be trimmed out of the
    // working set and this probe could false-pass. It is a valid discriminator only on an un-pressured
    // host (verified here with a large margin: tens of MiB observed vs the 128 MiB bound).
    const PART_SIZE: u64 = 16 * 1024 * 1024; // 16 MiB
    const CONCURRENCY: usize = 8;
    const FILE_SIZE: u64 = 256 * 1024 * 1024; // 256 MiB → 16 parts, 2 full concurrency rounds
    let whole_part_buffering_bound_bytes = PART_SIZE * CONCURRENCY as u64; // 128 MiB

    let src = tempfile::tempdir().unwrap();
    let rel = "peak-rss-probe.bin";
    write_pattern_file(&src.path().join(rel), FILE_SIZE);

    let mp = MultipartCfg {
        threshold_bytes: Some(PART_SIZE),
        part_size_bytes: Some(PART_SIZE),
        max_concurrent_parts: Some(CONCURRENCY),
    };
    let dest: SharedDestination = build_destination(
        &EgressCfg::S3(Box::new(real_egress(&bucket, &prefix, mp))),
        &DestDeps::default(),
    )
    .expect("build real S3 dest");
    let item = work_item(src.path(), rel);
    let bw = Bandwidth::new(
        Arc::new(TokenBucket::unlimited()),
        Arc::new(TokenBucket::unlimited()),
    );

    // Background sampler: refresh-and-record this process's RSS at a steady cadence for the duration
    // of the upload. `AtomicU64` for the running max (bytes), `AtomicBool` to stop the thread once the
    // upload (+ one final sample) completes.
    let peak_bytes = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = {
        let peak_bytes = peak_bytes.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
            let pid = Pid::from_u32(std::process::id());
            let mut sys = System::new();
            let refresh = ProcessRefreshKind::nothing().with_memory();
            loop {
                sys.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), true, refresh);
                if let Some(p) = sys.process(pid) {
                    let mem = p.memory(); // bytes (sysinfo 0.30+)
                    peak_bytes.fetch_max(mem, Ordering::Relaxed);
                }
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
        })
    };

    let delivered = dest
        .deliver(&item, None, &ProgressSink::noop(), &bw)
        .await
        .expect("deliver large multipart file to real S3");

    // Guard the multipart single-pass regression class: each part's trailing checksum must round-trip
    // into the handle as S3's composite `<base64>-N` object checksum (a silent per-part `None` would
    // leave the handle without one and downgrade verify()). 256 MiB / 16 MiB parts → 16 parts.
    assert_composite_checksum(&delivered.handle, (FILE_SIZE / PART_SIZE) as u32);

    stop.store(true, Ordering::Relaxed);
    sampler.join().expect("rss sampler thread");
    let peak = peak_bytes.load(Ordering::Relaxed);
    let peak_mb = peak as f64 / (1024.0 * 1024.0);
    let bound_mb = whole_part_buffering_bound_bytes as f64 / (1024.0 * 1024.0);
    eprintln!(
        "PEAK RSS during {}MiB multipart upload ({}MiB parts x {} concurrency) = {peak_mb:.1} MiB \
         (whole-part-buffering bound would be >= {bound_mb:.1} MiB)",
        FILE_SIZE / (1024 * 1024),
        PART_SIZE / (1024 * 1024),
        CONCURRENCY,
    );

    // Verify + clean up regardless of the assertion outcome below (best-effort delete).
    let vr = dest.verify(&item, &delivered, Verify::Checksum).await;
    eprintln!("VERIFY(Checksum) => {vr:?}");
    let key = format!("{prefix}{rel}");
    delete_object_best_effort(&bucket, &key).await;
    vr.expect("verify(Checksum) against REAL S3 must pass");

    assert!(
        peak > 0,
        "sampler never observed a non-zero RSS reading; the measurement is broken, not the code"
    );
    assert!(
        peak_bytes.load(Ordering::Relaxed) < whole_part_buffering_bound_bytes,
        "peak RSS ({peak_mb:.1} MiB) must stay well under the whole-part-buffering bound \
         ({bound_mb:.1} MiB = {}MiB part x {CONCURRENCY} concurrent) — a regression back to buffering \
         a whole part per in-flight task would sit at or above this bound",
        PART_SIZE / (1024 * 1024),
    );
    eprintln!("PASS: peak RSS {peak_mb:.1} MiB stays well under the {bound_mb:.1} MiB whole-part-buffering bound");
}

/// Write a `size`-byte file with a cheap repeating byte pattern without holding it all in memory at
/// once (this helper itself must not defeat the point of the test it sets up for).
fn write_pattern_file(path: &Path, size: u64) {
    use std::io::Write;
    const CHUNK: usize = 1024 * 1024;
    let pattern: Vec<u8> = (0..CHUNK).map(|i| (i % 251) as u8).collect();
    let mut f = std::io::BufWriter::new(std::fs::File::create(path).unwrap());
    let mut written = 0u64;
    while written < size {
        let want = ((size - written) as usize).min(CHUNK);
        f.write_all(&pattern[..want]).unwrap();
        written += want as u64;
    }
    f.flush().unwrap();
}

/// Best-effort `DeleteObject` cleanup so the RSS test doesn't leave the probe object behind under
/// `file-replicator-verify-test/` (mirrors the ambient-credential-chain client build the other tests
/// use via [`build_destination`], but this test needs the raw SDK client for a delete call the
/// `Destination` trait doesn't expose).
async fn delete_object_best_effort(bucket: &str, key: &str) {
    let sdk = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&sdk);
    if let Err(e) = client.delete_object().bucket(bucket).key(key).send().await {
        eprintln!("WARN: cleanup delete_object({bucket}/{key}) failed (non-fatal): {e:?}");
    }
}

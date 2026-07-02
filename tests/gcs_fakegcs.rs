//! # file-replicator — GCS destination integration test against fake-gcs-server (DESIGN §10.3, §21)
//!
//! Drives the real [`GcsDest`](file_replicator::dest::GcsDest) (through the public
//! [`Destination`](file_replicator::dest::Destination) trait + [`build_destination`] factory) against a
//! throwaway `fake-gcs-server` container on `localhost:4443`. One `#[tokio::test]` scenario (not several
//! independent tests, so container bring-up/teardown can be scoped cleanly around it — mirrors
//! `tests/azure_azurite.rs`/`tests/sftp_atmoz.rs`):
//!
//!   * small file (~64 KiB, a single `uploadType=resumable` session with one chunk) delivered via a
//!     `{"$secret":"…"}` credential reference (exercises the generic-secret auth path, DESIGN
//!     §11.5-equivalent — fake-gcs-server does not itself validate the bearer token, but this proves
//!     `GcsDest` resolves and sends it) — content + checksum round-trip via an independent raw
//!     `reqwest` read-back (`alt=media`, not `GcsDest`).
//!   * large file (~17 MiB, small `chunkBytes` so it spans several chunks) delivered anonymously, with a
//!     durable `StateStore` so the resume checkpoint (including the session URI) persists.
//!   * **resume**: throttle the large-file delivery, interrupt it after a persisted partial checkpoint,
//!     resume from the checkpoint with unlimited bandwidth, and assert the final cumulative progress
//!     equals the object size (not `committed + size`) — proof the resumed session (reusing the
//!     persisted session URI) staged only the missing tail, not the whole file again.
//!   * idempotent re-delivery to the same `relpath` overwrites (FR-REL-4-equivalent).
//!   * `verify(Checksum)` compares the locally-computed CRC32C directly against the object's `crc32c`
//!     metadata field (no re-download) — DESIGN §10.2's "CRC32C/MD5" verify column.
//!   * `abort` on a resume token pointing at an already-gone session is idempotent (treats 404 as clean).
//!
//! Self-skips (fast TCP probe → `eprintln!` + return) when Docker or the container never becomes
//! reachable, so `cargo test` stays green in CI (which provisions no simulators) and the 90% coverage
//! gate rests on the `dest/gcs/mod.rs` unit tests alone (`dest/gcs/client.rs` is excluded from the gate
//! — see its module doc).

#![cfg(feature = "dest-gcs")]

use std::net::TcpStream;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use file_replicator::config::{EgressCfg, GcsEgress};
use file_replicator::dest::{build_destination, DestDeps, SharedDestination};
use file_replicator::domain::{ItemState, ProgressSink, ResumeState, WorkItem};
use file_replicator::ratelimit::{Bandwidth, SystemClock, TokenBucket};
use file_replicator::state::{SqliteStore, StateStore};

const HOST: &str = "127.0.0.1";
const PORT: u16 = 4443;
const CONTAINER_NAME: &str = "fr-fakegcs";
const INSTANCE: &str = "gcsit";
const MIB: usize = 1024 * 1024;
const BUCKET: &str = "fr-test-bucket";
const SECRET_ACCESS_TOKEN: &str = "test-vault-access-token";

fn endpoint() -> String {
    format!("http://{HOST}:{PORT}")
}

/// True when fake-gcs-server answers an actual HTTP request within a short timeout — a plain TCP
/// connect is not enough here: under concurrent `cargo test` load (several `tests/*.rs` binaries each
/// racing to start their own throwaway container at once), Docker Desktop's port-forward can accept the
/// TCP handshake into its backlog slightly before the Go HTTP server behind it is actually servicing
/// requests, which was observed to make the very next real request fail with a transport error. A raw
/// HTTP/1.0 round-trip (no external HTTP client crate needed for this sync probe) proves the whole path
/// end to end.
fn fakegcs_up() -> bool {
    let addr = format!("{HOST}:{PORT}").parse().expect("addr");
    let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(400)) else {
        return false;
    };
    if stream.set_read_timeout(Some(Duration::from_millis(400))).is_err() {
        return false;
    }
    use std::io::{Read, Write};
    if stream
        .write_all(format!("GET /storage/v1/b?project=x HTTP/1.0\r\nHost: {HOST}:{PORT}\r\n\r\n").as_bytes())
        .is_err()
    {
        return false;
    }
    let mut buf = [0u8; 16];
    match stream.read(&mut buf) {
        Ok(n) if n > 0 => buf[..n].starts_with(b"HTTP/"),
        _ => false,
    }
}

/// Probe-only by default (mirrors `tests/s3_floci.rs` / `tests/p3_control_emqx.rs`): if fake-gcs-server
/// is already up, run; otherwise **self-skip** so `cargo test` / `cargo llvm-cov` stay green on a CI
/// runner that provisions no simulator (the 90% gate rests on the unit tests alone). Auto-starting a
/// throwaway `fake-gcs-server` container is opt-in via `FR_START_SIMULATORS=1` (a `docker run` on a
/// fixed host port, an image pull, and burned Actions minutes are all things CI must NOT do implicitly).
fn ensure_fakegcs_up() -> bool {
    if fakegcs_up() {
        return true;
    }
    if std::env::var_os("FR_START_SIMULATORS").is_none() {
        eprintln!(
            "skipping gcs_fakegcs: nothing listening on {HOST}:{PORT} \
             (set FR_START_SIMULATORS=1 to auto-start a fake-gcs-server container)"
        );
        return false;
    }
    let started = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-d",
            "-p",
            "4443:4443",
            "--name",
            CONTAINER_NAME,
            "fsouza/fake-gcs-server:latest",
            "-scheme",
            "http",
            "-public-host",
            "127.0.0.1:4443",
            "-port",
            "4443",
        ])
        .output();
    if started.is_err() {
        eprintln!("skipping gcs_fakegcs: docker is not available");
        return false;
    }
    // A non-zero exit (e.g. "name already in use") is fine — someone else is bringing it up
    // concurrently; we just poll below regardless.
    for _ in 0..60 {
        if fakegcs_up() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    eprintln!("skipping gcs_fakegcs: fake-gcs-server never became reachable on {HOST}:{PORT}");
    false
}

/// Stops the throwaway container on drop (best-effort; covers both normal return and a panic
/// unwinding through the test body).
struct ContainerGuard;
impl Drop for ContainerGuard {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", CONTAINER_NAME]).output();
    }
}

/// A fake credential service returning canned GCS credentials JSON for one secret name — exercises
/// `GcsDest`'s `{"$secret":"…"}` resolution path (DESIGN §11.5-equivalent) without any real vault.
struct FakeCreds;
impl ggcommons::credentials::CredentialService for FakeCreds {
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
    fn get_json(&self, _name: &str) -> ggcommons::Result<Option<serde_json::Value>> {
        Ok(Some(serde_json::json!({ "accessToken": SECRET_ACCESS_TOKEN })))
    }
}

fn gcs_egress(via_secret: bool, chunk_bytes: Option<u64>) -> GcsEgress {
    GcsEgress {
        bucket: BUCKET.into(),
        prefix: "in/".into(),
        endpoint_url: Some(endpoint()),
        access_token: None,
        anonymous: !via_secret,
        credentials: via_secret.then(|| serde_json::json!({ "$secret": "gcs-creds" })),
        chunk_bytes,
        checksum_algorithm: None,
    }
}

fn build_gcs(
    via_secret: bool,
    chunk_bytes: Option<u64>,
    store: Option<Arc<dyn StateStore>>,
) -> SharedDestination {
    let egress = EgressCfg::Gcs(Box::new(gcs_egress(via_secret, chunk_bytes)));
    let mut deps = DestDeps::default();
    if via_secret {
        deps = deps.with_credentials(Some(Arc::new(FakeCreds)));
    }
    if let Some(s) = store {
        deps = deps.with_store(s);
    }
    build_destination(&egress, &deps).expect("build gcs dest")
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

fn unique(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("frtest-{tag}-{nanos:x}")
}

fn payload(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i % 251) as u8).collect()
}

fn capped_bandwidth(mb_per_sec: u64) -> Bandwidth {
    Bandwidth::new(
        Arc::new(TokenBucket::new(mb_per_sec * MIB as u64, Arc::new(SystemClock))),
        Arc::new(TokenBucket::unlimited()),
    )
}

/// Idempotently create the throwaway test bucket via the raw JSON API (tolerates "already exists" —
/// mirrors `azure_azurite`'s `raw_container().create()`).
async fn ensure_bucket() {
    let client = reqwest::Client::new();
    let _ = client
        .post(format!("{}/storage/v1/b?project=any", endpoint()))
        .json(&serde_json::json!({ "name": BUCKET }))
        .send()
        .await;
}

/// Independently read back an object's bytes via a raw `reqwest` GET (`alt=media`, not `GcsDest`), so
/// the round-trip assertion does not just check "our own writer agrees with itself".
async fn raw_read_back(object: &str) -> Vec<u8> {
    let client = reqwest::Client::new();
    let url = format!(
        "{}/storage/v1/b/{BUCKET}/o/{}?alt=media",
        endpoint(),
        urlencode(object)
    );
    let resp = client.get(&url).send().await.expect("raw gcs get");
    assert!(resp.status().is_success(), "raw gcs get status: {}", resp.status());
    resp.bytes().await.expect("raw gcs body").to_vec()
}

/// Minimal percent-encoding for an object path used as a single opaque path segment (mirrors what
/// `reqwest::Url::path_segments_mut().push()` does inside `GcsDest` itself — kept independent here so
/// the read-back path is not reusing `GcsDest`'s own URL-building code).
fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Count of a persisted GCS resume checkpoint's committed bytes, if any.
fn committed_bytes(store: &Arc<dyn StateStore>, rel: &str) -> Option<u64> {
    store
        .load_resume(INSTANCE, rel, "gcs")
        .unwrap()
        .map(|rs| rs.bytes_committed)
}

/// Small chunks so a ~17 MiB file spans several chunks (mirrors `azure_azurite`'s 5 MiB blocks).
fn small_chunks() -> Option<u64> {
    Some(5 * MIB as u64)
}

#[tokio::test]
async fn gcs_fakegcs_full_scenario() {
    if !ensure_fakegcs_up() {
        return;
    }
    let _guard = ContainerGuard;

    ensure_bucket().await;

    // ---- small file via {"$secret":"…"} credentials, checksum round-trip -------------------------
    {
        let src = tempfile::tempdir().unwrap();
        let data = payload(64 * 1024 + 7); // one chunk, well under the default 8 MiB chunk size
        std::fs::write(src.path().join("a.bin"), &data).unwrap();

        let dest = build_gcs(true, None, None);
        let item = work_item(src.path(), "a.bin");
        let delivered = dest
            .deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .expect("deliver small (via $secret)");
        assert_eq!(delivered.bytes, data.len() as u64);

        dest.verify(&item, &delivered, file_replicator::config::Verify::Checksum)
            .await
            .expect("verify checksum (crc32c metadata echo, no re-download)");
        dest.verify(&item, &delivered, file_replicator::config::Verify::Size)
            .await
            .expect("verify size");
        dest.verify(&item, &delivered, file_replicator::config::Verify::None)
            .await
            .expect("verify exists");

        let back = raw_read_back("in/a.bin").await;
        assert_eq!(back, data, "independent raw gcs read-back matches source bytes");
    }

    // ---- large file (anonymous creds, chunked resumable session) + persisted resume checkpoint ----
    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
    let big_rel = unique("big") + ".bin";
    let data = payload(17 * MIB); // 5 MiB chunks → 4 chunks (5, 5, 5, 2)
    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join(&big_rel), &data).unwrap();

    {
        let dest = build_gcs(false, small_chunks(), Some(store.clone()));
        let item = work_item(src.path(), &big_rel);
        let delivered = dest
            .deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .expect("deliver large (chunked resumable session)");
        assert_eq!(delivered.bytes, data.len() as u64);
        dest.verify(&item, &delivered, file_replicator::config::Verify::Checksum)
            .await
            .expect("verify checksum (large)");
        let back = raw_read_back(&format!("in/{big_rel}")).await;
        assert_eq!(back, data, "chunked-session object round trip byte-for-byte");
        assert!(
            store.load_resume(INSTANCE, &big_rel, "gcs").unwrap().is_none(),
            "resume checkpoint cleared after a successful delivery"
        );
    }

    // ---- resume: interrupt after a partial checkpoint, restart, stage only the tail ----------------
    {
        let resume_rel = unique("resume") + ".bin";
        std::fs::write(src.path().join(&resume_rel), &data).unwrap();
        let dest = build_gcs(false, small_chunks(), Some(store.clone()));
        let item = work_item(src.path(), &resume_rel);

        let dest_c = dest.clone();
        let item_c = item.clone();
        let bw = capped_bandwidth(4); // slow enough that the poll below reliably catches a checkpoint
        let handle = tokio::spawn(async move {
            let _ = dest_c.deliver(&item_c, None, &ProgressSink::noop(), &bw).await;
        });

        let mut committed = 0u64;
        for _ in 0..800 {
            tokio::time::sleep(Duration::from_millis(25)).await;
            if let Some(c) = committed_bytes(&store, &resume_rel) {
                if c > 0 && c < data.len() as u64 {
                    committed = c;
                    break;
                }
            }
            if handle.is_finished() {
                break;
            }
        }
        handle.abort();
        let _ = handle.await;
        assert!(
            committed > 0 && committed < data.len() as u64,
            "expected to interrupt mid-transfer (committed {committed} of {})",
            data.len()
        );
        let resume = store
            .load_resume(INSTANCE, &resume_rel, "gcs")
            .unwrap()
            .expect("a partial checkpoint was persisted before the interrupt");
        assert_eq!(resume.bytes_committed, committed);

        // Restart from the checkpoint with unlimited bandwidth; track the MAX cumulative progress —
        // resuming only the missing tail (from the persisted local checkpoint + session URI) lands the
        // final value exactly at `data.len()` (re-sending the committed prefix would overshoot to
        // `committed + data.len()`).
        let max_seen = Arc::new(AtomicU64::new(0));
        let ms = max_seen.clone();
        let sink = ProgressSink::new(move |n| {
            ms.fetch_max(n, Ordering::SeqCst);
        });
        let delivered = dest
            .deliver(&item, Some(resume), &sink, &Bandwidth::unlimited())
            .await
            .expect("resume completes without re-sending committed bytes");
        assert_eq!(delivered.bytes, data.len() as u64);
        let max = max_seen.load(Ordering::SeqCst);
        assert_eq!(
            max,
            data.len() as u64,
            "resume must stage ONLY the missing tail: final progress {max} != size {} \
             (a full re-send would reach {})",
            data.len(),
            committed + data.len() as u64
        );

        dest.verify(&item, &delivered, file_replicator::config::Verify::Checksum)
            .await
            .expect("verify checksum after resume");
        let back = raw_read_back(&format!("in/{resume_rel}")).await;
        assert_eq!(back, data, "resumed transfer is byte-for-byte correct");
    }

    // ---- idempotent re-delivery: stable relpath overwrite (FR-REL-4-equivalent) -------------------
    {
        let dest = build_gcs(false, None, None);
        let dup_rel = unique("dup") + ".bin";

        let v1 = payload(20_000);
        std::fs::write(src.path().join(&dup_rel), &v1).unwrap();
        let item1 = work_item(src.path(), &dup_rel);
        let d1 = dest
            .deliver(&item1, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .expect("deliver v1");
        dest.verify(&item1, &d1, file_replicator::config::Verify::Checksum)
            .await
            .expect("verify v1");
        assert_eq!(raw_read_back(&format!("in/{dup_rel}")).await, v1);

        let v2 = payload(37_000);
        assert_ne!(v1, v2);
        std::fs::write(src.path().join(&dup_rel), &v2).unwrap();
        let item2 = work_item(src.path(), &dup_rel);
        let d2 = dest
            .deliver(&item2, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .expect("deliver v2 (overwrite)");
        assert_eq!(d2.bytes, v2.len() as u64);
        dest.verify(&item2, &d2, file_replicator::config::Verify::Checksum)
            .await
            .expect("verify v2");
        assert_eq!(
            raw_read_back(&format!("in/{dup_rel}")).await,
            v2,
            "stable relpath overwrote identically with the new content"
        );
    }

    // ---- abort: resume token pointing at an already-gone session is idempotent ---------------------
    {
        let dest = build_gcs(false, None, None);
        let abort_rel = unique("abort") + ".bin";
        let item = work_item(src.path(), &abort_rel);
        let resume = ResumeState {
            bytes_committed: 3 * MIB as u64,
            token: serde_json::json!({
                "gcs": {
                    "bucket": BUCKET, "object": "in/does-not-exist.bin",
                    "sessionUri": format!("{}/upload/storage/v1/b/{BUCKET}/o?upload_id=does-not-exist", endpoint()),
                    "size": 17825792, "mtimeMs": 0, "bytesCommitted": 3145728
                }
            }),
        };
        dest.abort(&item, &resume).await.expect("abort treats a gone session as clean");
        dest.abort(&item, &resume).await.expect("abort is idempotent");
    }
}

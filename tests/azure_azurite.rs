//! # file-replicator — Azure Blob destination integration test against Azurite (DESIGN §10.3, §21)
//!
//! Drives the real [`AzureDest`](file_replicator::dest::AzureDest) (through the public
//! [`Destination`](file_replicator::dest::Destination) trait + [`build_destination`] factory) against
//! a throwaway `azurite-blob` container on `localhost:10000` (well-known account `devstoreaccount1` +
//! its standard well-known dev key). One `#[tokio::test]` scenario (not several independent tests, so
//! container bring-up/teardown can be scoped cleanly around it — mirrors `tests/sftp_atmoz.rs`):
//!
//!   * small file (~64 KiB, ≤ the staged-block threshold) delivered via a `{"$secret":"…"}` credential
//!     reference (exercises the generic-secret auth path, DESIGN §11.5-equivalent) — content +
//!     checksum round-trip via an independent raw `azure_storage_blobs` read-back (a fresh client, not
//!     `AzureDest`).
//!   * large file (~17 MiB, staged blocks with a small `blockSizeBytes` so it spans several blocks)
//!     delivered via ambient (config) credentials, with a durable `StateStore` so the resume checkpoint
//!     persists.
//!   * **resume**: throttle the large-file delivery, interrupt it after a persisted partial checkpoint,
//!     resume from the checkpoint with unlimited bandwidth, and assert the final cumulative progress
//!     equals the object size (not `committed + size`) — proof the resume staged only the missing
//!     tail, not the whole file again.
//!   * idempotent re-delivery to the same `relpath` overwrites (FR-REL-4-equivalent).
//!   * `abort` is a documented no-op (Azure has no drop-uncommitted-blocks API) and stays idempotent.
//!
//! Self-skips (fast TCP probe → `eprintln!` + return) when Docker or the container never becomes
//! reachable, so `cargo test` stays green in CI (which provisions no simulators) and the 90% coverage
//! gate rests on the `dest/azure/mod.rs` unit tests alone (`dest/azure/client.rs` is excluded from the
//! gate — see its module doc).

#![cfg(feature = "dest-azure")]

use std::net::TcpStream;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use azure_core::auth::Secret;
use azure_storage::{CloudLocation, StorageCredentials};
use azure_storage_blobs::prelude::{ClientBuilder, ContainerClient};

use file_replicator::config::{AzureBlockCfg, AzureEgress, EgressCfg};
use file_replicator::dest::{build_destination, DestDeps, SharedDestination};
use file_replicator::domain::{ItemState, ProgressSink, ResumeState, WorkItem};
use file_replicator::ratelimit::{Bandwidth, SystemClock, TokenBucket};
use file_replicator::state::{SqliteStore, StateStore};

const HOST: &str = "127.0.0.1";
const PORT: u16 = 10000;
const CONTAINER_NAME: &str = "fr-azurite";
const INSTANCE: &str = "azit";
const MIB: usize = 1024 * 1024;
const ACCOUNT: &str = "devstoreaccount1";
// Azurite's well-known development account key (public, documented by Microsoft — not a secret).
const ACCOUNT_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
const BLOB_CONTAINER: &str = "fr-test-container";
fn endpoint() -> String {
    format!("http://{HOST}:{PORT}/{ACCOUNT}")
}

/// True when Azurite's blob port answers a TCP connect within a short timeout.
fn azurite_up() -> bool {
    let addr = format!("{HOST}:{PORT}").parse().expect("addr");
    TcpStream::connect_timeout(&addr, Duration::from_millis(400)).is_ok()
}

/// Probe-only by default (mirrors `tests/s3_floci.rs` / `tests/p3_control_emqx.rs`): if the Azurite
/// blob port is already up, run; otherwise **self-skip** so `cargo test` / `cargo llvm-cov` stay green
/// on a CI runner that provisions no simulator (the 90% gate rests on the unit tests alone).
/// Auto-starting a throwaway Azurite container is opt-in via `FR_START_SIMULATORS=1` (a `docker run` on
/// a fixed host port, an image pull, and burned Actions minutes are all things CI must NOT do implicitly).
fn ensure_azurite_up() -> bool {
    if azurite_up() {
        return true;
    }
    if std::env::var_os("FR_START_SIMULATORS").is_none() {
        eprintln!(
            "skipping azure_azurite: nothing listening on {HOST}:{PORT} \
             (set FR_START_SIMULATORS=1 to auto-start an azurite container)"
        );
        return false;
    }
    let started = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-d",
            "-p",
            "10000:10000",
            "--name",
            CONTAINER_NAME,
            "mcr.microsoft.com/azure-storage/azurite:latest",
            "azurite-blob",
            "--blobHost",
            "0.0.0.0",
            // `--loose`: Azurite's default "strict" mode rejects the `x-ms-range-get-content-crc64`
            // range-integrity header the SDK's chunked `get_content()` download sends on every ranged
            // GET (real Azure Blob Storage supports it) — loose mode accepts it. Simulator-only flag;
            // no client-side behavior change.
            "--loose",
        ])
        .output();
    if started.is_err() {
        eprintln!("skipping azure_azurite: docker is not available");
        return false;
    }
    // A non-zero exit (e.g. "name already in use") is fine — someone else is bringing it up
    // concurrently; we just poll below regardless.
    for _ in 0..60 {
        if azurite_up() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    eprintln!("skipping azure_azurite: azurite never became reachable on {HOST}:{PORT}");
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

/// A fake credential service returning canned Azure credentials JSON for one secret name — exercises
/// `AzureDest`'s `{"$secret":"…"}` resolution path (DESIGN §11.5-equivalent) without any real vault.
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
        Ok(Some(serde_json::json!({ "accountKey": ACCOUNT_KEY })))
    }
}

fn azure_egress(via_secret: bool, blocks: AzureBlockCfg) -> AzureEgress {
    AzureEgress {
        account: ACCOUNT.into(),
        container: BLOB_CONTAINER.into(),
        prefix: "in/".into(),
        endpoint_url: Some(endpoint()),
        account_key: if via_secret { None } else { Some(ACCOUNT_KEY.into()) },
        connection_string: None,
        credentials: via_secret.then(|| serde_json::json!({ "$secret": "azure-creds" })),
        blocks,
        checksum_algorithm: None,
    }
}

fn build_azure(via_secret: bool, blocks: AzureBlockCfg, store: Option<Arc<dyn StateStore>>) -> SharedDestination {
    let egress = EgressCfg::Azure(Box::new(azure_egress(via_secret, blocks)));
    let mut deps = DestDeps::default();
    if via_secret {
        deps = deps.with_credentials(Some(Arc::new(FakeCreds)));
    }
    if let Some(s) = store {
        deps = deps.with_store(s);
    }
    build_destination(&egress, &deps).expect("build azure dest")
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

/// A raw `azure_storage_blobs` container client (not `AzureDest`) for independent setup/read-back.
fn raw_container() -> ContainerClient {
    let creds = StorageCredentials::access_key(ACCOUNT, Secret::new(ACCOUNT_KEY));
    ClientBuilder::with_location(
        CloudLocation::Custom {
            account: ACCOUNT.to_string(),
            uri: endpoint(),
        },
        creds,
    )
    .container_client(BLOB_CONTAINER)
}

/// Independently read back a blob's bytes via a fresh raw client (not `AzureDest`), so the round-trip
/// assertion does not just check "our own writer agrees with itself".
async fn raw_read_back(blob: &str) -> Vec<u8> {
    raw_container()
        .blob_client(blob)
        .get_content()
        .await
        .expect("raw azure get_content")
}

/// Count of a persisted Azure resume checkpoint's committed bytes, if any.
fn committed_bytes(store: &Arc<dyn StateStore>, rel: &str) -> Option<u64> {
    store
        .load_resume(INSTANCE, rel, "azure")
        .unwrap()
        .map(|rs| rs.bytes_committed)
}

/// Small blocks so a ~17 MiB file spans several blocks (mirrors `s3_floci`'s 5 MiB parts).
fn small_blocks() -> AzureBlockCfg {
    AzureBlockCfg {
        threshold_bytes: Some(5 * MIB as u64),
        block_size_bytes: Some(5 * MIB as u64),
    }
}

#[tokio::test]
async fn azure_azurite_full_scenario() {
    if !ensure_azurite_up() {
        return;
    }
    let _guard = ContainerGuard;

    // Container creation (Azurite requires it to exist before blobs can be written — mirrors
    // `s3_floci`'s `make_bucket`). Tolerate "already exists" (a prior run against the same throwaway
    // container, or a concurrent CI shard).
    let _ = raw_container().create().await;

    // ---- small file via {"$secret":"…"} credentials, checksum round-trip -------------------------
    {
        let src = tempfile::tempdir().unwrap();
        let data = payload(64 * 1024 + 7); // well under the default 16 MiB / configured 5 MiB threshold
        std::fs::write(src.path().join("a.bin"), &data).unwrap();

        let dest = build_azure(true, AzureBlockCfg::default(), None);
        let item = work_item(src.path(), "a.bin");
        let delivered = dest
            .deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .expect("deliver small (via $secret)");
        assert_eq!(delivered.bytes, data.len() as u64);

        dest.verify(&item, &delivered, file_replicator::config::Verify::Checksum)
            .await
            .expect("verify checksum");
        dest.verify(&item, &delivered, file_replicator::config::Verify::Size)
            .await
            .expect("verify size");
        dest.verify(&item, &delivered, file_replicator::config::Verify::None)
            .await
            .expect("verify exists");

        let back = raw_read_back("in/a.bin").await;
        assert_eq!(back, data, "independent raw azure read-back matches source bytes");
    }

    // ---- large file (ambient creds, staged blocks) + persisted resume checkpoint ------------------
    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
    let big_rel = unique("big") + ".bin";
    let data = payload(17 * MIB); // 5 MiB blocks → 4 blocks (5, 5, 5, 2)
    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join(&big_rel), &data).unwrap();

    {
        let dest = build_azure(false, small_blocks(), Some(store.clone()));
        let item = work_item(src.path(), &big_rel);
        let delivered = dest
            .deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .expect("deliver large (staged blocks)");
        assert_eq!(delivered.bytes, data.len() as u64);
        dest.verify(&item, &delivered, file_replicator::config::Verify::Checksum)
            .await
            .expect("verify checksum (large)");
        let back = raw_read_back(&format!("in/{big_rel}")).await;
        assert_eq!(back, data, "staged-block blob round trip byte-for-byte");
        assert!(
            store.load_resume(INSTANCE, &big_rel, "azure").unwrap().is_none(),
            "resume checkpoint cleared after a successful delivery"
        );
    }

    // ---- resume: interrupt after a partial checkpoint, restart, stage only the tail ----------------
    {
        let resume_rel = unique("resume") + ".bin";
        std::fs::write(src.path().join(&resume_rel), &data).unwrap();
        let dest = build_azure(false, small_blocks(), Some(store.clone()));
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
            .load_resume(INSTANCE, &resume_rel, "azure")
            .unwrap()
            .expect("a partial checkpoint was persisted before the interrupt");
        assert_eq!(resume.bytes_committed, committed);

        // Restart from the checkpoint with unlimited bandwidth; track the MAX cumulative progress —
        // resuming only the missing tail lands the final value exactly at `data.len()` (re-sending the
        // committed prefix would overshoot to `committed + data.len()`).
        let max_seen = Arc::new(AtomicU64::new(0));
        let ms = max_seen.clone();
        let sink = ProgressSink::new(move |n| {
            ms.fetch_max(n, Ordering::SeqCst);
        });
        let delivered = dest
            .deliver(&item, Some(resume), &sink, &Bandwidth::unlimited())
            .await
            .expect("resume completes without re-staging committed bytes");
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
        let dest = build_azure(false, AzureBlockCfg::default(), None);
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

    // ---- abort: documented no-op (Azure has no drop-uncommitted-blocks API), stays idempotent ------
    {
        let dest = build_azure(false, AzureBlockCfg::default(), None);
        let abort_rel = unique("abort") + ".bin";
        let item = work_item(src.path(), &abort_rel);
        let resume = ResumeState {
            bytes_committed: 3 * MIB as u64,
            token: serde_json::json!({
                "azure": { "blob": "in/does-not-exist.bin", "blockSize": 5242880, "size": 17825792, "mtimeMs": 0, "bytesCommitted": 3145728 }
            }),
        };
        dest.abort(&item, &resume).await.expect("abort no-op");
        dest.abort(&item, &resume).await.expect("abort is idempotent");
    }
}

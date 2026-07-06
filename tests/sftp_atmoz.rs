//! # file-replicator — SFTP destination integration test against `atmoz/sftp` (DESIGN §10.3, §21)
//!
//! Drives the real [`SftpDest`](file_replicator::dest::SftpDest) (through the public
//! [`Destination`](file_replicator::dest::Destination) trait + [`build_destination`] factory) against
//! a throwaway `atmoz/sftp` container on `localhost:2222` (user `testuser` / password `testpass`,
//! writable dir `/upload`). The whole scenario runs as **one** `#[tokio::test]` function (not several
//! independent tests) so container bring-up/teardown can be scoped cleanly around it — `cargo test`
//! runs test functions concurrently within a binary, so per-function container lifecycle management
//! would race; a single scenario avoids that while still exercising every required path:
//!
//!   * small file (~64 KiB) delivered via a `{"$secret":"…"}` credential reference (exercises the
//!     generic-secret auth path, DESIGN §11.5-equivalent) — content + checksum round-trip via an
//!     independent raw SFTP read-back (a fresh `russh`/`russh-sftp` session, not `SftpDest`).
//!   * large file (~17 MiB) delivered via ambient (config) credentials, with a durable `StateStore` so
//!     the offset-resume checkpoint persists.
//!   * **resume**: throttle the large-file delivery, interrupt it after a persisted partial offset,
//!     resume from the checkpoint with unlimited bandwidth, and assert the final cumulative progress
//!     equals the object size (not `committed + size`) — proof the resume streamed only the missing
//!     tail, not the whole file again.
//!   * idempotent re-delivery to the same `relpath` overwrites (FR-REL-4-equivalent).
//!
//! Self-skips (fast TCP probe → `eprintln!` + return) when Docker or the container never becomes
//! reachable, so `cargo test` stays green in CI (which provisions no simulators) and the 90% coverage
//! gate rests on the `dest/sftp/mod.rs` unit tests alone (`dest/sftp/client.rs` is excluded from the
//! gate — see its module doc).

#![cfg(feature = "dest-sftp")]

use std::net::TcpStream;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use file_replicator::config::SftpEgress;
use file_replicator::dest::{build_destination, DestDeps, SharedDestination};
use file_replicator::config::EgressCfg;
use file_replicator::domain::{ItemState, ProgressSink, ResumeState, WorkItem};
use file_replicator::ratelimit::{Bandwidth, SystemClock, TokenBucket};
use file_replicator::state::{SqliteStore, StateStore};

const HOST: &str = "127.0.0.1";
const PORT: u16 = 2222;
const CONTAINER: &str = "fr-sftp";
const INSTANCE: &str = "sftpit";
const MIB: usize = 1024 * 1024;

/// True when the SFTP port answers a TCP connect AND sends the SSH identification banner within a
/// short timeout. A bare TCP-connect probe is not enough here: `sshd` inside the container can accept
/// the connection slightly before it is ready to complete a key exchange (briefly during startup),
/// which would otherwise let the caller race ahead and see a spurious `Disconnect`.
fn sftp_up() -> bool {
    use std::io::Read;
    let addr = format!("{HOST}:{PORT}").parse().expect("addr");
    let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(400)) else {
        return false;
    };
    if stream.set_read_timeout(Some(Duration::from_millis(400))).is_err() {
        return false;
    }
    let mut buf = [0u8; 4];
    matches!(stream.read(&mut buf), Ok(4) if &buf == b"SSH-")
}

/// Probe-only by default (mirrors `tests/s3_floci.rs` / `tests/p3_control_emqx.rs`): if the SFTP port
/// is already up, run; otherwise **self-skip** so `cargo test` / `cargo llvm-cov` stay green on a
/// CI runner that provisions no simulator (the 90% gate rests on the unit tests alone). Auto-starting a
/// throwaway `atmoz/sftp` container is opt-in via `FR_START_SIMULATORS=1` (a `docker run` on a
/// fixed host port, an image pull, and burned Actions minutes are all things CI must NOT do implicitly).
fn ensure_sftp_up() -> bool {
    if sftp_up() {
        return true;
    }
    if std::env::var_os("FR_START_SIMULATORS").is_none() {
        eprintln!(
            "skipping sftp_atmoz: nothing listening on {HOST}:{PORT} \
             (set FR_START_SIMULATORS=1 to auto-start an atmoz/sftp container)"
        );
        return false;
    }
    let started = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            CONTAINER,
            "-p",
            "2222:22",
            "atmoz/sftp:latest",
            "testuser:testpass:::upload",
        ])
        .output();
    if started.is_err() {
        eprintln!("skipping sftp_atmoz: docker is not available");
        return false;
    }
    // A non-zero exit (e.g. "name already in use") is fine — someone else is bringing it up
    // concurrently; we just poll below regardless.
    for _ in 0..60 {
        if sftp_up() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    eprintln!("skipping sftp_atmoz: atmoz/sftp never became reachable on {HOST}:{PORT}");
    false
}

/// Stops the throwaway container on drop (best-effort; covers both normal return and a panic
/// unwinding through the test body).
struct ContainerGuard;
impl Drop for ContainerGuard {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", CONTAINER]).output();
    }
}

/// A fake credential service returning canned SFTP credentials JSON for one secret name — exercises
/// `SftpDest`'s `{"$secret":"…"}` resolution path (DESIGN §11.5-equivalent) without any real vault.
struct FakeCreds;
impl edgecommons::credentials::CredentialService for FakeCreds {
    fn get(&self, _n: &str) -> edgecommons::Result<Option<edgecommons::credentials::Secret>> {
        Ok(None)
    }
    fn get_version(
        &self,
        _n: &str,
        _v: &str,
    ) -> edgecommons::Result<Option<edgecommons::credentials::Secret>> {
        Ok(None)
    }
    fn exists(&self, _n: &str) -> edgecommons::Result<bool> {
        Ok(true)
    }
    fn list(&self, _p: &str) -> edgecommons::Result<Vec<edgecommons::credentials::SecretMeta>> {
        Ok(vec![])
    }
    fn versions(&self, _n: &str) -> edgecommons::Result<Vec<String>> {
        Ok(vec![])
    }
    fn put(
        &self,
        _n: &str,
        _v: &[u8],
        _o: edgecommons::credentials::PutOptions,
    ) -> edgecommons::Result<String> {
        Ok("v1".into())
    }
    fn delete(&self, _n: &str) -> edgecommons::Result<bool> {
        Ok(false)
    }
    fn get_json(&self, _name: &str) -> edgecommons::Result<Option<serde_json::Value>> {
        Ok(Some(serde_json::json!({ "username": "testuser", "password": "testpass" })))
    }
}

fn sftp_egress(via_secret: bool) -> SftpEgress {
    SftpEgress {
        host: HOST.into(),
        port: Some(PORT),
        base_dir: "/upload".into(),
        username: if via_secret { None } else { Some("testuser".into()) },
        password: if via_secret { None } else { Some("testpass".into()) },
        private_key: None,
        passphrase: None,
        credentials: via_secret.then(|| serde_json::json!({ "$secret": "sftp-creds" })),
        host_key: None, // no pinned host key for this throwaway container...
        // ...so opt into the insecure accept-any escape hatch (the fail-closed default would refuse
        // the connection). Fine for a disposable local test container; never in production.
        insecure_accept_any_host_key: true,
        temp_suffix: None,
        fsync: false,
        checksum_algorithm: None,
    }
}

fn build_sftp(via_secret: bool, store: Option<Arc<dyn StateStore>>) -> SharedDestination {
    let egress = EgressCfg::Sftp(Box::new(sftp_egress(via_secret)));
    let mut deps = DestDeps::default();
    if via_secret {
        deps = deps.with_credentials(Some(Arc::new(FakeCreds)));
    }
    if let Some(s) = store {
        deps = deps.with_store(s);
    }
    build_destination(&egress, &deps).expect("build sftp dest")
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

/// Independently read back a remote file's bytes: a fresh `russh`/`russh-sftp` session (not
/// `SftpDest`), so the round-trip assertion does not just check "our own writer agrees with itself".
async fn raw_read_back(remote_path: &str) -> Vec<u8> {
    use russh::client::{connect, Config, Handler};

    struct AcceptAll;
    impl Handler for AcceptAll {
        type Error = russh::Error;
        async fn check_server_key(
            &mut self,
            _key: &russh::keys::PublicKey,
        ) -> std::result::Result<bool, Self::Error> {
            Ok(true)
        }
    }

    let cfg = Arc::new(Config::default());
    let mut handle = connect(cfg, (HOST, PORT), AcceptAll)
        .await
        .expect("raw ssh connect");
    let auth = handle
        .authenticate_password("testuser", "testpass")
        .await
        .expect("raw ssh auth");
    assert!(auth.success(), "raw ssh auth must succeed");
    let channel = handle.channel_open_session().await.expect("raw channel open");
    channel
        .request_subsystem(true, "sftp")
        .await
        .expect("raw sftp subsystem");
    let sftp = russh_sftp::client::SftpSession::new(channel.into_stream())
        .await
        .expect("raw sftp session init");
    sftp.read(remote_path).await.expect("raw sftp read")
}

/// Count of a persisted SFTP resume checkpoint's committed bytes, if any.
fn committed_bytes(store: &Arc<dyn StateStore>, rel: &str) -> Option<u64> {
    store
        .load_resume(INSTANCE, rel, "sftp")
        .unwrap()
        .map(|rs| rs.bytes_committed)
}

#[tokio::test]
async fn sftp_atmoz_full_scenario() {
    if !ensure_sftp_up() {
        return;
    }
    let _guard = ContainerGuard;

    // ---- small file via {"$secret":"…"} credentials, checksum round-trip -------------------------
    {
        let src = tempfile::tempdir().unwrap();
        let data = payload(64 * 1024 + 7);
        std::fs::write(src.path().join("a.bin"), &data).unwrap();

        let dest = build_sftp(true, None);
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

        let back = raw_read_back("/upload/a.bin").await;
        assert_eq!(back, data, "independent raw sftp read-back matches source bytes");
    }

    // ---- large file (ambient creds) + persisted resume checkpoint ---------------------------------
    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
    let big_rel = unique("big") + ".bin";
    let data = payload(17 * MIB);
    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join(&big_rel), &data).unwrap();

    {
        let dest = build_sftp(false, Some(store.clone()));
        let item = work_item(src.path(), &big_rel);
        let delivered = dest
            .deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .expect("deliver large");
        assert_eq!(delivered.bytes, data.len() as u64);
        dest.verify(&item, &delivered, file_replicator::config::Verify::Checksum)
            .await
            .expect("verify checksum (large)");
        let back = raw_read_back(&format!("/upload/{big_rel}")).await;
        assert_eq!(back, data, "large file round trip byte-for-byte");
        assert!(
            store.load_resume(INSTANCE, &big_rel, "sftp").unwrap().is_none(),
            "resume checkpoint cleared after a successful delivery"
        );
    }

    // ---- resume: interrupt after a partial checkpoint, restart, stream only the tail --------------
    {
        let resume_rel = unique("resume") + ".bin";
        std::fs::write(src.path().join(&resume_rel), &data).unwrap();
        let dest = build_sftp(false, Some(store.clone()));
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
            .load_resume(INSTANCE, &resume_rel, "sftp")
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
            .expect("resume completes without re-sending committed bytes");
        assert_eq!(delivered.bytes, data.len() as u64);
        let max = max_seen.load(Ordering::SeqCst);
        assert_eq!(
            max,
            data.len() as u64,
            "resume must stream ONLY the missing tail: final progress {max} != size {} \
             (a full re-send would reach {})",
            data.len(),
            committed + data.len() as u64
        );

        dest.verify(&item, &delivered, file_replicator::config::Verify::Checksum)
            .await
            .expect("verify checksum after resume");
        let back = raw_read_back(&format!("/upload/{resume_rel}")).await;
        assert_eq!(back, data, "resumed transfer is byte-for-byte correct");
    }

    // ---- idempotent re-delivery: stable relpath overwrite (FR-REL-4-equivalent) -------------------
    {
        let dest = build_sftp(false, None);
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
        assert_eq!(raw_read_back(&format!("/upload/{dup_rel}")).await, v1);

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
            raw_read_back(&format!("/upload/{dup_rel}")).await,
            v2,
            "stable relpath overwrote identically with the new content"
        );
    }

    // ---- abort: give-up cleanup removes the orphan temp file (idempotent) -------------------------
    {
        let dest = build_sftp(false, None);
        let abort_rel = unique("abort") + ".bin";
        let item = work_item(src.path(), &abort_rel);
        let resume = ResumeState {
            bytes_committed: 0,
            token: serde_json::json!({
                "sftp": { "tempPath": "/upload/.does-not-exist.part", "size": 0, "mtimeMs": 0, "bytesCommitted": 0 }
            }),
        };
        dest.abort(&item, &resume)
            .await
            .expect("abort tolerates a missing temp file");
        dest.abort(&item, &resume).await.expect("abort is idempotent");
    }
}

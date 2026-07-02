//! # file-replicator — HTTP(S) destination integration test against an in-process server (DESIGN §10.3)
//!
//! Drives the real [`HttpDest`](file_replicator::dest::HttpDest) (through the public
//! [`Destination`](file_replicator::dest::Destination) trait + [`build_destination`] factory) against a
//! tiny hand-rolled HTTP/1.1 server ([`TestServer`]) running in-process on `127.0.0.1:0` — no Docker, no
//! external crate, so this test **always runs** (never self-skips) and always contributes to the 90%
//! coverage gate. The server supports exactly what the backend needs: `PUT`/`POST` (whole-body or
//! `Content-Range`-addressed), `HEAD`/`GET` (for `verify`), `DELETE` (for `abort`), and two auth-gated
//! path prefixes (`/authed-bearer/…`, `/authed-basic/…`) to exercise both credential shapes.
//!
//! The whole scenario runs as **one** `#[tokio::test]` function (matching `tests/sftp_atmoz.rs`) so
//! server lifetime is scoped cleanly around it. Covers:
//!
//!   * small file round trip via ambient HTTP Basic auth — content + checksum + size + none `verify`.
//!   * large file (chunked ranged `PUT`, small `chunkBytes` to force several chunks) via a
//!     `{"$secret":"…"}` bearer-token credential, with a durable `StateStore` resume checkpoint.
//!   * **resume**: throttle the large-file delivery, interrupt it after a persisted partial checkpoint,
//!     resume with unlimited bandwidth, and assert the final cumulative progress equals the object size
//!     (not `committed + size`) — proof the resume streamed only the missing tail.
//!   * idempotent re-delivery to the same relpath overwrites (FR-REL-4-equivalent).
//!   * `abort`: give-up cleanup `DELETE`s a partially-uploaded object, and is a no-op (and idempotent)
//!     when nothing was ever committed.
//!   * `method: POST` (resume forced off) single-shot round trip.

#![cfg(feature = "dest-http")]

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use file_replicator::config::{EgressCfg, HttpEgress, Verify};
use file_replicator::dest::{build_destination, DestDeps, SharedDestination};
use file_replicator::domain::{ItemState, ProgressSink, ResumeState, WorkItem};
use file_replicator::ratelimit::{Bandwidth, SystemClock, TokenBucket};
use file_replicator::state::{SqliteStore, StateStore};

const INSTANCE: &str = "httpit";
const MIB: usize = 1024 * 1024;
/// `base64("alice:s3cr3t")` — the ambient Basic-auth credential the server checks under `/authed-basic/`.
const BASIC_EXPECTED: &str = "Basic YWxpY2U6czNjcjN0";
const BEARER_EXPECTED: &str = "Bearer vault-bearer-tok";

// ============================================================================================
// A minimal in-process HTTP/1.1 server: enough of the protocol for this backend and nothing more.
// ============================================================================================

struct TestServer {
    addr: SocketAddr,
    store: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    shutdown: Arc<AtomicBool>,
}

impl TestServer {
    fn start() -> TestServer {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let addr = listener.local_addr().expect("local_addr");
        let store: Arc<Mutex<HashMap<String, Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));
        let shutdown = Arc::new(AtomicBool::new(false));

        let store_c = store.clone();
        let shutdown_c = shutdown.clone();
        std::thread::spawn(move || {
            while !shutdown_c.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let _ = stream.set_nonblocking(false);
                        let s = store_c.clone();
                        std::thread::spawn(move || handle_connection(stream, s));
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });

        TestServer {
            addr,
            store,
            shutdown,
        }
    }

    fn base_url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }

    /// Read back an uploaded object directly from the server's store (an independent check on the
    /// bytes the backend actually sent over the wire, without going through `HttpDest` again).
    fn get(&self, path: &str) -> Option<Vec<u8>> {
        self.store.lock().unwrap().get(path).cloned()
    }

    /// Seed an object directly (bypassing HTTP) — used to set up the `abort` scenario.
    fn put_direct(&self, path: &str, data: Vec<u8>) {
        self.store.lock().unwrap().insert(path.to_string(), data);
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        // Nudge the accept() loop past its `WouldBlock` poll so the thread exits promptly.
        let _ = TcpStream::connect(self.addr);
    }
}

struct ParsedRequest {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn read_request(stream: &mut TcpStream) -> Option<ParsedRequest> {
    let mut buf: Vec<u8> = Vec::new();
    let mut tmp = [0u8; 8192];
    let header_end = loop {
        let n = stream.read(&mut tmp).ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos + 4;
        }
        if buf.len() > 16 * 1024 * 1024 {
            return None;
        }
    };
    let header_str = String::from_utf8_lossy(&buf[..header_end - 4]).to_string();
    let mut lines = header_str.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();
    let mut headers: HashMap<String, String> = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    let content_length: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mut body = buf[header_end..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut tmp).ok()?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(content_length);
    Some(ParsedRequest {
        method,
        path,
        headers,
        body,
    })
}

fn write_response(stream: &mut TcpStream, status: u16, reason: &str, body: &[u8]) {
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body);
    let _ = stream.flush();
}

fn write_head_response(stream: &mut TcpStream, status: u16, reason: &str, content_length: usize) {
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {content_length}\r\nConnection: close\r\n\r\n"
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.flush();
}

/// Parse a `Content-Range: bytes {start}-{end}/{total}` header value.
fn parse_content_range(v: &str) -> Option<(u64, u64, u64)> {
    let rest = v.trim().strip_prefix("bytes ")?;
    let (range, total) = rest.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?, total.parse().ok()?))
}

fn handle_connection(mut stream: TcpStream, store: Arc<Mutex<HashMap<String, Vec<u8>>>>) {
    let Some(req) = read_request(&mut stream) else {
        return;
    };

    if req.path.starts_with("/authed-bearer/") {
        let ok = req
            .headers
            .get("authorization")
            .map(|v| v == BEARER_EXPECTED)
            .unwrap_or(false);
        if !ok {
            write_response(&mut stream, 401, "Unauthorized", b"");
            return;
        }
    }
    if req.path.starts_with("/authed-basic/") {
        let ok = req
            .headers
            .get("authorization")
            .map(|v| v == BASIC_EXPECTED)
            .unwrap_or(false);
        if !ok {
            write_response(&mut stream, 401, "Unauthorized", b"");
            return;
        }
    }

    match req.method.as_str() {
        "PUT" | "POST" => {
            let mut map = store.lock().unwrap();
            if let Some(range) = req.headers.get("content-range") {
                let Some((start, end, total)) = parse_content_range(range) else {
                    write_response(&mut stream, 400, "Bad Request", b"");
                    return;
                };
                let entry = map.entry(req.path.clone()).or_default();
                if (entry.len() as u64) < total {
                    entry.resize(total as usize, 0);
                }
                let (s, e) = (start as usize, end as usize);
                if e >= entry.len() || (e - s + 1) != req.body.len() {
                    write_response(&mut stream, 416, "Range Not Satisfiable", b"");
                    return;
                }
                entry[s..=e].copy_from_slice(&req.body);
            } else {
                map.insert(req.path.clone(), req.body.clone());
            }
            write_response(&mut stream, 200, "OK", b"");
        }
        "HEAD" => {
            let map = store.lock().unwrap();
            match map.get(&req.path) {
                Some(data) => write_head_response(&mut stream, 200, "OK", data.len()),
                None => write_head_response(&mut stream, 404, "Not Found", 0),
            }
        }
        "GET" => {
            let map = store.lock().unwrap();
            match map.get(&req.path) {
                Some(data) => write_response(&mut stream, 200, "OK", data),
                None => write_response(&mut stream, 404, "Not Found", b""),
            }
        }
        "DELETE" => {
            let mut map = store.lock().unwrap();
            if map.remove(&req.path).is_some() {
                write_response(&mut stream, 204, "No Content", b"");
            } else {
                write_response(&mut stream, 404, "Not Found", b"");
            }
        }
        _ => write_response(&mut stream, 405, "Method Not Allowed", b""),
    }
}

// ============================================================================================
// Test scaffolding (mirrors tests/sftp_atmoz.rs)
// ============================================================================================

/// A fake credential service returning canned HTTP credentials JSON for one secret name.
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
        Ok(Some(
            serde_json::json!({ "bearerToken": "vault-bearer-tok" }),
        ))
    }
}

fn base_egress(url: String) -> HttpEgress {
    HttpEgress {
        url,
        method: None,
        headers: Default::default(),
        bearer_token: None,
        username: None,
        password: None,
        credentials: None,
        resumable: None,
        chunk_bytes: None,
        checksum_algorithm: None,
    }
}

fn build_http(
    cfg: HttpEgress,
    creds: bool,
    store: Option<Arc<dyn StateStore>>,
) -> SharedDestination {
    let egress = EgressCfg::Http(Box::new(cfg));
    let mut deps = DestDeps::default();
    if creds {
        deps = deps.with_credentials(Some(Arc::new(FakeCreds)));
    }
    if let Some(s) = store {
        deps = deps.with_store(s);
    }
    build_destination(&egress, &deps).expect("build http dest")
}

fn work_item(src_root: &std::path::Path, rel: &str) -> WorkItem {
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
        Arc::new(TokenBucket::new(
            mb_per_sec * MIB as u64,
            Arc::new(SystemClock),
        )),
        Arc::new(TokenBucket::unlimited()),
    )
}

fn committed_bytes(store: &Arc<dyn StateStore>, rel: &str) -> Option<u64> {
    store
        .load_resume(INSTANCE, rel, "http")
        .unwrap()
        .map(|rs| rs.bytes_committed)
}

#[tokio::test]
async fn http_inprocess_full_scenario() {
    let server = TestServer::start();
    let src = tempfile::tempdir().unwrap();

    // ---- small file round trip via ambient Basic auth; checksum/size/none verify ------------------
    {
        let mut cfg = base_egress(server.base_url("/authed-basic/upload"));
        cfg.username = Some("alice".into());
        cfg.password = Some("s3cr3t".into());
        let dest = build_http(cfg, false, None);

        let data = payload(64 * 1024 + 7);
        std::fs::write(src.path().join("a.bin"), &data).unwrap();
        let item = work_item(src.path(), "a.bin");

        let delivered = dest
            .deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .expect("deliver small (basic auth)");
        assert_eq!(delivered.bytes, data.len() as u64);

        dest.verify(&item, &delivered, Verify::Checksum)
            .await
            .expect("verify checksum");
        dest.verify(&item, &delivered, Verify::Size)
            .await
            .expect("verify size");
        dest.verify(&item, &delivered, Verify::None)
            .await
            .expect("verify none");

        let back = server
            .get("/authed-basic/upload/a.bin")
            .expect("object present on server");
        assert_eq!(back, data, "server-side bytes match the source");
    }

    // ---- large file (chunked ranged PUT) via {"$secret":"…"} bearer credential + resume checkpoint --
    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
    let big_rel = unique("big") + ".bin";
    let data = payload(12 * MIB);
    std::fs::write(src.path().join(&big_rel), &data).unwrap();

    {
        let mut cfg = base_egress(server.base_url("/authed-bearer/upload"));
        cfg.credentials = Some(serde_json::json!({ "$secret": "http-creds" }));
        cfg.chunk_bytes = Some(1024 * 1024); // force several chunks over a 12 MiB file
        let dest = build_http(cfg, true, Some(store.clone()));
        let item = work_item(src.path(), &big_rel);

        let delivered = dest
            .deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .expect("deliver large (bearer $secret)");
        assert_eq!(delivered.bytes, data.len() as u64);
        dest.verify(&item, &delivered, Verify::Checksum)
            .await
            .expect("verify checksum (large)");

        let back = server
            .get(&format!("/authed-bearer/upload/{big_rel}"))
            .expect("large object present");
        assert_eq!(back, data, "large file round trip byte-for-byte");
        assert!(
            store
                .load_resume(INSTANCE, &big_rel, "http")
                .unwrap()
                .is_none(),
            "resume checkpoint cleared after a successful delivery"
        );
    }

    // ---- resume: interrupt after a partial checkpoint, restart, stream only the tail --------------
    {
        let resume_rel = unique("resume") + ".bin";
        std::fs::write(src.path().join(&resume_rel), &data).unwrap();

        let mut cfg = base_egress(server.base_url("/upload"));
        cfg.chunk_bytes = Some(512 * 1024);
        let dest = build_http(cfg, false, Some(store.clone()));
        let item = work_item(src.path(), &resume_rel);

        let dest_c = dest.clone();
        let item_c = item.clone();
        let bw = capped_bandwidth(3); // slow enough that the poll below reliably catches a checkpoint
        let handle = tokio::spawn(async move {
            let _ = dest_c
                .deliver(&item_c, None, &ProgressSink::noop(), &bw)
                .await;
        });

        let mut committed = 0u64;
        for _ in 0..800 {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
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
            .load_resume(INSTANCE, &resume_rel, "http")
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

        dest.verify(&item, &delivered, Verify::Checksum)
            .await
            .expect("verify checksum after resume");
        let back = server
            .get(&format!("/upload/{resume_rel}"))
            .expect("resumed object present");
        assert_eq!(back, data, "resumed transfer is byte-for-byte correct");
    }

    // ---- idempotent re-delivery: stable relpath overwrite (FR-REL-4-equivalent) -------------------
    {
        let cfg = base_egress(server.base_url("/upload"));
        let dest = build_http(cfg, false, None);
        let dup_rel = unique("dup") + ".bin";

        let v1 = payload(20_000);
        std::fs::write(src.path().join(&dup_rel), &v1).unwrap();
        let item1 = work_item(src.path(), &dup_rel);
        let d1 = dest
            .deliver(&item1, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .expect("deliver v1");
        dest.verify(&item1, &d1, Verify::Checksum)
            .await
            .expect("verify v1");
        assert_eq!(server.get(&format!("/upload/{dup_rel}")).unwrap(), v1);

        let v2 = payload(37_000);
        assert_ne!(v1, v2);
        std::fs::write(src.path().join(&dup_rel), &v2).unwrap();
        let item2 = work_item(src.path(), &dup_rel);
        let d2 = dest
            .deliver(&item2, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .expect("deliver v2 (overwrite)");
        assert_eq!(d2.bytes, v2.len() as u64);
        dest.verify(&item2, &d2, Verify::Checksum)
            .await
            .expect("verify v2");
        assert_eq!(
            server.get(&format!("/upload/{dup_rel}")).unwrap(),
            v2,
            "stable relpath overwrote identically with the new content"
        );
    }

    // ---- abort: give-up cleanup DELETEs a partial object (and is idempotent / a no-op with nothing
    //      committed) -----------------------------------------------------------------------------
    {
        let cfg = base_egress(server.base_url("/upload"));
        let dest = build_http(cfg, false, None);

        let abort_rel = unique("abort") + ".bin";
        let abort_path = format!("/upload/{abort_rel}");
        server.put_direct(&abort_path, vec![1, 2, 3]);
        let item = work_item(src.path(), &abort_rel);
        let resume = ResumeState {
            bytes_committed: 3,
            token: serde_json::json!({
                "http": { "url": server.base_url(&abort_path), "size": 10, "mtimeMs": 0, "bytesCommitted": 3 }
            }),
        };
        dest.abort(&item, &resume)
            .await
            .expect("abort deletes the partial object");
        assert!(
            server.get(&abort_path).is_none(),
            "abort removed the orphaned object"
        );
        dest.abort(&item, &resume)
            .await
            .expect("abort tolerates an already-missing object (404)");

        // Nothing committed → no DELETE is even attempted; a pre-existing object at the same URL
        // (e.g. left by an unrelated prior delivery) must be left alone.
        let noop_rel = unique("abort-noop") + ".bin";
        let noop_path = format!("/upload/{noop_rel}");
        server.put_direct(&noop_path, vec![9, 9, 9]);
        let noop_resume = ResumeState {
            bytes_committed: 0,
            token: serde_json::json!({
                "http": { "url": server.base_url(&noop_path), "size": 10, "mtimeMs": 0, "bytesCommitted": 0 }
            }),
        };
        dest.abort(&item, &noop_resume)
            .await
            .expect("abort with nothing committed is a no-op");
        assert_eq!(
            server.get(&noop_path),
            Some(vec![9, 9, 9]),
            "untouched object survives a no-op abort"
        );
    }

    // ---- method: POST (single-shot, resume forced off) ---------------------------------------------
    {
        let mut cfg = base_egress(server.base_url("/upload"));
        cfg.method = Some("POST".into());
        let dest = build_http(cfg, false, None);
        assert!(!dest.supports_resume(), "POST forces resume off");

        let post_rel = unique("post") + ".bin";
        let data = payload(9_000);
        std::fs::write(src.path().join(&post_rel), &data).unwrap();
        let item = work_item(src.path(), &post_rel);
        let delivered = dest
            .deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .expect("POST single-shot deliver");
        assert_eq!(delivered.bytes, data.len() as u64);
        dest.verify(&item, &delivered, Verify::Checksum)
            .await
            .expect("verify POST upload");
        assert_eq!(server.get(&format!("/upload/{post_rel}")).unwrap(), data);
    }
}

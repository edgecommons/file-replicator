//! # file-replicator — local-directory destination (DESIGN §10.2, P1)
//!
//! The one shipping P1 backend: replicate a source file into a destination directory tree, preserving
//! the recursive subtree (`item.relpath`) under the configured `egress.path` root. Delivery is
//! crash-safe by construction (DESIGN §13.2):
//!
//! 1. Stream the source → a sibling **temp file** (`.<name>.<rand>.part`) in the *final* directory
//!    (same filesystem, so the rename is atomic), computing the single-pass [`Checksum`] on the bytes
//!    as they are written and driving byte progress through the [`Bandwidth`] governor.
//! 2. Optionally `fsync` the temp file (and its parent dir) for on-disk durability.
//! 3. **Atomic rename** temp → final key. After this the object is LIVE at its deterministic key — the
//!    call *is* the delivery commit. Same `relpath` → same key → overwrite-identical (FR-REL-4).
//!
//! [`verify`](LocalDest::verify) re-opens the delivered file and applies the [`Verify`] policy: re-hash
//! (compare against the [`Delivered::checksum`] captured on write), size, or none. It is idempotent, so
//! crash recovery can re-run it against an already-live object.
//!
//! Resume: the local backend reports [`supports_resume`](LocalDest::supports_resume) `= true`, but a
//! local resume simply **restarts the copy** into a fresh temp file (there is no partial-append hazard —
//! the atomic rename means an interrupted transfer only ever leaves an orphan temp file, never a
//! half-written final object). The [`ResumeState`] shape is honored so the S3 backend (P2) can carry
//! `{uploadId, parts}` through the same seam; [`abort`](LocalDest::abort) deletes the orphan temp file.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

use crate::config::{LocalEgress, Verify};
use crate::domain::{Checksum, Delivered, ProgressSink, ResumeState, WorkItem};
use crate::error::{ReplError, Result};
use crate::integrity::{self, Algorithm, Hasher};
use crate::ratelimit::Bandwidth;

use super::Destination;

/// Stream-copy chunk size. Matches [`integrity::hash_reader`]'s 64 KiB window so the throttle
/// granularity and the read granularity line up.
const COPY_CHUNK: usize = 64 * 1024;

/// The local-directory backend. Writes under `root`, preserving `item.relpath`; hashes with `algo`;
/// `fsync`es before rename when requested.
pub struct LocalDest {
    root: PathBuf,
    fsync: bool,
    algo: Algorithm,
}

impl LocalDest {
    /// Build a local destination from its typed egress config and the destination checksum algorithm
    /// (from `completion.verify` selecting the hash; the factory passes the resolved [`Algorithm`]).
    pub fn new(cfg: &LocalEgress, algo: Algorithm) -> Self {
        LocalDest {
            root: cfg.path.clone(),
            fsync: cfg.fsync,
            algo,
        }
    }

    /// The deterministic final key for `item`: `root` joined with the forward-slash `relpath`,
    /// resolved component-by-component so it stays inside `root` on every OS.
    fn final_path(&self, item: &WorkItem) -> PathBuf {
        let mut p = self.root.clone();
        for seg in item.relpath.split('/') {
            if seg.is_empty() || seg == "." || seg == ".." {
                continue;
            }
            p.push(seg);
        }
        p
    }
}

/// Pick a temp-file path in the same directory as `final_path` so the eventual rename is same-volume
/// and therefore atomic. Name: `.<filename>.<rand>.part` (hidden, collision-resistant).
fn temp_path_for(final_path: &Path, rand_tag: u64) -> PathBuf {
    let dir = final_path.parent().unwrap_or_else(|| Path::new("."));
    let name = final_path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "file".to_string());
    dir.join(format!(".{name}.{rand_tag:016x}.part"))
}

/// fsync a directory entry so a create/rename is durable (best-effort on platforms without dir sync).
async fn fsync_dir(dir: &Path) -> Result<()> {
    // Opening a directory for sync is not supported on all platforms (notably Windows). Treat an
    // open failure as "directory sync unavailable" rather than a transfer error.
    match fs::File::open(dir).await {
        Ok(f) => {
            // A directory handle may reject sync_all on some platforms; ignore that specific failure.
            let _ = f.sync_all().await;
            Ok(())
        }
        Err(_) => Ok(()),
    }
}

#[async_trait]
impl Destination for LocalDest {
    fn kind(&self) -> &'static str {
        "local"
    }

    fn supports_resume(&self) -> bool {
        // Local resume restarts the copy into a fresh temp file (see module docs); it never leaves a
        // partial final object, so "resume" is safe and idempotent.
        true
    }

    async fn deliver(
        &self,
        item: &WorkItem,
        resume: Option<ResumeState>,
        progress: &ProgressSink,
        bw: &Bandwidth,
    ) -> Result<Delivered> {
        let final_path = self.final_path(item);

        // Create the destination subtree so the recursive layout is preserved.
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).await.map_err(ReplError::classify_io)?;
        }

        // A prior interrupted attempt may have left an orphan temp file recorded in `resume`; drop it
        // before restarting the copy (local resume = restart, module docs).
        if let Some(prev) = resume.as_ref().and_then(resume_temp_path) {
            let _ = fs::remove_file(&prev).await;
        }

        // Derive a per-attempt temp name. Mix identity + attempt so concurrent/retried transfers of
        // different items never collide on the same temp path.
        let rand_tag = temp_tag(item);
        let temp = temp_path_for(&final_path, rand_tag);

        // Open the source and stream it into the temp file, hashing + throttling as we go.
        let src = fs::File::open(&item.abs_source)
            .await
            .map_err(ReplError::classify_io)?;
        let mut reader = BufReader::with_capacity(COPY_CHUNK, src);

        // Own a writer we can flush/sync/drop deterministically before the rename.
        let mut writer = fs::File::create(&temp)
            .await
            .map_err(ReplError::classify_io)?;

        let mut hasher = Hasher::new(self.algo);
        let mut buf = vec![0u8; COPY_CHUNK];
        let mut total: u64 = 0;

        let copy_result = async {
            loop {
                let n = reader.read(&mut buf).await.map_err(ReplError::classify_io)?;
                if n == 0 {
                    break;
                }
                // Pass BOTH the per-instance and global bandwidth caps before writing this chunk.
                bw.throttle(n as u64).await;
                writer
                    .write_all(&buf[..n])
                    .await
                    .map_err(ReplError::classify_io)?;
                hasher.update(&buf[..n]);
                total += n as u64;
                progress.report(total);
            }
            writer.flush().await.map_err(ReplError::classify_io)?;
            if self.fsync {
                writer.sync_all().await.map_err(ReplError::classify_io)?;
            }
            Ok::<(), ReplError>(())
        }
        .await;

        // Ensure the temp handle is closed before rename, and clean it up on any streaming failure.
        drop(writer);
        if let Err(e) = copy_result {
            let _ = fs::remove_file(&temp).await;
            return Err(e);
        }

        // Atomic publish: temp → final. `rename` replaces an existing final object (overwrite-identical,
        // FR-REL-4). On Windows `rename` fails if the target exists, so remove-then-rename there.
        if let Err(e) = fs::rename(&temp, &final_path).await {
            if final_path.exists() {
                fs::remove_file(&final_path)
                    .await
                    .map_err(ReplError::classify_io)?;
                fs::rename(&temp, &final_path)
                    .await
                    .map_err(ReplError::classify_io)?;
            } else {
                let _ = fs::remove_file(&temp).await;
                return Err(ReplError::classify_io(e));
            }
        }

        // Durability of the rename itself.
        if self.fsync {
            if let Some(parent) = final_path.parent() {
                fsync_dir(parent).await?;
            }
        }

        let checksum = hasher.finish();
        let handle = serde_json::json!({ "path": final_path.to_string_lossy() });
        Ok(Delivered {
            bytes: total,
            checksum,
            handle,
        })
    }

    async fn verify(&self, item: &WorkItem, delivered: &Delivered, policy: Verify) -> Result<()> {
        let final_path = self.final_path(item);

        match policy {
            Verify::None => Ok(()),
            Verify::Size => {
                let meta = fs::metadata(&final_path)
                    .await
                    .map_err(ReplError::classify_io)?;
                integrity::verify_size(delivered.bytes, meta.len())
            }
            Verify::Checksum => {
                // Re-hash the delivered object and compare to the checksum captured while writing.
                let algo = checksum_algo(&delivered.checksum).unwrap_or(self.algo);
                let path = final_path.clone();
                let (_, actual) = tokio::task::spawn_blocking(move || {
                    let mut f = std::fs::File::open(&path)?;
                    integrity::hash_reader(&mut f, algo)
                })
                .await
                .map_err(|e| ReplError::Transient(format!("verify task join: {e}")))?
                .map_err(ReplError::classify_io)?;
                integrity::verify_checksum(&delivered.checksum, &actual)
            }
        }
    }

    async fn abort(&self, item: &WorkItem, resume: &ResumeState) -> Result<()> {
        // Remove the orphan temp file if the resume state recorded one; otherwise best-effort clean the
        // deterministic temp name for this item. Idempotent — missing files are fine.
        if let Some(prev) = resume_temp_path(resume) {
            let _ = fs::remove_file(&prev).await;
        }
        let temp = temp_path_for(&self.final_path(item), temp_tag(item));
        let _ = fs::remove_file(&temp).await;
        Ok(())
    }
}

/// Deterministic temp tag for an item so retries reuse a predictable name (still per-item unique via
/// the relpath hash + attempt counter).
fn temp_tag(item: &WorkItem) -> u64 {
    use std::hash::{Hash, Hasher as _};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    item.instance.hash(&mut h);
    item.relpath.hash(&mut h);
    item.attempts.hash(&mut h);
    h.finish()
}

/// Extract the temp-file path recorded in a [`ResumeState`] token, if any.
fn resume_temp_path(resume: &ResumeState) -> Option<PathBuf> {
    resume
        .token
        .get("temp")
        .and_then(|v| v.as_str())
        .map(PathBuf::from)
}

/// The [`Algorithm`] that produced a given [`Checksum`], so `verify` re-hashes with the matching one.
fn checksum_algo(c: &Checksum) -> Option<Algorithm> {
    match c {
        Checksum::Crc32c(_) => Some(Algorithm::Crc32c),
        Checksum::Sha256(_) => Some(Algorithm::Sha256),
        Checksum::None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ItemState;
    use std::io::Write as _;

    fn work_item(root: &Path, relpath: &str) -> WorkItem {
        let abs = root.join(relpath.replace('/', std::path::MAIN_SEPARATOR_STR));
        let size = std::fs::metadata(&abs).map(|m| m.len()).unwrap_or(0);
        WorkItem {
            instance: "inst".to_string(),
            relpath: relpath.to_string(),
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

    fn write_file(path: &Path, bytes: &[u8]) {
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p).unwrap();
        }
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(bytes).unwrap();
        f.sync_all().unwrap();
    }

    #[tokio::test]
    async fn deliver_copies_bytes_and_verifies() {
        let src_root = tempfile::tempdir().unwrap();
        let dst_root = tempfile::tempdir().unwrap();
        let data = b"hello file-replicator local destination";
        write_file(&src_root.path().join("a.txt"), data);

        let dest = LocalDest::new(
            &LocalEgress {
                path: dst_root.path().to_path_buf(),
                fsync: false,
            },
            Algorithm::Crc32c,
        );
        let item = work_item(src_root.path(), "a.txt");
        let delivered = dest
            .deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .unwrap();

        assert_eq!(delivered.bytes, data.len() as u64);
        let out = dst_root.path().join("a.txt");
        assert_eq!(std::fs::read(&out).unwrap(), data);
        // No leftover temp files in the destination dir.
        let leftovers: Vec<_> = std::fs::read_dir(dst_root.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".part"))
            .collect();
        assert!(leftovers.is_empty(), "temp file must be renamed away");

        dest.verify(&item, &delivered, Verify::Checksum)
            .await
            .unwrap();
        dest.verify(&item, &delivered, Verify::Size).await.unwrap();
        dest.verify(&item, &delivered, Verify::None).await.unwrap();
    }

    #[tokio::test]
    async fn deliver_preserves_recursive_subtree() {
        let src_root = tempfile::tempdir().unwrap();
        let dst_root = tempfile::tempdir().unwrap();
        let rel = "deep/nested/dir/report.csv";
        write_file(
            &src_root.path().join("deep/nested/dir/report.csv"),
            b"col1,col2\n1,2\n",
        );

        let dest = LocalDest::new(
            &LocalEgress {
                path: dst_root.path().to_path_buf(),
                fsync: true, // exercise the fsync path too
            },
            Algorithm::Sha256,
        );
        let item = work_item(src_root.path(), rel);
        let delivered = dest
            .deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .unwrap();

        let out = dst_root.path().join("deep/nested/dir/report.csv");
        assert!(out.exists(), "subtree must be recreated under the dest root");
        assert_eq!(std::fs::read(&out).unwrap(), b"col1,col2\n1,2\n");
        dest.verify(&item, &delivered, Verify::Checksum)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn verify_detects_checksum_mismatch() {
        let src_root = tempfile::tempdir().unwrap();
        let dst_root = tempfile::tempdir().unwrap();
        write_file(&src_root.path().join("x.bin"), b"original-content");

        let dest = LocalDest::new(
            &LocalEgress {
                path: dst_root.path().to_path_buf(),
                fsync: false,
            },
            Algorithm::Crc32c,
        );
        let item = work_item(src_root.path(), "x.bin");
        let delivered = dest
            .deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .unwrap();

        // Corrupt the delivered object out from under verify → re-hash must not match.
        write_file(&dst_root.path().join("x.bin"), b"tampered-content!!");
        let err = dest
            .verify(&item, &delivered, Verify::Checksum)
            .await
            .unwrap_err();
        assert!(matches!(err, ReplError::Integrity(_)), "got {err:?}");

        // Size policy also catches the differing length.
        let err = dest.verify(&item, &delivered, Verify::Size).await.unwrap_err();
        assert!(matches!(err, ReplError::Integrity(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn deliver_is_idempotent_overwrite() {
        let src_root = tempfile::tempdir().unwrap();
        let dst_root = tempfile::tempdir().unwrap();
        write_file(&src_root.path().join("dup.txt"), b"v1");

        let dest = LocalDest::new(
            &LocalEgress {
                path: dst_root.path().to_path_buf(),
                fsync: false,
            },
            Algorithm::Crc32c,
        );
        let mut item = work_item(src_root.path(), "dup.txt");
        dest.deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .unwrap();

        // Re-deliver a new source content to the SAME relpath → same key, overwrite.
        write_file(&src_root.path().join("dup.txt"), b"v2-longer");
        item = work_item(src_root.path(), "dup.txt");
        let d2 = dest
            .deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .unwrap();
        assert_eq!(d2.bytes, b"v2-longer".len() as u64);
        assert_eq!(std::fs::read(dst_root.path().join("dup.txt")).unwrap(), b"v2-longer");
    }

    #[tokio::test]
    async fn deliver_missing_source_is_permanent() {
        let dst_root = tempfile::tempdir().unwrap();
        let dest = LocalDest::new(
            &LocalEgress {
                path: dst_root.path().to_path_buf(),
                fsync: false,
            },
            Algorithm::Crc32c,
        );
        let mut item = work_item(dst_root.path(), "does-not-exist.txt");
        item.abs_source = PathBuf::from(dst_root.path()).join("nope").join("gone.txt");
        let err = dest
            .deliver(&item, None, &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .unwrap_err();
        assert!(err.is_permanent(), "NotFound source must classify permanent: {err:?}");
    }

    #[tokio::test]
    async fn progress_reports_total_bytes() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        let src_root = tempfile::tempdir().unwrap();
        let dst_root = tempfile::tempdir().unwrap();
        // Larger than one COPY_CHUNK so progress is reported multiple times.
        let data = vec![7u8; COPY_CHUNK * 3 + 123];
        write_file(&src_root.path().join("big.bin"), &data);

        let last = Arc::new(AtomicU64::new(0));
        let calls = Arc::new(AtomicU64::new(0));
        let l = last.clone();
        let c = calls.clone();
        let sink = ProgressSink::new(move |n| {
            l.store(n, Ordering::SeqCst);
            c.fetch_add(1, Ordering::SeqCst);
        });

        let dest = LocalDest::new(
            &LocalEgress {
                path: dst_root.path().to_path_buf(),
                fsync: false,
            },
            Algorithm::Crc32c,
        );
        let item = work_item(src_root.path(), "big.bin");
        let delivered = dest
            .deliver(&item, None, &sink, &Bandwidth::unlimited())
            .await
            .unwrap();
        assert_eq!(delivered.bytes, data.len() as u64);
        assert_eq!(last.load(Ordering::SeqCst), data.len() as u64);
        assert!(calls.load(Ordering::SeqCst) >= 4, "multi-chunk progress expected");
    }

    #[tokio::test]
    async fn abort_removes_temp_and_is_idempotent() {
        let dst_root = tempfile::tempdir().unwrap();
        let dest = LocalDest::new(
            &LocalEgress {
                path: dst_root.path().to_path_buf(),
                fsync: false,
            },
            Algorithm::Crc32c,
        );
        let item = work_item(dst_root.path(), "sub/thing.dat");
        // Simulate an orphan temp file recorded in resume state.
        let final_path = dest.final_path(&item);
        std::fs::create_dir_all(final_path.parent().unwrap()).unwrap();
        let orphan = temp_path_for(&final_path, 0xdead_beef);
        write_file(&orphan, b"partial");
        let resume = ResumeState {
            bytes_committed: 7,
            token: serde_json::json!({ "temp": orphan.to_string_lossy() }),
        };
        dest.abort(&item, &resume).await.unwrap();
        assert!(!orphan.exists(), "abort must remove the orphan temp file");
        // Idempotent second call.
        dest.abort(&item, &resume).await.unwrap();
    }

    #[tokio::test]
    async fn resume_restarts_and_cleans_prior_temp() {
        let src_root = tempfile::tempdir().unwrap();
        let dst_root = tempfile::tempdir().unwrap();
        write_file(&src_root.path().join("r.txt"), b"resume-me");

        let dest = LocalDest::new(
            &LocalEgress {
                path: dst_root.path().to_path_buf(),
                fsync: false,
            },
            Algorithm::Crc32c,
        );
        let item = work_item(src_root.path(), "r.txt");
        let final_path = dest.final_path(&item);
        std::fs::create_dir_all(final_path.parent().unwrap()).unwrap();
        let stale = temp_path_for(&final_path, 0x1234);
        write_file(&stale, b"stale-partial-bytes");
        let resume = ResumeState {
            bytes_committed: 5,
            token: serde_json::json!({ "temp": stale.to_string_lossy() }),
        };

        let delivered = dest
            .deliver(&item, Some(resume), &ProgressSink::noop(), &Bandwidth::unlimited())
            .await
            .unwrap();
        assert_eq!(delivered.bytes, b"resume-me".len() as u64);
        assert!(!stale.exists(), "prior temp must be cleaned on resume-restart");
        assert_eq!(std::fs::read(dst_root.path().join("r.txt")).unwrap(), b"resume-me");
    }

    #[test]
    fn final_path_strips_traversal() {
        let dest = LocalDest::new(
            &LocalEgress {
                path: PathBuf::from("/root"),
                fsync: false,
            },
            Algorithm::Crc32c,
        );
        let mut item = work_item(Path::new("/src"), "a/../b/./c.txt");
        item.relpath = "a/../b/./c.txt".to_string();
        let p = dest.final_path(&item);
        // ".." and "." segments are dropped; result stays under root.
        assert!(p.starts_with("/root"));
        assert!(p.ends_with("c.txt"));
        assert!(!p.to_string_lossy().contains(".."));
    }
}

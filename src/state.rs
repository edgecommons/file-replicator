//! # file-replicator — durable state (DESIGN §14)
//!
//! The [`StateStore`] trait is the write-ahead durable store behind the queue, resume checkpoints,
//! per-instance activation, and statistics (DESIGN §14). [`SqliteStore`] is the P1 implementation
//! (rusqlite, `bundled`, WAL mode), realizing the DESIGN §14.2 schema.
//!
//! The trait is **synchronous** to match blocking rusqlite: the async engine calls O(1) row ops
//! inline and pushes bulk scans through `spawn_blocking`. One DB is shared across instances, keyed by
//! `instance`. Every state transition is persisted *before* the side effect it authorizes (§13.2):
//! `Ready → InProgress` ([`claim_ready`](StateStore::claim_ready)) precedes the transfer, `Verified`
//! ([`set_state`](StateStore::set_state)) precedes the source delete/archive, so a crash at any point
//! is recovered idempotently via [`recover_incomplete`](StateStore::recover_incomplete).

use std::path::Path;
use std::sync::Mutex;

use rusqlite::{params, Connection, OptionalExtension, Row};

use crate::domain::{ItemState, ResumeState, WorkItem};
use crate::error::{ReplError, Result};

/// Durable, crash-safe state for the queue, resume checkpoints, activation, and stats.
pub trait StateStore: Send + Sync {
    // ---- work items -----------------------------------------------------------------------------

    /// Insert (or refresh) a `Ready` work item. Only ready items enter the store (DESIGN §9).
    ///
    /// `mtime_ms` is the source file's last-modified time and, together with `size`, is the
    /// change signature the store uses to tell a genuinely-new file from a re-discovery of the same
    /// one. On re-discovery of a **terminal** row (`Completed`/`Quarantined`/`Retained`) whose
    /// `size`/`mtime_ms` differ from what was stored, the item is reset to `Ready` — the
    /// fixed/rotating-filename producer pattern (a daily `export.csv` overwritten in place, then
    /// deleted on success) MUST be re-replicated rather than silently dropped (FR-REL-1, NFR-5).
    /// An unchanged terminal row is preserved so a completed file is never resurrected in a loop.
    fn upsert_ready(&self, instance: &str, relpath: &str, size: u64, mtime_ms: i64, now: i64)
        -> Result<()>;

    /// Atomically claim up to `limit` oldest `Ready` items for `instance` whose `next_attempt_at`
    /// ≤ `now`, transitioning `Ready → InProgress` (write-ahead). Ordered by `discovered_at`.
    fn claim_ready(&self, instance: &str, limit: usize, now: i64) -> Result<Vec<WorkItem>>;

    /// Force an item to `state`.
    fn set_state(&self, instance: &str, relpath: &str, state: ItemState, now: i64) -> Result<()>;

    /// Failure path: `attempts += 1`, record `err`, set `next_attempt_at`, move to `next_state`
    /// (`Failed` while retrying, `Exhausted` on give-up).
    fn record_attempt(
        &self,
        instance: &str,
        relpath: &str,
        err: &str,
        next_state: ItemState,
        next_attempt_at: i64,
        now: i64,
    ) -> Result<()>;

    /// Persist streamed-byte progress for an in-flight item.
    fn set_bytes_done(&self, instance: &str, relpath: &str, bytes: u64, now: i64) -> Result<()>;

    /// Fetch a single item by identity.
    fn get(&self, instance: &str, relpath: &str) -> Result<Option<WorkItem>>;

    /// Crash recovery: items left `InProgress` or `Verified` by a prior run (DESIGN §13.2).
    fn recover_incomplete(&self, instance: &str) -> Result<Vec<WorkItem>>;

    /// All items in a given state (diagnostics / recovery).
    fn list_by_state(&self, instance: &str, state: ItemState) -> Result<Vec<WorkItem>>;

    // ---- resume checkpoints ---------------------------------------------------------------------

    fn save_resume(&self, instance: &str, relpath: &str, dest: &str, r: &ResumeState) -> Result<()>;
    fn load_resume(&self, instance: &str, relpath: &str, dest: &str)
        -> Result<Option<ResumeState>>;
    fn clear_resume(&self, instance: &str, relpath: &str, dest: &str) -> Result<()>;

    // ---- activation (persisted; wins over config `enabled`, DESIGN §7.5) ------------------------

    fn load_activation(&self, instance: &str) -> Result<Option<Activation>>;
    fn set_activation(&self, instance: &str, active: bool, source: &str, now: i64) -> Result<()>;
    /// Reset path → drop the override so config `enabled` drives again.
    fn clear_activation(&self, instance: &str) -> Result<()>;

    // ---- stats (metrics now; get-status in P3) --------------------------------------------------

    fn bump_stats(&self, instance: &str, d: StatsDelta) -> Result<()>;
    fn stats(&self, instance: &str) -> Result<Stats>;
}

/// Persisted activation override for an instance (DESIGN §7.5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Activation {
    pub active: bool,
    pub source: String,
    pub updated_at: i64,
}

/// An accumulating delta applied by [`StateStore::bump_stats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatsDelta {
    pub replicated: i64,
    pub failed: i64,
    pub bytes: i64,
}

/// Per-instance running counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    pub replicated: u64,
    pub failed: u64,
    pub bytes: u64,
}

// ---- SQLite implementation (DESIGN §14.2) -------------------------------------------------------

/// The DDL for the state DB. One DB per component data dir; instances share every table, keyed by
/// `instance`. `next_attempt_at` is the P1 addition to the §14.2 schema (the backoff re-claim gate,
/// see [`WorkItem::next_attempt_at`]). `resume.blob` is a JSON-serialized [`ResumeState`] so the S3
/// backend (P2) reuses the column for `{uploadId, completedParts}` with no migration.
const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS work_items(
  instance        TEXT    NOT NULL,
  relpath         TEXT    NOT NULL,
  state           TEXT    NOT NULL,
  size            INTEGER NOT NULL,
  mtime_ms        INTEGER NOT NULL DEFAULT 0,
  discovered_at   INTEGER NOT NULL,
  attempts        INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER NOT NULL DEFAULT 0,
  last_error      TEXT,
  bytes_done      INTEGER NOT NULL DEFAULT 0,
  updated_at      INTEGER NOT NULL,
  PRIMARY KEY(instance, relpath));
CREATE INDEX IF NOT EXISTS ix_ready ON work_items(instance, state, discovered_at);
CREATE TABLE IF NOT EXISTS resume(
  instance TEXT NOT NULL, relpath TEXT NOT NULL, dest TEXT NOT NULL, blob BLOB NOT NULL,
  PRIMARY KEY(instance, relpath, dest));
CREATE TABLE IF NOT EXISTS activation(
  instance TEXT PRIMARY KEY, active INTEGER NOT NULL, source TEXT NOT NULL, updated_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS stats(
  instance   TEXT PRIMARY KEY,
  replicated INTEGER NOT NULL DEFAULT 0,
  failed     INTEGER NOT NULL DEFAULT 0,
  bytes      INTEGER NOT NULL DEFAULT 0);
";

/// The column list for every `work_items` full-row read, in [`row_to_item`] order.
const ITEM_COLS: &str = "instance, relpath, state, size, discovered_at, attempts, \
                         next_attempt_at, last_error, bytes_done, updated_at";

/// Crash-safe durable state backed by SQLite in WAL mode (DESIGN §14).
///
/// The blocking [`Connection`] lives behind a [`Mutex`] so the store is `Send + Sync` for the async
/// engine; contention is negligible because the hot path is O(1) indexed row ops.
pub struct SqliteStore {
    conn: Mutex<Connection>,
}

impl SqliteStore {
    /// Open (creating if absent) the state DB at `path`, enable WAL, and apply the schema.
    ///
    /// WAL + `synchronous=NORMAL` is the crash-safe recommended pairing: a committed transaction
    /// survives an application crash (the write-ahead guarantee §13.2 relies on), trading only the
    /// last commit's durability against an OS-level power loss.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        Self::init(conn)
    }

    /// An in-memory store for unit tests (no crash-recovery-across-reopen semantics).
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::init(conn)
    }

    fn init(conn: Connection) -> Result<Self> {
        // WAL survives an app crash; NORMAL keeps fsync off the per-commit hot path. busy_timeout
        // absorbs the brief WAL-writer contention between the async engine's inline row ops.
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        // A poisoned mutex means another thread panicked mid-transaction; the connection itself is
        // still consistent (SQLite rolls back the uncommitted txn), so recover the guard rather than
        // cascading the panic through every subsequent state op.
        self.conn.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// Map a full `work_items` row (in [`ITEM_COLS`] order) to a [`WorkItem`]. `abs_source` is derived
/// by the engine from `ingress.path` and is not a DB column, so it is reconstructed from `relpath`
/// alone here (the caller re-joins the ingress root when it needs a live path).
fn row_to_item(r: &Row<'_>) -> rusqlite::Result<WorkItem> {
    let state_str: String = r.get(2)?;
    let state = ItemState::from_str(&state_str).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            2,
            rusqlite::types::Type::Text,
            format!("unknown item state {state_str:?}").into(),
        )
    })?;
    let relpath: String = r.get(1)?;
    let abs_source = std::path::PathBuf::from(&relpath);
    Ok(WorkItem {
        instance: r.get(0)?,
        relpath,
        abs_source,
        state,
        size: r.get::<_, i64>(3)? as u64,
        discovered_at: r.get(4)?,
        attempts: r.get::<_, i64>(5)? as u32,
        next_attempt_at: r.get(6)?,
        last_error: r.get(7)?,
        bytes_done: r.get::<_, i64>(8)? as u64,
        updated_at: r.get(9)?,
    })
}

impl StateStore for SqliteStore {
    fn upsert_ready(
        &self,
        instance: &str,
        relpath: &str,
        size: u64,
        mtime_ms: i64,
        now: i64,
    ) -> Result<()> {
        // Insert as Ready. On re-discovery the behavior forks on whether the file actually changed:
        //   * A TERMINAL row (completed/quarantined/retained) whose size or mtime differs from the
        //     stored signature means a genuinely NEW file was written at the same relpath (the
        //     rotating/fixed-name producer pattern). Reset it to Ready and clear the prior attempt
        //     bookkeeping so it is claimed and re-replicated — otherwise it would be discovered every
        //     scan yet never claimed (claim only selects 'ready'), silently dropped (FR-REL-1/NFR-5).
        //   * Otherwise (a non-terminal/in-flight row, or a terminal row that is byte-for-byte the
        //     same file) preserve the state so a completed file is never resurrected into a loop and
        //     an in-flight transfer is never yanked back to Ready.
        // `changed` is evaluated in SQL against the existing row so the decision is atomic.
        self.lock().execute(
            "INSERT INTO work_items(instance, relpath, state, size, mtime_ms, discovered_at, updated_at)
             VALUES (?1, ?2, 'ready', ?3, ?4, ?5, ?5)
             ON CONFLICT(instance, relpath) DO UPDATE SET
               state = CASE
                 WHEN work_items.state IN ('completed','quarantined','retained')
                      AND (work_items.size <> excluded.size OR work_items.mtime_ms <> excluded.mtime_ms)
                 THEN 'ready' ELSE work_items.state END,
               attempts = CASE
                 WHEN work_items.state IN ('completed','quarantined','retained')
                      AND (work_items.size <> excluded.size OR work_items.mtime_ms <> excluded.mtime_ms)
                 THEN 0 ELSE work_items.attempts END,
               next_attempt_at = CASE
                 WHEN work_items.state IN ('completed','quarantined','retained')
                      AND (work_items.size <> excluded.size OR work_items.mtime_ms <> excluded.mtime_ms)
                 THEN 0 ELSE work_items.next_attempt_at END,
               last_error = CASE
                 WHEN work_items.state IN ('completed','quarantined','retained')
                      AND (work_items.size <> excluded.size OR work_items.mtime_ms <> excluded.mtime_ms)
                 THEN NULL ELSE work_items.last_error END,
               bytes_done = CASE
                 WHEN work_items.state IN ('completed','quarantined','retained')
                      AND (work_items.size <> excluded.size OR work_items.mtime_ms <> excluded.mtime_ms)
                 THEN 0 ELSE work_items.bytes_done END,
               discovered_at = CASE
                 WHEN work_items.state IN ('completed','quarantined','retained')
                      AND (work_items.size <> excluded.size OR work_items.mtime_ms <> excluded.mtime_ms)
                 THEN excluded.discovered_at ELSE work_items.discovered_at END,
               size = excluded.size,
               mtime_ms = excluded.mtime_ms,
               updated_at = excluded.updated_at",
            params![instance, relpath, size as i64, mtime_ms, now],
        )?;
        Ok(())
    }

    fn claim_ready(&self, instance: &str, limit: usize, now: i64) -> Result<Vec<WorkItem>> {
        let mut guard = self.lock();
        let tx = guard.transaction()?;
        // Oldest-ready-first, gated by the backoff clock; the ix_ready index serves the ORDER BY.
        let mut items: Vec<WorkItem> = {
            let mut stmt = tx.prepare(&format!(
                "SELECT {ITEM_COLS} FROM work_items
                 WHERE instance = ?1 AND state = 'ready' AND next_attempt_at <= ?2
                 ORDER BY discovered_at ASC LIMIT ?3"
            ))?;
            let rows = stmt.query_map(params![instance, now, limit as i64], row_to_item)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        // Write-ahead the Ready → InProgress transition before any transfer begins (§13.2).
        for it in &mut items {
            tx.execute(
                "UPDATE work_items SET state = 'in_progress', updated_at = ?3
                 WHERE instance = ?1 AND relpath = ?2",
                params![instance, it.relpath, now],
            )?;
            it.state = ItemState::InProgress;
            it.updated_at = now;
        }
        tx.commit()?;
        Ok(items)
    }

    fn set_state(&self, instance: &str, relpath: &str, state: ItemState, now: i64) -> Result<()> {
        self.lock().execute(
            "UPDATE work_items SET state = ?3, updated_at = ?4
             WHERE instance = ?1 AND relpath = ?2",
            params![instance, relpath, state.as_str(), now],
        )?;
        Ok(())
    }

    fn record_attempt(
        &self,
        instance: &str,
        relpath: &str,
        err: &str,
        next_state: ItemState,
        next_attempt_at: i64,
        now: i64,
    ) -> Result<()> {
        self.lock().execute(
            "UPDATE work_items
             SET attempts = attempts + 1, last_error = ?3, state = ?4,
                 next_attempt_at = ?5, updated_at = ?6
             WHERE instance = ?1 AND relpath = ?2",
            params![
                instance,
                relpath,
                err,
                next_state.as_str(),
                next_attempt_at,
                now
            ],
        )?;
        Ok(())
    }

    fn set_bytes_done(&self, instance: &str, relpath: &str, bytes: u64, now: i64) -> Result<()> {
        self.lock().execute(
            "UPDATE work_items SET bytes_done = ?3, updated_at = ?4
             WHERE instance = ?1 AND relpath = ?2",
            params![instance, relpath, bytes as i64, now],
        )?;
        Ok(())
    }

    fn get(&self, instance: &str, relpath: &str) -> Result<Option<WorkItem>> {
        let item = self
            .lock()
            .query_row(
                &format!(
                    "SELECT {ITEM_COLS} FROM work_items WHERE instance = ?1 AND relpath = ?2"
                ),
                params![instance, relpath],
                row_to_item,
            )
            .optional()?;
        Ok(item)
    }

    fn recover_incomplete(&self, instance: &str) -> Result<Vec<WorkItem>> {
        let guard = self.lock();
        let mut stmt = guard.prepare(&format!(
            "SELECT {ITEM_COLS} FROM work_items
             WHERE instance = ?1 AND state IN ('in_progress', 'verified')
             ORDER BY discovered_at ASC"
        ))?;
        let rows = stmt.query_map(params![instance], row_to_item)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn list_by_state(&self, instance: &str, state: ItemState) -> Result<Vec<WorkItem>> {
        let guard = self.lock();
        let mut stmt = guard.prepare(&format!(
            "SELECT {ITEM_COLS} FROM work_items
             WHERE instance = ?1 AND state = ?2 ORDER BY discovered_at ASC"
        ))?;
        let rows = stmt.query_map(params![instance, state.as_str()], row_to_item)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn save_resume(&self, instance: &str, relpath: &str, dest: &str, r: &ResumeState) -> Result<()> {
        let blob = serde_json::to_vec(r)
            .map_err(|e| ReplError::State(format!("serialize resume: {e}")))?;
        self.lock().execute(
            "INSERT INTO resume(instance, relpath, dest, blob) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(instance, relpath, dest) DO UPDATE SET blob = excluded.blob",
            params![instance, relpath, dest, blob],
        )?;
        Ok(())
    }

    fn load_resume(
        &self,
        instance: &str,
        relpath: &str,
        dest: &str,
    ) -> Result<Option<ResumeState>> {
        let blob: Option<Vec<u8>> = self
            .lock()
            .query_row(
                "SELECT blob FROM resume WHERE instance = ?1 AND relpath = ?2 AND dest = ?3",
                params![instance, relpath, dest],
                |r| r.get(0),
            )
            .optional()?;
        match blob {
            Some(b) => Ok(Some(
                serde_json::from_slice(&b)
                    .map_err(|e| ReplError::State(format!("deserialize resume: {e}")))?,
            )),
            None => Ok(None),
        }
    }

    fn clear_resume(&self, instance: &str, relpath: &str, dest: &str) -> Result<()> {
        self.lock().execute(
            "DELETE FROM resume WHERE instance = ?1 AND relpath = ?2 AND dest = ?3",
            params![instance, relpath, dest],
        )?;
        Ok(())
    }

    fn load_activation(&self, instance: &str) -> Result<Option<Activation>> {
        let act = self
            .lock()
            .query_row(
                "SELECT active, source, updated_at FROM activation WHERE instance = ?1",
                params![instance],
                |r| {
                    Ok(Activation {
                        active: r.get::<_, i64>(0)? != 0,
                        source: r.get(1)?,
                        updated_at: r.get(2)?,
                    })
                },
            )
            .optional()?;
        Ok(act)
    }

    fn set_activation(&self, instance: &str, active: bool, source: &str, now: i64) -> Result<()> {
        self.lock().execute(
            "INSERT INTO activation(instance, active, source, updated_at) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(instance) DO UPDATE
               SET active = excluded.active, source = excluded.source, updated_at = excluded.updated_at",
            params![instance, active as i64, source, now],
        )?;
        Ok(())
    }

    fn clear_activation(&self, instance: &str) -> Result<()> {
        self.lock().execute(
            "DELETE FROM activation WHERE instance = ?1",
            params![instance],
        )?;
        Ok(())
    }

    fn bump_stats(&self, instance: &str, d: StatsDelta) -> Result<()> {
        self.lock().execute(
            "INSERT INTO stats(instance, replicated, failed, bytes) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(instance) DO UPDATE
               SET replicated = replicated + excluded.replicated,
                   failed     = failed     + excluded.failed,
                   bytes      = bytes      + excluded.bytes",
            params![instance, d.replicated, d.failed, d.bytes],
        )?;
        Ok(())
    }

    fn stats(&self, instance: &str) -> Result<Stats> {
        let s = self
            .lock()
            .query_row(
                "SELECT replicated, failed, bytes FROM stats WHERE instance = ?1",
                params![instance],
                |r| {
                    Ok(Stats {
                        replicated: r.get::<_, i64>(0)?.max(0) as u64,
                        failed: r.get::<_, i64>(1)?.max(0) as u64,
                        bytes: r.get::<_, i64>(2)?.max(0) as u64,
                    })
                },
            )
            .optional()?;
        Ok(s.unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_default_is_zero() {
        let s = Stats::default();
        assert_eq!((s.replicated, s.failed, s.bytes), (0, 0, 0));
    }

    #[test]
    fn stats_delta_default_is_zero() {
        let d = StatsDelta::default();
        assert_eq!((d.replicated, d.failed, d.bytes), (0, 0, 0));
    }

    #[test]
    fn activation_equality() {
        let a = Activation {
            active: true,
            source: "control".into(),
            updated_at: 1,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    // ---- SqliteStore -------------------------------------------------------------------------

    const INST: &str = "inst-a";

    /// A file-backed store in a fresh temp dir, plus the dir guard (kept alive by the caller so the
    /// path stays valid). File-backed (not `:memory:`) so reopen/recovery tests see real WAL data.
    fn temp_store() -> (SqliteStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(dir.path().join("state.db")).unwrap();
        (store, dir)
    }

    #[test]
    fn open_enables_wal() {
        let (store, _d) = temp_store();
        let mode: String = store
            .lock()
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode.to_lowercase(), "wal");
    }

    #[test]
    fn upsert_then_get_round_trips() {
        let (s, _d) = temp_store();
        s.upsert_ready(INST, "a/b.txt", 100, 0, 10).unwrap();
        let it = s.get(INST, "a/b.txt").unwrap().unwrap();
        assert_eq!(it.instance, INST);
        assert_eq!(it.relpath, "a/b.txt");
        assert_eq!(it.state, ItemState::Ready);
        assert_eq!(it.size, 100);
        assert_eq!(it.discovered_at, 10);
        assert_eq!(it.attempts, 0);
        assert_eq!(it.next_attempt_at, 0);
        assert_eq!(it.bytes_done, 0);
        assert!(it.last_error.is_none());
        assert_eq!(it.abs_source, std::path::PathBuf::from("a/b.txt"));
    }

    #[test]
    fn get_missing_is_none() {
        let (s, _d) = temp_store();
        assert!(s.get(INST, "nope").unwrap().is_none());
    }

    #[test]
    fn upsert_preserves_terminal_state_when_file_unchanged() {
        // Re-discovering the SAME terminal file (identical size+mtime) must not resurrect it — a
        // completed file is never re-replicated in a loop.
        let (s, _d) = temp_store();
        s.upsert_ready(INST, "f", 10, 111, 1).unwrap();
        s.set_state(INST, "f", ItemState::Completed, 2).unwrap();
        // Same size AND same mtime → unchanged → state preserved (only updated_at refreshed).
        s.upsert_ready(INST, "f", 10, 111, 3).unwrap();
        let it = s.get(INST, "f").unwrap().unwrap();
        assert_eq!(it.state, ItemState::Completed, "unchanged file must not resurrect to Ready");
        assert_eq!(it.size, 10);
        assert_eq!(it.updated_at, 3);
    }

    #[test]
    fn upsert_resets_terminal_state_when_file_changed() {
        // A genuinely NEW file at a terminal relpath (the rotating/fixed-name producer pattern) must
        // be reset to Ready so it is re-claimed and re-replicated (FR-REL-1, NFR-5) — the bug the
        // review flagged: a size/mtime change silently dropped the new file.
        let (s, _d) = temp_store();
        s.upsert_ready(INST, "report.csv", 10, 111, 1).unwrap();
        // Simulate a full lifecycle to a terminal state, with prior attempt bookkeeping.
        s.record_attempt(INST, "report.csv", "old boom", ItemState::Failed, 5, 2).unwrap();
        s.set_state(INST, "report.csv", ItemState::Completed, 3).unwrap();

        // Case 1: same size, DIFFERENT mtime (in-place overwrite of identical length) → reset.
        s.upsert_ready(INST, "report.csv", 10, 222, 10).unwrap();
        let it = s.get(INST, "report.csv").unwrap().unwrap();
        assert_eq!(it.state, ItemState::Ready, "changed mtime → new file → Ready");
        assert_eq!(it.attempts, 0, "attempt bookkeeping cleared");
        assert_eq!(it.next_attempt_at, 0);
        assert!(it.last_error.is_none());
        assert_eq!(it.discovered_at, 10, "re-dated so ordering treats it as fresh");
        // It is now claimable (the whole point).
        assert_eq!(s.claim_ready(INST, 10, 100).unwrap().len(), 1);

        // Case 2: DIFFERENT size resets a terminal row too.
        s.set_state(INST, "report.csv", ItemState::Quarantined, 20).unwrap();
        s.upsert_ready(INST, "report.csv", 999, 222, 21).unwrap();
        let it = s.get(INST, "report.csv").unwrap().unwrap();
        assert_eq!(it.state, ItemState::Ready, "changed size → new file → Ready");
        assert_eq!(it.size, 999);
    }

    #[test]
    fn upsert_does_not_disturb_in_flight_state() {
        // A size/mtime change on a non-terminal (in-flight) row must NOT yank it back to Ready.
        let (s, _d) = temp_store();
        s.upsert_ready(INST, "f", 10, 111, 1).unwrap();
        s.set_state(INST, "f", ItemState::InProgress, 2).unwrap();
        s.upsert_ready(INST, "f", 999, 222, 3).unwrap();
        let it = s.get(INST, "f").unwrap().unwrap();
        assert_eq!(it.state, ItemState::InProgress, "in-flight item never reset by re-discovery");
        assert_eq!(it.size, 999, "size still refreshed");
    }

    #[test]
    fn claim_ready_is_oldest_first_and_limited() {
        let (s, _d) = temp_store();
        s.upsert_ready(INST, "new", 1, 0, 30).unwrap();
        s.upsert_ready(INST, "old", 1, 0, 10).unwrap();
        s.upsert_ready(INST, "mid", 1, 0, 20).unwrap();
        let claimed = s.claim_ready(INST, 2, 100).unwrap();
        assert_eq!(
            claimed.iter().map(|i| i.relpath.as_str()).collect::<Vec<_>>(),
            vec!["old", "mid"]
        );
        // Claimed items are transitioned InProgress (write-ahead) both in the return and the DB.
        assert!(claimed.iter().all(|i| i.state == ItemState::InProgress));
        assert_eq!(
            s.get(INST, "old").unwrap().unwrap().state,
            ItemState::InProgress
        );
        // The un-claimed one stays Ready and is claimed on the next pass.
        assert_eq!(s.get(INST, "new").unwrap().unwrap().state, ItemState::Ready);
        let again = s.claim_ready(INST, 10, 100).unwrap();
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].relpath, "new");
    }

    #[test]
    fn claim_respects_backoff_gate_and_instance_scope() {
        let (s, _d) = temp_store();
        s.upsert_ready(INST, "f", 1, 0, 1).unwrap();
        s.upsert_ready("other", "g", 1, 0, 1).unwrap();
        // Push f's next attempt into the future, then a claim at now < gate skips it.
        s.record_attempt(INST, "f", "boom", ItemState::Ready, 500, 2).unwrap();
        assert!(s.claim_ready(INST, 10, 100).unwrap().is_empty());
        // At/after the gate it is claimable again.
        let c = s.claim_ready(INST, 10, 500).unwrap();
        assert_eq!(c.len(), 1);
        // The other instance's item was never visible to INST's claim.
        assert_eq!(s.get("other", "g").unwrap().unwrap().state, ItemState::Ready);
    }

    #[test]
    fn record_attempt_increments_and_sets_fields() {
        let (s, _d) = temp_store();
        s.upsert_ready(INST, "f", 1, 0, 1).unwrap();
        s.record_attempt(INST, "f", "err1", ItemState::Failed, 100, 5).unwrap();
        let it = s.get(INST, "f").unwrap().unwrap();
        assert_eq!(it.attempts, 1);
        assert_eq!(it.state, ItemState::Failed);
        assert_eq!(it.last_error.as_deref(), Some("err1"));
        assert_eq!(it.next_attempt_at, 100);
        assert_eq!(it.updated_at, 5);
        s.record_attempt(INST, "f", "err2", ItemState::Exhausted, 200, 6).unwrap();
        let it = s.get(INST, "f").unwrap().unwrap();
        assert_eq!(it.attempts, 2);
        assert_eq!(it.state, ItemState::Exhausted);
        assert_eq!(it.last_error.as_deref(), Some("err2"));
    }

    #[test]
    fn set_bytes_done_persists_progress() {
        let (s, _d) = temp_store();
        s.upsert_ready(INST, "f", 1000, 0, 1).unwrap();
        s.set_bytes_done(INST, "f", 512, 7).unwrap();
        let it = s.get(INST, "f").unwrap().unwrap();
        assert_eq!(it.bytes_done, 512);
        assert_eq!(it.updated_at, 7);
    }

    #[test]
    fn list_by_state_filters_and_orders() {
        let (s, _d) = temp_store();
        s.upsert_ready(INST, "r2", 1, 0, 20).unwrap();
        s.upsert_ready(INST, "r1", 1, 0, 10).unwrap();
        s.upsert_ready(INST, "done", 1, 0, 5).unwrap();
        s.set_state(INST, "done", ItemState::Completed, 6).unwrap();
        let ready = s.list_by_state(INST, ItemState::Ready).unwrap();
        assert_eq!(
            ready.iter().map(|i| i.relpath.as_str()).collect::<Vec<_>>(),
            vec!["r1", "r2"]
        );
        assert_eq!(s.list_by_state(INST, ItemState::Completed).unwrap().len(), 1);
        assert!(s.list_by_state(INST, ItemState::Quarantined).unwrap().is_empty());
    }

    #[test]
    fn recover_incomplete_returns_in_progress_and_verified_only() {
        let (s, _d) = temp_store();
        for (rel, disc) in [("a", 1), ("b", 2), ("c", 3), ("d", 4)] {
            s.upsert_ready(INST, rel, 1, 0, disc).unwrap();
        }
        s.set_state(INST, "a", ItemState::InProgress, 10).unwrap();
        s.set_state(INST, "b", ItemState::Verified, 10).unwrap();
        s.set_state(INST, "c", ItemState::Completed, 10).unwrap();
        // "d" stays Ready.
        let rec = s.recover_incomplete(INST).unwrap();
        assert_eq!(
            rec.iter().map(|i| i.relpath.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }

    #[test]
    fn state_survives_reopen_crash_recovery() {
        // Simulate a crash: write InProgress/Verified, drop the store, reopen the same file.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        {
            let s = SqliteStore::open(&path).unwrap();
            s.upsert_ready(INST, "inflight", 1, 0, 1).unwrap();
            s.upsert_ready(INST, "verified", 1, 0, 2).unwrap();
            s.set_state(INST, "inflight", ItemState::InProgress, 3).unwrap();
            s.set_state(INST, "verified", ItemState::Verified, 3).unwrap();
        } // store dropped == process exit
        let s = SqliteStore::open(&path).unwrap();
        let rec = s.recover_incomplete(INST).unwrap();
        assert_eq!(rec.len(), 2, "in-flight + verified recovered after reopen");
        assert_eq!(
            s.get(INST, "verified").unwrap().unwrap().state,
            ItemState::Verified
        );
    }

    #[test]
    fn resume_round_trip_update_and_clear() {
        let (s, _d) = temp_store();
        assert!(s.load_resume(INST, "f", "local").unwrap().is_none());
        let r = ResumeState {
            bytes_committed: 4096,
            token: serde_json::json!({"temp": "f.part"}),
        };
        s.save_resume(INST, "f", "local", &r).unwrap();
        let got = s.load_resume(INST, "f", "local").unwrap().unwrap();
        assert_eq!(got.bytes_committed, 4096);
        assert_eq!(got.token, serde_json::json!({"temp": "f.part"}));
        // Upsert on the same (instance, relpath, dest) key.
        let r2 = ResumeState {
            bytes_committed: 8192,
            token: serde_json::json!({"temp": "f.part2"}),
        };
        s.save_resume(INST, "f", "local", &r2).unwrap();
        assert_eq!(
            s.load_resume(INST, "f", "local").unwrap().unwrap().bytes_committed,
            8192
        );
        s.clear_resume(INST, "f", "local").unwrap();
        assert!(s.load_resume(INST, "f", "local").unwrap().is_none());
    }

    #[test]
    fn resume_is_keyed_by_dest() {
        let (s, _d) = temp_store();
        let mk = |n: u64| ResumeState {
            bytes_committed: n,
            token: serde_json::Value::Null,
        };
        s.save_resume(INST, "f", "local", &mk(1)).unwrap();
        s.save_resume(INST, "f", "s3", &mk(2)).unwrap();
        assert_eq!(s.load_resume(INST, "f", "local").unwrap().unwrap().bytes_committed, 1);
        assert_eq!(s.load_resume(INST, "f", "s3").unwrap().unwrap().bytes_committed, 2);
    }

    #[test]
    fn activation_get_set_clear() {
        let (s, _d) = temp_store();
        assert!(s.load_activation(INST).unwrap().is_none());
        s.set_activation(INST, true, "control", 100).unwrap();
        let a = s.load_activation(INST).unwrap().unwrap();
        assert!(a.active);
        assert_eq!(a.source, "control");
        assert_eq!(a.updated_at, 100);
        // Overwrite via the upsert path.
        s.set_activation(INST, false, "config", 200).unwrap();
        let a = s.load_activation(INST).unwrap().unwrap();
        assert!(!a.active);
        assert_eq!(a.source, "config");
        assert_eq!(a.updated_at, 200);
        s.clear_activation(INST).unwrap();
        assert!(s.load_activation(INST).unwrap().is_none());
    }

    #[test]
    fn stats_default_missing_and_accumulate() {
        let (s, _d) = temp_store();
        assert_eq!(s.stats(INST).unwrap(), Stats::default());
        s.bump_stats(
            INST,
            StatsDelta { replicated: 2, failed: 1, bytes: 500 },
        )
        .unwrap();
        s.bump_stats(
            INST,
            StatsDelta { replicated: 3, failed: 0, bytes: 1500 },
        )
        .unwrap();
        assert_eq!(
            s.stats(INST).unwrap(),
            Stats { replicated: 5, failed: 1, bytes: 2000 }
        );
    }

    #[test]
    fn stats_are_per_instance() {
        let (s, _d) = temp_store();
        s.bump_stats(INST, StatsDelta { replicated: 1, failed: 0, bytes: 10 }).unwrap();
        s.bump_stats("other", StatsDelta { replicated: 9, failed: 0, bytes: 90 }).unwrap();
        assert_eq!(s.stats(INST).unwrap().replicated, 1);
        assert_eq!(s.stats("other").unwrap().replicated, 9);
    }

    #[test]
    fn in_memory_store_works() {
        let s = SqliteStore::open_in_memory().unwrap();
        s.upsert_ready(INST, "f", 1, 0, 1).unwrap();
        assert_eq!(s.claim_ready(INST, 10, 10).unwrap().len(), 1);
    }

    #[test]
    fn unknown_persisted_state_is_a_state_error() {
        let (s, _d) = temp_store();
        s.upsert_ready(INST, "f", 1, 0, 1).unwrap();
        // Corrupt the persisted state token out of band.
        s.lock()
            .execute(
                "UPDATE work_items SET state = 'bogus' WHERE instance = ?1 AND relpath = 'f'",
                params![INST],
            )
            .unwrap();
        let err = s.get(INST, "f").unwrap_err();
        assert!(matches!(err, ReplError::State(_)), "got {err:?}");
    }

    #[test]
    fn store_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SqliteStore>();
    }
}

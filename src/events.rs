//! # file-replicator — event catalog, UNS facade wiring, and progress throttle
//!
//! **One-liner purpose**: The outbound half of the control plane — the [`Event`] catalog (DESIGN
//! §17.1) plus the progress [`ProgressThrottle`] (unchanged engine logic), now emitted through the
//! edgecommons [`EventsFacade`] (`gg.instance(id).events()` / `gg.events()`) instead of a hand-built
//! `{thing}/file-replicator/evt/…` topic.
//!
//! ## UNS migration (replaces the old `evt`/`state` scheme)
//! Every event used to ride a hand-built topic (`{prefix}/evt/instances/{id}/{EventName}` or
//! `{prefix}/evt/{EventName}`) with a flat `{event, instance?, …fields, ts}` body. That is now the
//! **facade's** job: [`Event::plan`] maps each catalog variant to `(severity, type, message?,
//! context)`, and [`Events::emit`] hands that to [`EventsFacade::emit`] /
//! [`EventsFacade::raise_alarm`] / [`EventsFacade::clear_alarm`], which derives the
//! `evt/{severity}/{type}` channel from the body itself (DESIGN-class-facades §2.2) — exactly as
//! telemetry-processor/opcua-adapter/modbus-adapter now do. `Disconnected`/`Reconnected` (the P4
//! destination circuit-breaker, still deferred — see `CLAUDE.md`) share ONE wire type
//! (`"disconnected"`) so a future raise/clear pair rides the same channel, mirroring the
//! `connection-lost`/`connection-restored` convention the other adapters established.
//!
//! An [`Events`] value is bound to exactly ONE facade (an instance-scoped `gg.instance(id).events()`
//! for a replication [`crate::instance::Instance`], or the component-level `gg.events()` for
//! component-wide events like `ComponentReady`) — see `crate::app`/`crate::instance` for the wiring.
//! [`crate::control::InstanceControl::notify`] lets the control plane emit an event through a
//! SPECIFIC instance's own bound facade without the control plane holding a duplicate one.
//!
//! ## Retained `state/…` — dropped, not migrated (DESIGN §0 / §17.2 superseded)
//! The old per-instance/component `state/…` snapshot publish is **removed outright**, not ported: the
//! new UNS `state` class is **reserved** (library-owned; an app-level publish to it is rejected by
//! the guard) and carries only the library's own RUNNING/STOPPED keepalive — there is no seam left
//! for a component-specific rich snapshot. This was ALREADY the platform's decision for this exact
//! gap (the edge-console mandate walk, M7: retain dropped fleet-wide, LWT-only; a timestamped
//! app-layer cache is the retain substitute), not a reduction invented for this migration. The
//! reliable "current state on demand" path is unchanged: `get-status` (now a `gg.commands()` verb,
//! see `crate::control`) still answers with the exact same [`crate::control::instance_status_json`]
//! document a live subscriber used to also get pushed to it.
//!
//! ## Testability (matches the sibling migrations)
//! [`edgecommons::facades::EventsFacade`] has no public constructor outside the `edgecommons` crate (by
//! design — DESIGN-class-facades keeps the wire contract single-sourced there, and its OWN
//! conformance vectors/unit tests already pin the topic/body shape byte-for-byte). So — exactly like
//! telemetry-processor's `EvtEmitterProbe` and opcua-adapter/modbus-adapter's untouched test suites —
//! this crate can no longer assert the exact evt TOPIC from a unit test. What it still owns and
//! exhaustively tests is its OWN business decision: which [`Event`] the engine raises, when, and with
//! which fields ([`Event::plan`]'s severity/type/message/context mapping, tested directly; the engine
//! call sites tested via the in-process [`test_support::recording_events`] recorder, which records
//! the [`Event`] value itself rather than a wire message).

#[cfg(test)]
use std::sync::{Arc, Mutex};

use edgecommons::facades::{EventsFacade, Severity};
use serde_json::{json, Map, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

/// Default `progressPercentStep` for [`ProgressThrottle`] (DESIGN §17.2 / FR-EVT-2).
pub const DEFAULT_PROGRESS_PERCENT_STEP: i32 = 10;
/// Default `progressIntervalSecs` (as milliseconds) for [`ProgressThrottle`].
pub const DEFAULT_PROGRESS_INTERVAL_MS: i64 = 5_000;

/// The event catalog (DESIGN §17.1). Each variant carries the fields that become the facade's
/// `context` object (beyond the derived `severity`/`type`, and an extracted `message` for the
/// variants with a natural error string — see [`Event::plan`]).
///
/// A few variants are catalogued now so the wire contract is complete, but are only *emitted* once
/// their source exists in a later phase — they are tagged below.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// A candidate file was seen but is not yet ready. **Deferred**: the P1 watcher only surfaces
    /// ready candidates, so this is not emitted yet (needs the not-yet-ready surface).
    FileDiscovered { path: String, size: u64 },
    /// A file became ready and was enqueued.
    FileReady { path: String, size: u64 },
    /// A transfer started (item claimed → InProgress).
    ReplicationStarted {
        path: String,
        size: u64,
        destination: String,
        attempt: u32,
    },
    /// A throttled progress tick (see [`ProgressThrottle`]). `percent` is `0.0..=100.0`.
    ReplicationProgress {
        path: String,
        size: u64,
        bytes_done: u64,
        percent: f64,
        destination: String,
        attempt: u32,
    },
    /// A transfer completed and verified.
    ReplicationCompleted {
        path: String,
        size: u64,
        destination: String,
        bytes: u64,
    },
    /// A transfer attempt failed but will be retried. Body carries `willRetry: true`.
    ReplicationFailed {
        path: String,
        destination: String,
        attempt: u32,
        error: String,
        /// Next scheduled attempt, Unix ms; rendered as RFC3339 `nextAttemptAt` when present.
        next_attempt_at_ms: Option<i64>,
    },
    /// The retry budget was exhausted (no more attempts).
    RetriesExhausted {
        path: String,
        destination: String,
        attempts: u32,
        last_error: String,
    },
    /// The source file was archived on success. `archivePath` omitted when unknown.
    FileArchived {
        path: String,
        archive_path: Option<String>,
    },
    /// The source file was deleted on success.
    FileDeleted { path: String },
    /// The source file was quarantined after exhaustion. `quarantinePath` omitted when unknown.
    FileQuarantined {
        path: String,
        attempts: u32,
        last_error: String,
        quarantine_path: Option<String>,
    },
    /// A scan/tick finished. `discovered` = newly enqueued this tick; `awaiting` = total ready.
    ScanComplete { discovered: u64, awaiting: u64 },
    /// An instance was activated (via the control plane). `source` = who toggled it.
    InstanceActivated { source: String },
    /// An instance was deactivated / reset (via the control plane).
    InstanceDeactivated { source: String },
    /// The component finished initializing (`set_ready(true)`). Component-level (main instance).
    ComponentReady { instances: u64, version: String },
    /// A trigger command forced a scan/replication, or a `cron` schedule fired (DESIGN §12.2: cron
    /// mode releases all ready work at each fire, reusing the same "the schedule released work now"
    /// semantics as the control-plane `trigger` command). `scope` = `"all"` or the instance id.
    ScheduleTriggered { scope: String },
    /// A replication window opened (DESIGN §12.3/§17.1). `window` is the schedule's human-readable
    /// label (e.g. `"0 22 * * * -> 0 6 * * *"`).
    WindowOpened { window: String },
    /// A replication window closed (DESIGN §12.3/§17.1); see [`Event::ScheduleComplete`] for the
    /// paired "wrap-up finished" signal (pause or finish-current).
    WindowClosed { window: String },
    /// A schedule-gated release cycle finished (DESIGN §12.4): either a `cron` fire has claimed and
    /// processed everything it could this fire, or a `window`'s close-time wrap-up (pause-resume or
    /// finish-current) has completed. `mode` = `"cron"` | `"window"`.
    ScheduleComplete { mode: String },
    /// **Deferred (circuit-breaker, §13.4)**: the destination link went down. Raises a sustained
    /// alarm (shares the `"disconnected"` wire type with [`Event::Reconnected`] so the pair rides
    /// one `evt/critical/disconnected` channel — see [`Event::plan`]).
    Disconnected { link: String },
    /// **Deferred (circuit-breaker, §13.4)**: the destination link recovered — clears the
    /// [`Event::Disconnected`] alarm.
    Reconnected { link: String },
    /// A permission/access error on one of the instance's directories — startup validation or a
    /// runtime rescan/transfer (`src/permission.rs`, Feature A). ALWAYS emitted once per dedup-logged
    /// occurrence (log-once, not per rescan — see [`crate::permission::PermissionLog`]). `role` is
    /// `"ingress"`/`"egress"`/`"archive"`/`"failed"` (see [`crate::permission::Role`]).
    PermissionDenied {
        path: String,
        role: String,
        error: String,
    },
}

impl Event {
    /// The internal discriminator (used for test filtering/logging only — see [`Event::plan`] for
    /// the actual wire `type` token, which is kebab-case and NOT always the same string).
    pub fn name(&self) -> &'static str {
        match self {
            Event::FileDiscovered { .. } => "FileDiscovered",
            Event::FileReady { .. } => "FileReady",
            Event::ReplicationStarted { .. } => "ReplicationStarted",
            Event::ReplicationProgress { .. } => "ReplicationProgress",
            Event::ReplicationCompleted { .. } => "ReplicationCompleted",
            Event::ReplicationFailed { .. } => "ReplicationFailed",
            Event::RetriesExhausted { .. } => "RetriesExhausted",
            Event::FileArchived { .. } => "FileArchived",
            Event::FileDeleted { .. } => "FileDeleted",
            Event::FileQuarantined { .. } => "FileQuarantined",
            Event::ScanComplete { .. } => "ScanComplete",
            Event::InstanceActivated { .. } => "InstanceActivated",
            Event::InstanceDeactivated { .. } => "InstanceDeactivated",
            Event::ComponentReady { .. } => "ComponentReady",
            Event::ScheduleTriggered { .. } => "ScheduleTriggered",
            Event::WindowOpened { .. } => "WindowOpened",
            Event::WindowClosed { .. } => "WindowClosed",
            Event::ScheduleComplete { .. } => "ScheduleComplete",
            Event::Disconnected { .. } => "Disconnected",
            Event::Reconnected { .. } => "Reconnected",
            Event::PermissionDenied { .. } => "PermissionDenied",
        }
    }

    /// The complete event-specific field map — the full business data (including the natural error
    /// string, when there is one). [`Event::plan`] derives the facade's `context` from a COPY of this
    /// with the message-promoted key removed (so it is never duplicated on the wire); the test
    /// recorder ([`test_support`]) exposes this full map unfiltered, so a test can still assert on
    /// `error`/`lastError` directly.
    fn fields(&self) -> Map<String, Value> {
        let obj = |v: Value| match v {
            Value::Object(m) => m,
            _ => Map::new(),
        };
        match self {
            Event::FileDiscovered { path, size } | Event::FileReady { path, size } => {
                obj(json!({ "path": path, "size": size }))
            }
            Event::ReplicationStarted {
                path,
                size,
                destination,
                attempt,
            } => obj(json!({
                "path": path, "size": size, "destination": destination, "attempt": attempt
            })),
            Event::ReplicationProgress {
                path,
                size,
                bytes_done,
                percent,
                destination,
                attempt,
            } => obj(json!({
                "path": path, "size": size, "bytesDone": bytes_done, "percent": percent,
                "destination": destination, "attempt": attempt
            })),
            Event::ReplicationCompleted {
                path,
                size,
                destination,
                bytes,
            } => obj(json!({
                "path": path, "size": size, "destination": destination, "bytes": bytes
            })),
            Event::ReplicationFailed {
                path,
                destination,
                attempt,
                error,
                next_attempt_at_ms,
            } => {
                let mut m = obj(json!({
                    "path": path, "destination": destination, "attempt": attempt,
                    "error": error, "willRetry": true
                }));
                if let Some(ms) = next_attempt_at_ms {
                    m.insert("nextAttemptAt".into(), json!(rfc3339(*ms)));
                }
                m
            }
            Event::RetriesExhausted {
                path,
                destination,
                attempts,
                last_error,
            } => obj(json!({
                "path": path, "destination": destination, "attempts": attempts,
                "lastError": last_error
            })),
            Event::FileArchived { path, archive_path } => {
                let mut m = obj(json!({ "path": path }));
                if let Some(ap) = archive_path {
                    m.insert("archivePath".into(), json!(ap));
                }
                m
            }
            Event::FileDeleted { path } => obj(json!({ "path": path })),
            Event::FileQuarantined {
                path,
                attempts,
                last_error,
                quarantine_path,
            } => {
                let mut m = obj(json!({ "path": path, "attempts": attempts, "lastError": last_error }));
                if let Some(qp) = quarantine_path {
                    m.insert("quarantinePath".into(), json!(qp));
                }
                m
            }
            Event::ScanComplete {
                discovered,
                awaiting,
            } => obj(json!({ "discovered": discovered, "awaiting": awaiting })),
            Event::InstanceActivated { source } | Event::InstanceDeactivated { source } => {
                obj(json!({ "source": source }))
            }
            Event::ComponentReady { instances, version } => {
                obj(json!({ "instances": instances, "version": version }))
            }
            Event::ScheduleTriggered { scope } => obj(json!({ "scope": scope })),
            Event::WindowOpened { window } | Event::WindowClosed { window } => {
                obj(json!({ "window": window }))
            }
            Event::ScheduleComplete { mode } => obj(json!({ "mode": mode })),
            Event::Disconnected { link } | Event::Reconnected { link } => {
                obj(json!({ "link": link }))
            }
            Event::PermissionDenied { path, role, error } => {
                obj(json!({ "path": path, "role": role, "error": error }))
            }
        }
    }

    /// Maps this event onto the `events()` facade's publish contract (DESIGN-class-facades §2.2):
    /// severity, the kebab-case wire `type` (the evt channel token), an optional `message` (promoted
    /// out of `context` when a natural error string exists, never duplicated), and the `context`
    /// object. `alarm` selects `raise_alarm`/`clear_alarm` (`Some(active)`) vs. a plain `emit`
    /// (`None`) — see [`Events::emit`].
    ///
    /// Severity policy: `Info` for routine lifecycle/progress; `Warning` for a retryable failure
    /// (the engine will try again); `Critical` for a terminal per-file failure (exhausted/
    /// quarantined), a permission violation (disables the instance under the default policy), or a
    /// sustained link-down condition. `Disconnected`/`Reconnected` share the wire type
    /// `"disconnected"` so raise/clear ride one channel (DESIGN-class-facades' `raise_alarm`/
    /// `clear_alarm` contract) — mirroring opcua-adapter/modbus-adapter's `connection-lost` pairing.
    fn plan(&self) -> EmitPlan {
        let mut context = self.fields();
        let message = match self {
            Event::ReplicationFailed { error, .. } => Some(error.clone()),
            Event::RetriesExhausted { last_error, .. } => Some(last_error.clone()),
            Event::FileQuarantined { last_error, .. } => Some(last_error.clone()),
            Event::PermissionDenied { error, .. } => Some(error.clone()),
            _ => None,
        };
        let severity = match self {
            Event::ReplicationFailed { .. } => Severity::Warning,
            Event::RetriesExhausted { .. }
            | Event::FileQuarantined { .. }
            | Event::Disconnected { .. }
            | Event::Reconnected { .. }
            | Event::PermissionDenied { .. } => Severity::Critical,
            Event::FileDiscovered { .. } => Severity::Debug,
            _ => Severity::Info,
        };
        let event_type = match self {
            Event::FileDiscovered { .. } => "file-discovered",
            Event::FileReady { .. } => "file-ready",
            Event::ReplicationStarted { .. } => "replication-started",
            Event::ReplicationProgress { .. } => "replication-progress",
            Event::ReplicationCompleted { .. } => "replication-completed",
            Event::ReplicationFailed { .. } => "replication-failed",
            Event::RetriesExhausted { .. } => "retries-exhausted",
            Event::FileArchived { .. } => "file-archived",
            Event::FileDeleted { .. } => "file-deleted",
            Event::FileQuarantined { .. } => "file-quarantined",
            Event::ScanComplete { .. } => "scan-complete",
            Event::InstanceActivated { .. } => "instance-activated",
            Event::InstanceDeactivated { .. } => "instance-deactivated",
            Event::ComponentReady { .. } => "component-ready",
            Event::ScheduleTriggered { .. } => "schedule-triggered",
            Event::WindowOpened { .. } => "window-opened",
            Event::WindowClosed { .. } => "window-closed",
            Event::ScheduleComplete { .. } => "schedule-complete",
            // Raise/clear share one channel — see the doc comment above.
            Event::Disconnected { .. } | Event::Reconnected { .. } => "disconnected",
            Event::PermissionDenied { .. } => "permission-denied",
        };
        let alarm = match self {
            Event::Disconnected { .. } => Some(true),
            Event::Reconnected { .. } => Some(false),
            _ => None,
        };
        // The promoted message field is dropped from context so it is never duplicated on the wire.
        for key in ["error", "lastError"] {
            context.remove(key);
        }
        EmitPlan {
            severity,
            event_type,
            message,
            context: Value::Object(context),
            alarm,
        }
    }
}

/// The resolved `(severity, type, message, context, alarm)` an [`Event`] plans onto the facade —
/// see [`Event::plan`].
struct EmitPlan {
    severity: Severity,
    event_type: &'static str,
    message: Option<String>,
    context: Value,
    alarm: Option<bool>,
}

/// Format a Unix-millis instant as an RFC3339 UTC string (the `nextAttemptAt` wire format). Falls
/// back to the epoch on the (practically impossible) out-of-range/format failure.
fn rfc3339(now_ms: i64) -> String {
    OffsetDateTime::from_unix_timestamp_nanos((now_ms as i128) * 1_000_000)
        .ok()
        .and_then(|dt| dt.format(&Rfc3339).ok())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}

/// Progress-emission throttle (DESIGN §17.2 / FR-EVT-2). One per in-flight transfer; pure logic,
/// fully unit-tested. Unchanged by the UNS migration — this gates HOW OFTEN the engine calls
/// [`Events::emit`], not the wire shape.
#[derive(Debug, Clone)]
pub struct ProgressThrottle {
    last_percent: i32,
    last_emit_ms: i64,
    percent_step: i32,
    interval_ms: i64,
}

impl ProgressThrottle {
    /// A throttle with an explicit percent step and interval (ms). A non-positive `percent_step`
    /// clamps to 1 so every observed integer-percent change emits.
    pub fn new(percent_step: i32, interval_ms: i64) -> Self {
        Self {
            last_percent: -1,
            last_emit_ms: 0,
            percent_step: percent_step.max(1),
            interval_ms: interval_ms.max(0),
        }
    }

    /// A throttle with the DESIGN defaults (10% step, 5s interval).
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_PROGRESS_PERCENT_STEP, DEFAULT_PROGRESS_INTERVAL_MS)
    }

    /// Whether a progress event should be emitted at `percent` (integer, `0..=100`) at `now_ms`.
    ///
    /// Emits when: this is the first observation at 0%; OR `percent >= 100` (always); OR the percent
    /// jumped by ≥ `percent_step`; OR ≥ `interval_ms` elapsed since the last emission. On a `true`
    /// result the internal `last_percent`/`last_emit_ms` advance.
    pub fn should_emit(&mut self, percent: i32, now_ms: i64) -> bool {
        let first_at_zero = self.last_percent < 0 && percent <= 0; // first observation at (or before) 0%
        let emit = first_at_zero
            || percent >= 100 // 100% always
            || percent - self.last_percent >= self.percent_step
            || now_ms - self.last_emit_ms >= self.interval_ms;
        if emit {
            self.last_percent = percent;
            self.last_emit_ms = now_ms;
        }
        emit
    }
}

/// The cloneable event emitter every instance/worker holds (and the component-level wiring for
/// `ComponentReady`/scope-`all` events) — bound to exactly ONE edgecommons [`EventsFacade`] (an
/// instance-scoped `gg.instance(id).events()`, or the component-level `gg.events()`). `disabled()`
/// (no facade bound — messaging unavailable on this platform, DESIGN §6) makes every call a no-op,
/// so the P1/P2 replication engine runs unchanged.
#[derive(Clone)]
pub struct Events {
    facade: Option<EventsFacade>,
    /// Test-only recorder (see [`test_support::recording_events`]): when set, [`Self::emit`] records
    /// the [`Event`] value for in-process assertions instead of (or in addition to, though in
    /// practice `facade` is always `None` when this is `Some`) a real facade publish. `EventsFacade`
    /// has no public constructor outside the edgecommons crate, so this is the only way component code
    /// can assert "the engine raised event X" without going through a live `EdgeCommons`.
    #[cfg(test)]
    recorder: Option<Arc<Mutex<Vec<Event>>>>,
}

impl Events {
    /// Build an emitter bound to a live facade (`gg.instance(id).events()` for an instance, or
    /// `gg.events()` for the component-level "main" instance).
    pub fn new(facade: EventsFacade) -> Self {
        Self {
            facade: Some(facade),
            #[cfg(test)]
            recorder: None,
        }
    }

    /// A disabled (no-op) emitter — no facade bound. Used when messaging is unavailable on this
    /// platform and by tests that don't care about event emission.
    pub fn disabled() -> Self {
        Self {
            facade: None,
            #[cfg(test)]
            recorder: None,
        }
    }

    /// Whether [`Self::emit`] would actually do something — a facade is bound, or (test builds only)
    /// a [`test_support::recording_events`] recorder is attached. Used by
    /// [`crate::instance::worker::build_progress`] to skip spawning the progress-throttle drainer
    /// task entirely when nothing would ever observe it.
    pub fn is_enabled(&self) -> bool {
        #[cfg(test)]
        {
            self.facade.is_some() || self.recorder.is_some()
        }
        #[cfg(not(test))]
        {
            self.facade.is_some()
        }
    }

    /// Emits one event through the bound `events()` facade: [`Event::plan`] derives severity/type/
    /// message/context, and alarm-bearing variants ride `raise_alarm`/`clear_alarm` so the
    /// `evt/{severity}/{type}` channel and the body can never disagree (DESIGN-class-facades §2.2).
    /// No-op when no facade is bound (DESIGN §6 graceful degradation); a publish failure is logged at
    /// DEBUG and swallowed — event emission never disturbs the replication engine.
    pub async fn emit(&self, ev: Event) {
        #[cfg(test)]
        if let Some(rec) = &self.recorder {
            rec.lock().unwrap().push(ev.clone());
        }
        let Some(facade) = &self.facade else { return };
        let plan = ev.plan();
        let result = match plan.alarm {
            Some(true) => {
                facade
                    .raise_alarm(plan.severity, plan.event_type, plan.message, Some(plan.context))
                    .await
            }
            Some(false) => facade.clear_alarm(plan.severity, plan.event_type, Some(plan.context)).await,
            None => facade.emit(plan.severity, plan.event_type, plan.message, Some(plan.context)).await,
        };
        if let Err(e) = result {
            tracing::debug!(error = %e, event = ev.name(), "evt publish failed (ignored)");
        }
    }
}

/// Test-only event recorder, shared by the control-plane's/engine's event-wiring tests
/// (`crate::instance::worker`, `crate::instance`, `crate::control`) so each does not reinvent a fake.
/// Compiled only under `#[cfg(test)]`. See the [module docs](self) "Testability" section for why this
/// records the [`Event`] value rather than a wire message.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    /// One recorded event, exposed with a `body` field shaped like the event's context (plus, for
    /// call sites that still expect it, direct field access via [`serde_json::Value`] indexing) — a
    /// drop-in read-only view so existing `fake.events_named("X")[0].body["field"]`-style assertions
    /// keep working unchanged after the facade migration.
    #[derive(Debug, Clone)]
    pub(crate) struct RecordedEvent {
        pub(crate) body: Value,
    }

    /// The handle [`recording_events`] returns: query recorded [`Event`]s by name (mirrors the old
    /// `RecordingMessaging` fake's surface so existing test call sites need minimal changes).
    pub(crate) struct EventRecorder {
        events: Arc<Mutex<Vec<Event>>>,
    }

    impl EventRecorder {
        /// Every recorded event whose [`Event::name`] equals `name`, in emission order.
        pub(crate) fn events_named(&self, name: &str) -> Vec<RecordedEvent> {
            self.events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.name() == name)
                .map(|e| RecordedEvent { body: Value::Object(e.fields()) })
                .collect()
        }

        /// Whether nothing has been recorded yet.
        pub(crate) fn is_empty(&self) -> bool {
            self.events.lock().unwrap().is_empty()
        }
    }

    /// An [`Events`] emitter that records every emitted [`Event`] in-process (no facade — see the
    /// [module docs](super) "Testability" section for why this replaced recording actual wire
    /// messages). Returns the recorder (for assertions) and the emitter (to inject).
    pub(crate) fn recording_events() -> (Arc<EventRecorder>, Events) {
        let events = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::new(EventRecorder { events: events.clone() });
        let emitter = Events {
            facade: None,
            recorder: Some(events),
        };
        (recorder, emitter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- rfc3339 -------------------------------------------------------------------------------

    #[test]
    fn rfc3339_formats_epoch_and_a_known_instant() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
        // 2026-07-01T09:15:22Z == 1_782_897_322 s (whole seconds → no fractional part).
        assert_eq!(rfc3339(1_782_897_322_000), "2026-07-01T09:15:22Z");
    }

    // ---- Event::plan (the facade mapping) -------------------------------------------------------

    #[test]
    fn progress_plan_is_info_with_no_message() {
        let ev = Event::ReplicationProgress {
            path: "big.parquet".into(),
            size: 734_003_200,
            bytes_done: 220_200_960,
            percent: 30.0,
            destination: "s3".into(),
            attempt: 1,
        };
        let plan = ev.plan();
        assert_eq!(plan.severity, Severity::Info);
        assert_eq!(plan.event_type, "replication-progress");
        assert!(plan.message.is_none());
        assert_eq!(plan.context["path"], json!("big.parquet"));
        assert_eq!(plan.context["bytesDone"], json!(220_200_960u64));
        assert_eq!(plan.context["percent"], json!(30.0));
        assert!(plan.alarm.is_none());
    }

    #[test]
    fn failed_plan_is_warning_promotes_message_and_carries_next_attempt() {
        let with = Event::ReplicationFailed {
            path: "x.csv".into(),
            destination: "local".into(),
            attempt: 3,
            error: "boom".into(),
            next_attempt_at_ms: Some(0),
        };
        let plan = with.plan();
        assert_eq!(plan.severity, Severity::Warning);
        assert_eq!(plan.event_type, "replication-failed");
        assert_eq!(plan.message, Some("boom".to_string()));
        assert!(plan.context.get("error").is_none(), "promoted, not duplicated");
        assert_eq!(plan.context["willRetry"], json!(true));
        assert_eq!(plan.context["nextAttemptAt"], json!("1970-01-01T00:00:00Z"));
        assert!(plan.alarm.is_none());

        let without = Event::ReplicationFailed {
            path: "x.csv".into(),
            destination: "local".into(),
            attempt: 3,
            error: "boom".into(),
            next_attempt_at_ms: None,
        };
        assert!(without.plan().context.get("nextAttemptAt").is_none(), "omitted when None");
    }

    #[test]
    fn retries_exhausted_and_quarantined_are_critical_plain_emits() {
        let exhausted = Event::RetriesExhausted {
            path: "p".into(),
            destination: "s3".into(),
            attempts: 5,
            last_error: "denied".into(),
        };
        let plan = exhausted.plan();
        assert_eq!(plan.severity, Severity::Critical);
        assert_eq!(plan.event_type, "retries-exhausted");
        assert_eq!(plan.message, Some("denied".to_string()));
        assert!(plan.alarm.is_none(), "a terminal per-file failure has no clear counterpart");

        let quarantined = Event::FileQuarantined {
            path: "a".into(),
            attempts: 5,
            last_error: "e".into(),
            quarantine_path: Some("/f/a".into()),
        };
        let plan = quarantined.plan();
        assert_eq!(plan.severity, Severity::Critical);
        assert_eq!(plan.event_type, "file-quarantined");
        assert_eq!(plan.context["quarantinePath"], json!("/f/a"));
        assert!(plan.context.get("lastError").is_none(), "promoted to message, not duplicated");
    }

    #[test]
    fn optional_paths_omitted_when_absent_present_when_set() {
        let arch_none = Event::FileArchived {
            path: "a".into(),
            archive_path: None,
        };
        assert!(arch_none.plan().context.get("archivePath").is_none());
        let arch_some = Event::FileArchived {
            path: "a".into(),
            archive_path: Some("/arch/a".into()),
        };
        assert_eq!(arch_some.plan().context["archivePath"], json!("/arch/a"));
    }

    #[test]
    fn component_ready_plan_has_no_alarm_and_info_severity() {
        let ev = Event::ComponentReady {
            instances: 3,
            version: "0.1.0".into(),
        };
        let plan = ev.plan();
        assert_eq!(plan.severity, Severity::Info);
        assert_eq!(plan.event_type, "component-ready");
        assert_eq!(plan.context["instances"], json!(3));
        assert_eq!(plan.context["version"], json!("0.1.0"));
        assert!(plan.alarm.is_none());
    }

    #[test]
    fn disconnected_and_reconnected_share_the_wire_type_and_form_a_raise_clear_pair() {
        let down = Event::Disconnected { link: "s3".into() };
        let up = Event::Reconnected { link: "s3".into() };
        let down_plan = down.plan();
        let up_plan = up.plan();
        assert_eq!(down_plan.event_type, "disconnected");
        assert_eq!(up_plan.event_type, "disconnected", "raise/clear share one channel");
        assert_eq!(down_plan.severity, Severity::Critical);
        assert_eq!(up_plan.severity, Severity::Critical);
        assert_eq!(down_plan.alarm, Some(true), "raise_alarm");
        assert_eq!(up_plan.alarm, Some(false), "clear_alarm");
        assert_eq!(down_plan.context["link"], json!("s3"));
    }

    #[test]
    fn permission_denied_plan_promotes_error_and_is_critical() {
        let ev = Event::PermissionDenied {
            path: "/data/in".into(),
            role: "ingress".into(),
            error: "permission denied (os error 13)".into(),
        };
        let plan = ev.plan();
        assert_eq!(plan.severity, Severity::Critical);
        assert_eq!(plan.event_type, "permission-denied");
        assert_eq!(plan.message, Some("permission denied (os error 13)".to_string()));
        assert_eq!(plan.context["path"], json!("/data/in"));
        assert_eq!(plan.context["role"], json!("ingress"));
        assert!(plan.context.get("error").is_none(), "promoted, not duplicated");
    }

    #[test]
    fn names_cover_the_whole_catalog() {
        // Spot-check that name() (the internal/test discriminator) matches across shared-field
        // variants, independent of the wire `type` token asserted above.
        assert_eq!(Event::FileReady { path: "p".into(), size: 1 }.name(), "FileReady");
        assert_eq!(
            Event::FileDiscovered { path: "p".into(), size: 1 }.name(),
            "FileDiscovered"
        );
        assert_eq!(
            Event::InstanceActivated { source: "control".into() }.name(),
            "InstanceActivated"
        );
        assert_eq!(
            Event::InstanceDeactivated { source: "control".into() }.name(),
            "InstanceDeactivated"
        );
        assert_eq!(
            Event::ScheduleTriggered { scope: "all".into() }.name(),
            "ScheduleTriggered"
        );
        assert_eq!(Event::WindowOpened { window: "w".into() }.name(), "WindowOpened");
        assert_eq!(Event::WindowClosed { window: "w".into() }.name(), "WindowClosed");
        assert_eq!(
            Event::ScheduleComplete { mode: "window".into() }.name(),
            "ScheduleComplete"
        );
        assert_eq!(Event::Disconnected { link: "s3".into() }.name(), "Disconnected");
        assert_eq!(Event::Reconnected { link: "s3".into() }.name(), "Reconnected");
        assert_eq!(
            Event::RetriesExhausted {
                path: "p".into(),
                destination: "s3".into(),
                attempts: 2,
                last_error: "e".into()
            }
            .name(),
            "RetriesExhausted"
        );
        assert_eq!(Event::FileDeleted { path: "p".into() }.name(), "FileDeleted");
        assert_eq!(
            Event::ReplicationStarted {
                path: "p".into(),
                size: 1,
                destination: "s3".into(),
                attempt: 1
            }
            .name(),
            "ReplicationStarted"
        );
        assert_eq!(
            Event::ReplicationCompleted {
                path: "p".into(),
                size: 1,
                destination: "s3".into(),
                bytes: 1
            }
            .name(),
            "ReplicationCompleted"
        );
        assert_eq!(
            Event::ScanComplete { discovered: 1, awaiting: 2 }.name(),
            "ScanComplete"
        );
    }

    // ---- ProgressThrottle ----------------------------------------------------------------------

    #[test]
    fn throttle_emits_first_zero_and_hundred_always() {
        let mut t = ProgressThrottle::with_defaults();
        assert!(t.should_emit(0, 0), "first observation at 0% emits");
        assert!(!t.should_emit(0, 0), "second 0% at same time does not");
        assert!(t.should_emit(100, 10), "100% always emits");
    }

    #[test]
    fn throttle_gates_by_percent_step() {
        let mut t = ProgressThrottle::new(10, 1_000_000); // huge interval → percent-only
        assert!(t.should_emit(0, 0));
        assert!(!t.should_emit(5, 1), "under 10% step, before interval");
        assert!(t.should_emit(10, 2), "reached the 10% step");
        assert!(!t.should_emit(19, 3), "9% since last, under step");
        assert!(t.should_emit(20, 4), "another 10% step");
    }

    #[test]
    fn throttle_gates_by_interval() {
        let mut t = ProgressThrottle::new(50, 5_000); // big step → interval-driven
        assert!(t.should_emit(0, 0));
        assert!(!t.should_emit(1, 4_999), "1% and under 5s");
        assert!(t.should_emit(2, 5_000), "5s elapsed forces an emit");
        assert!(!t.should_emit(3, 9_999), "under 5s since last emit");
    }

    #[test]
    fn throttle_clamps_nonpositive_step() {
        // A 0/negative step clamps to 1 → every 1% change emits.
        let mut t = ProgressThrottle::new(0, i64::MAX);
        assert!(t.should_emit(0, 0));
        assert!(t.should_emit(1, 0), "1% jump with step clamped to 1");
        assert!(!t.should_emit(1, 0), "no change");
    }

    // ---- Events (disabled / recording) ----------------------------------------------------------

    #[tokio::test]
    async fn disabled_emitter_is_a_silent_no_op() {
        let ev = Events::disabled();
        assert!(!ev.is_enabled());
        // No panic on any variant, including an alarm-bearing one.
        ev.emit(Event::FileDeleted { path: "p".into() }).await;
        ev.emit(Event::Disconnected { link: "s3".into() }).await;
        ev.emit(Event::ComponentReady { instances: 0, version: "v".into() }).await;
    }

    #[tokio::test]
    async fn recording_emitter_captures_every_emitted_event_by_name() {
        use test_support::recording_events;
        let (rec, ev) = recording_events();
        assert!(rec.is_empty());
        ev.emit(Event::FileReady { path: "a/b.csv".into(), size: 42 }).await;
        ev.emit(Event::ComponentReady { instances: 2, version: "0.1.0".into() }).await;

        let ready = rec.events_named("FileReady");
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].body["path"], json!("a/b.csv"));
        assert_eq!(ready[0].body["size"], json!(42));
        assert_eq!(rec.events_named("ComponentReady").len(), 1);
        assert_eq!(rec.events_named("Nope").len(), 0);
        assert!(!rec.is_empty());
    }
}

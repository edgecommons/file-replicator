//! # file-replicator — control-message dispatcher (DESIGN §16)
//!
//! **One-liner purpose**: The inbound half of the control plane — register `get-status` / `trigger`
//! / `set-activation` on the ggcommons component **command inbox** (`gg.commands()`,
//! `ecv1/{device}/file-replicator/main/cmd/#`) and answer via the library's request/reply wrapping.
//! The built-in `ping` / `reload-config` / `get-configuration` verbs (registered by the library)
//! answer everything the old custom `cmd/config` verb used to.
//!
//! ## UNS migration (replaces the hand-rolled `cmd/#` subscribe)
//! [`ControlPlane`] used to own its own `{prefix}/cmd/#` subscription, a hand-rolled topic router
//! ([`crate::uns::parse_cmd`], now deleted), and its own reply envelope. All three are now the
//! library's job: [`ControlPlane::register`] registers three verbs on the shared
//! [`ggcommons::commands::CommandInbox`] (mirroring exactly how opcua-adapter/modbus-adapter register
//! their `sb/*` verbs and telemetry-processor its `flush`/`get-stats`/`pause`/`resume` verbs) — the
//! inbox handles the subscribe, the verb/topic routing, the reply envelope (with the request's
//! `correlation_id`), and the normative `{"ok":true,"result":…}` / `{"ok":false,"error":{code,
//! message}}` body shape. Handlers return `Result<Option<Value>, CommandError>` instead of building
//! their own reply.
//!
//! **Scoping — body field, not topic segment.** The old scheme split `cmd/status` (all instances)
//! from `cmd/instances/{id}/status` (one instance) by TOPIC. The command inbox is one subscription
//! per component (bound to the `main` instance), so scoping now rides an optional `instance` field in
//! the request body — the same convention opcua-adapter/modbus-adapter use for their multi-instance
//! `sb/*` verbs and telemetry-processor uses for `pause`/`resume`'s `route` field. `get-status`/
//! `trigger` omit `instance` for "all"; `set-activation` always requires it (it never had an "all"
//! form).
//!
//! ## get-config — retired, not migrated
//! The custom `get-config` verb (and its optional `legacyConfigTopic` alias) is dropped outright: the
//! library's built-in `get-configuration` verb already answers this, and more safely — it returns the
//! **redacted** effective config (secrets replaced, not just left as unresolved `$secret` refs).
//! There is nothing left for this component to add.
//!
//! ## Instance seam
//! The dispatcher never touches the engine directly — it drives instances through [`InstanceControl`]
//! (id / activation / trigger / destination label / event notification). The real
//! [`crate::instance::Instance`] implements it; unit tests substitute a fake, so routing, status
//! serialization, and activation lifecycle are tested with a real (temp) [`StateStore`] and no live
//! messaging transport (see the module docs' "Testability" note in `crate::events`).
//!
//! ## Instance-scoped events — delegated, not duplicated
//! `InstanceActivated`/`InstanceDeactivated`/a per-instance `ScheduleTriggered` used to publish
//! through the control plane's OWN `Events` emitter (a second copy of the same component-wide topic
//! prefix). Under the UNS core, an event's facade is bound to ONE instance at construction
//! (`gg.instance(id).events()`, minted once when that instance is built — see `crate::app`), so the
//! control plane does not hold a redundant one: it calls [`InstanceControl::notify`] on the SPECIFIC
//! instance it already looked up, which emits through that instance's own bound
//! [`crate::events::Events`]. The control plane keeps only its own component-level `Events` (bound to
//! `gg.events()`, the `main` instance) for scope-`"all"` events (`ScheduleTriggered{scope:"all"}`).
//!
//! ## `state/…` — dropped (see `crate::events` module docs)
//! The retained per-instance/component snapshot publish (`republish_after_transition`/
//! `publish_initial_state`) is removed outright, not migrated — `state` is now a reserved,
//! library-owned UNS class. `get-status` (unchanged status-assembly logic) remains the reliable
//! "current state on demand" path.
//!
//! ## Graceful degradation (DESIGN §6)
//! Messaging may be **absent** on a platform. `gg.commands()` is then `None` and
//! [`ControlPlane::register`] is simply never called by `crate::app`; the P1/P2 replication engine is
//! unaffected.

use std::sync::Arc;

use async_trait::async_trait;
use ggcommons::commands::{command_handler, CommandError, CommandHandler, CommandInbox};
use ggcommons::messaging::Message;
use ggcommons::prelude::Config;
use serde_json::{json, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::domain::{ItemState, WorkItem};
use crate::events::{Event, Events};
use crate::instance::now_ms;
use crate::state::{Stats, StateStore};

/// Upper bound on the per-array file/item listing in a `get-status` reply, so a 100k-file spool never
/// produces a multi-megabyte reply (the `count`/`bytes` totals are always exact regardless).
const MAX_LISTED: usize = 100;

/// Error code: the request named (or was required to name) an instance id this component does not
/// run.
const ERR_UNKNOWN_INSTANCE: &str = "UNKNOWN_INSTANCE";
/// Error code: `set-activation` was sent with no `instance` field (it has no "all" form).
const ERR_INSTANCE_REQUIRED: &str = "INSTANCE_REQUIRED";
/// Error code: `set-activation` carried neither `active` nor `reset: true`.
const ERR_INVALID_REQUEST: &str = "INVALID_REQUEST";
/// Error code: the [`InstanceControl`] handler itself failed (e.g. a durable-state write error).
const ERR_ACTIVATION_FAILED: &str = "ACTIVATION_FAILED";

/// The engine seam the dispatcher drives — the minimal surface `get-status` / `trigger` /
/// `set-activation` need, kept narrow so the control layer is testable with a fake and never depends
/// on the full replication engine. Implemented by [`crate::instance::Instance`].
#[async_trait]
pub trait InstanceControl: Send + Sync {
    /// The instance id (the `component.instances[].id`, unit of control + statistics).
    fn id(&self) -> &str;
    /// The current effective activation state (persisted runtime state wins, DESIGN §7.5).
    fn is_active(&self) -> bool;
    /// The configured `enabled` value (what a `reset` reverts to; reported by `get-status`).
    fn configured_enabled(&self) -> bool;
    /// A short destination label (e.g. `"local"`, `"s3"`) for `inProgress` status rows.
    fn dest_label(&self) -> &str;
    /// The configured schedule mode (`"immediate"`/`"cron"`/`"window"`), reported verbatim by
    /// `get-status` (cron/window execution is P4; the *reported* mode reflects config today).
    fn schedule_mode(&self) -> &str;
    /// Force one scan/replication pass **now** (FR-CTL-3). With `ignore_window = true` the forced tick
    /// **bypasses the schedule gate** and drains all ready work regardless of cron/window state (the
    /// operator override); with `false` it respects the gate (a `cron` instance between fires or a
    /// `window` instance while closed discovers/enqueues but replicates nothing). The control plane
    /// runs this as a detached task so a long-running batch never blocks the reply or holds a
    /// command-subscription slot.
    async fn trigger_scan(&self, now: i64, ignore_window: bool);
    /// Activate/deactivate (FR-CTL-4). When `persist` is true the override is written to durable
    /// state (survives restart, DESIGN §7.5); when false it is a runtime-only flip. Returns the new
    /// effective active state.
    fn apply_activation(
        &self,
        active: bool,
        persist: bool,
        source: &str,
        now: i64,
    ) -> anyhow::Result<bool>;
    /// Clear the persisted activation override, reverting to config `enabled` (DESIGN §7.5 reset).
    /// Returns the new effective active state.
    fn reset_activation(&self, now: i64) -> anyhow::Result<bool>;
    /// Emit `ev` through THIS instance's own bound `events()` facade (see the module docs'
    /// "Instance-scoped events" note) — used by the control plane to notify an activation transition
    /// or a scoped `trigger` without holding a duplicate per-instance emitter itself.
    async fn notify(&self, ev: Event);
}

/// One instance that never started because of a startup permission/access violation (Feature A,
/// `src/permission.rs`), under an `onPermissionError: disableInstance` (the default) policy. Kept so
/// `get-status` reflects it (with its reason) instead of reporting "unknown instance" for an id that
/// was legitimately configured but could not run — DESIGN §16's status surface must not lie about why
/// an expected instance is absent.
#[derive(Debug, Clone)]
pub struct DisabledInstanceInfo {
    pub id: String,
    /// Human-readable summary of why it was disabled (joined violation messages).
    pub reason: String,
    /// The [`crate::permission::Role`] of the first violation (`"ingress"`/`"egress"`/`"archive"`/
    /// `"failed"`) — the primary offending directory role, for a compact status doc.
    pub role: String,
    /// The path of the first violation.
    pub path: String,
}

/// The inbound control-message dispatcher (DESIGN §16).
pub struct ControlPlane {
    config: Arc<Config>,
    store: Arc<dyn StateStore>,
    instances: Vec<Arc<dyn InstanceControl>>,
    /// Component-level ("main" instance) event emitter — scope-`"all"` `ScheduleTriggered` only; every
    /// instance-scoped event is delegated to [`InstanceControl::notify`] instead (module docs).
    events: Events,
    /// Instances that never started (Feature A `onPermissionError: disableInstance`) — see
    /// [`DisabledInstanceInfo`]. Empty unless [`with_disabled`](Self::with_disabled) was called.
    disabled: Vec<DisabledInstanceInfo>,
}

impl ControlPlane {
    /// Assemble the dispatcher. `events` is the component-level ("main" instance) emitter (e.g.
    /// `Events::new(gg.events())`) — see the module docs' "Instance-scoped events" note for why this
    /// is the ONLY emitter the control plane itself holds.
    pub fn new(
        config: Arc<Config>,
        store: Arc<dyn StateStore>,
        instances: Vec<Arc<dyn InstanceControl>>,
        events: Events,
    ) -> Self {
        Self {
            config,
            store,
            instances,
            events,
            disabled: Vec::new(),
        }
    }

    /// Attach the set of instances that never started (Feature A, `onPermissionError:
    /// disableInstance`), so `get-status` reflects them instead of reporting "unknown instance".
    /// Consumes and returns `self` (a small builder step) so existing `ControlPlane::new` call sites
    /// are unaffected when there is nothing disabled.
    pub fn with_disabled(mut self, disabled: Vec<DisabledInstanceInfo>) -> Self {
        self.disabled = disabled;
        self
    }

    /// Registers `get-status` / `trigger` / `set-activation` on the component command inbox
    /// (`gg.commands()`). No-op (a debug log) per verb that fails to register (mirrors
    /// telemetry-processor's `try_register`) — a duplicate/invalid verb name must not crash startup.
    /// Called once by `crate::app` right after the inbox exists; a no-op call site (never made) when
    /// no messaging transport was wired (DESIGN §6).
    pub fn register(self: Arc<Self>, commands: &CommandInbox) {
        {
            let me = self.clone();
            try_register(
                commands,
                "get-status",
                command_handler(move |req| {
                    let me = me.clone();
                    async move { me.on_get_status(req).await }
                }),
            );
        }
        {
            let me = self.clone();
            try_register(
                commands,
                "trigger",
                command_handler(move |req| {
                    let me = me.clone();
                    async move { me.on_trigger(req).await }
                }),
            );
        }
        {
            let me = self.clone();
            try_register(
                commands,
                "set-activation",
                command_handler(move |req| {
                    let me = me.clone();
                    async move { me.on_set_activation(req).await }
                }),
            );
        }
    }

    // ---- handlers -------------------------------------------------------------------------------

    /// `get-status` → per-instance (scoped by an `instance` body field) or component-wide statistics
    /// (DESIGN §16). A scoped request for a disabled instance id (Feature A) returns its disabled doc,
    /// not `UNKNOWN_INSTANCE` — it was legitimately configured, just never started.
    async fn on_get_status(&self, req: Message) -> Result<Option<Value>, CommandError> {
        let now = now_ms();
        match instance_of(&req) {
            None => Ok(Some(self.component_status_json(now))),
            Some(id) => match self.find(id) {
                Some(inst) => Ok(Some(self.instance_status(inst.as_ref(), now))),
                None => match self.find_disabled(id) {
                    Some(info) => Ok(Some(disabled_status_json(info))),
                    None => Err(unknown_instance(id)),
                },
            },
        }
    }

    /// `trigger` → force a scan now for one instance (`instance` body field) or all (FR-CTL-3),
    /// emitting `ScheduleTriggered`.
    ///
    /// The scan is run as a **detached task** and the reply is sent immediately ("accepted + counts",
    /// DESIGN §16): a scan is a full deliver→verify→complete batch (potentially a multi-GB/slow-S3
    /// upload), so awaiting it here would time out the request/reply client AND hold a command
    /// subscription slot for the whole transfer, starving get-status / an emergency set-activation. An
    /// inactive instance is **skipped** (its `tick` is a no-op), not falsely reported as triggered.
    ///
    /// `ignoreWindow` (default `false`) is threaded into [`InstanceControl::trigger_scan`]: when `true`
    /// the forced tick bypasses the P4 schedule gate and replicates all ready work regardless of the
    /// instance's cron/window state; when `false` the trigger respects the gate. `triggered` counts
    /// the instances whose scan was **dispatched**, not files sent (the async batch's outcome is not
    /// known at reply time).
    async fn on_trigger(&self, req: Message) -> Result<Option<Value>, CommandError> {
        let now = now_ms();
        let ignore_window = lenient_bool(req.body.get("ignoreWindow")).unwrap_or(false);

        match instance_of(&req) {
            None => {
                let mut triggered: Vec<String> = Vec::new();
                let mut skipped: Vec<String> = Vec::new();
                for inst in &self.instances {
                    if inst.is_active() {
                        spawn_trigger(inst.clone(), now, ignore_window);
                        triggered.push(inst.id().to_string());
                    } else {
                        skipped.push(inst.id().to_string());
                    }
                }
                self.events.emit(Event::ScheduleTriggered { scope: "all".into() }).await;
                Ok(Some(json!({
                    "scope": "all", "triggered": triggered.len(),
                    "instances": triggered, "skipped": skipped, "ignoreWindow": ignore_window
                })))
            }
            Some(id) => match self.find(id) {
                Some(inst) => {
                    if !inst.is_active() {
                        // A deactivated instance's tick is a no-op — report the truth, emit nothing.
                        Ok(Some(json!({
                            "scope": id, "triggered": 0, "skipped": "inactive",
                            "ignoreWindow": ignore_window
                        })))
                    } else {
                        spawn_trigger(inst.clone(), now, ignore_window);
                        inst.notify(Event::ScheduleTriggered { scope: id.to_string() }).await;
                        Ok(Some(json!({ "scope": id, "triggered": 1, "ignoreWindow": ignore_window })))
                    }
                }
                None => Err(unknown_instance(id)),
            },
        }
    }

    /// `set-activation` → activate/deactivate/reset an instance (FR-CTL-4 / DESIGN §7.5), emitting
    /// `InstanceActivated` / `InstanceDeactivated` via [`InstanceControl::notify`]. Unlike
    /// `get-status`/`trigger`, this has no "all" form — `instance` is required.
    async fn on_set_activation(&self, req: Message) -> Result<Option<Value>, CommandError> {
        let now = now_ms();
        let Some(id) = instance_of(&req) else {
            return Err(CommandError::new(
                ERR_INSTANCE_REQUIRED,
                "set-activation requires an \"instance\" field (it has no \"all\" form)",
            ));
        };
        let Some(inst) = self.find(id) else {
            return Err(unknown_instance(id));
        };

        // Field-by-field lenient parse (NOT whole-body `serde_json::from_value`): a Greengrass-style
        // numeric double (`{"active": false, "persist": 1}`) must not fail deserialization and get
        // silently discarded — that would drop a real deactivate while the instance keeps replicating
        // (FR-CFG-3 lenient-number convention).
        let reset = lenient_bool(req.body.get("reset")).unwrap_or(false);
        let active = lenient_bool(req.body.get("active"));

        if reset {
            return match inst.reset_activation(now) {
                Ok(effective) => {
                    self.emit_activation(inst.as_ref(), effective).await;
                    Ok(Some(json!({
                        "instance": id, "active": effective,
                        "configuredEnabled": inst.configured_enabled(), "reset": true
                    })))
                }
                Err(e) => Err(CommandError::new(ERR_ACTIVATION_FAILED, e.to_string())),
            };
        }
        if let Some(active) = active {
            let persist = lenient_bool(req.body.get("persist")).unwrap_or(true);
            return match inst.apply_activation(active, persist, "control", now) {
                Ok(effective) => {
                    self.emit_activation(inst.as_ref(), effective).await;
                    Ok(Some(json!({
                        "instance": id, "active": effective,
                        "configuredEnabled": inst.configured_enabled(), "persisted": persist
                    })))
                }
                Err(e) => Err(CommandError::new(ERR_ACTIVATION_FAILED, e.to_string())),
            };
        }
        Err(CommandError::new(
            ERR_INVALID_REQUEST,
            "set-activation requires `active` (bool) or `reset: true`",
        ))
    }

    /// Notify the activation lifecycle event matching the new effective state, through `inst`'s own
    /// bound facade.
    async fn emit_activation(&self, inst: &dyn InstanceControl, active: bool) {
        let ev = if active {
            Event::InstanceActivated { source: "control".into() }
        } else {
            Event::InstanceDeactivated { source: "control".into() }
        };
        inst.notify(ev).await;
    }

    // ---- status assembly ------------------------------------------------------------------------

    /// Component-wide status: every LIVE instance's snapshot, plus every DISABLED instance's doc
    /// (Feature A), plus a summary. A disabled instance is never counted `active`, but it IS counted in
    /// the roster total so the summary accounts for every configured instance, running or not.
    fn component_status_json(&self, now: i64) -> Value {
        let mut instances: Vec<Value> = self
            .instances
            .iter()
            .map(|i| self.instance_status(i.as_ref(), now))
            .collect();
        instances.extend(self.disabled.iter().map(disabled_status_json));
        let active = self.instances.iter().filter(|i| i.is_active()).count();
        json!({
            "component": self.config.component_name,
            "thing": self.config.thing_name,
            "instances": instances,
            "summary": {
                "instances": self.instances.len() + self.disabled.len(),
                "active": active,
                "disabled": self.disabled.len(),
            },
        })
    }

    /// One instance's status, sourced from the durable store (awaiting / in-progress / failed rows +
    /// running stats) and its live activation/destination state.
    fn instance_status(&self, inst: &dyn InstanceControl, now: i64) -> Value {
        instance_state_snapshot(
            self.store.as_ref(),
            inst.id(),
            inst.is_active(),
            inst.configured_enabled(),
            inst.schedule_mode(),
            inst.dest_label(),
            now,
        )
    }

    /// Look up a live instance handle by id.
    fn find(&self, id: &str) -> Option<&Arc<dyn InstanceControl>> {
        self.instances.iter().find(|i| i.id() == id)
    }

    /// Look up a disabled instance's info by id (Feature A).
    fn find_disabled(&self, id: &str) -> Option<&DisabledInstanceInfo> {
        self.disabled.iter().find(|d| d.id == id)
    }
}

/// Registers a verb, logging (not failing) if the inbox rejects it — mirrors
/// telemetry-processor's `try_register`.
fn try_register(commands: &CommandInbox, verb: &str, handler: Arc<dyn CommandHandler>) {
    if let Err(e) = commands.register(verb, handler) {
        tracing::warn!(verb, error = %e, "failed to register command verb");
    }
}

/// The request body's `"instance"` selector (absent → "all", except `set-activation`, which requires
/// it — see [`ControlPlane::on_set_activation`]).
fn instance_of(req: &Message) -> Option<&str> {
    req.body.get("instance").and_then(Value::as_str)
}

/// A coded error for a request that named (or was required to name) an instance id this component
/// does not run.
fn unknown_instance(id: &str) -> CommandError {
    CommandError::new(ERR_UNKNOWN_INSTANCE, format!("unknown instance '{id}'"))
}

/// Build the compact status doc for an instance that never started (Feature A, `onPermissionError:
/// disableInstance`) — the disabled-instance counterpart of [`instance_status_json`], shaped so a
/// consumer parsing `get-status` doesn't need a separate schema for the disabled case: same top-level
/// keys, `active`/`awaiting`/`inProgress`/`replicated`/`failed` all zeroed/empty, plus `disabled:
/// true` and `disabledReason` so the "why" is never lost.
fn disabled_status_json(info: &DisabledInstanceInfo) -> Value {
    json!({
        "instance": info.id,
        "active": false,
        "configuredEnabled": false,
        "disabled": true,
        "disabledReason": info.reason,
        "disabledRole": info.role,
        "disabledPath": info.path,
        "schedule": { "mode": "disabled" },
        "awaiting": { "count": 0, "bytes": 0, "files": [] },
        "inProgress": [],
        "replicated": { "count": 0, "bytes": 0 },
        "failed": { "count": 0, "items": [] },
    })
}

/// Coerce a JSON value to a bool, tolerating a Greengrass numeric double (`0`/`0.0` → false, any
/// other finite number → true) and a `"true"`/`"false"`/`"1"`/`"0"` string, matching the codebase's
/// lenient-number convention (FR-CFG-3). `None` for absent/null/uncoercible values.
fn lenient_bool(v: Option<&Value>) -> Option<bool> {
    match v? {
        Value::Bool(b) => Some(*b),
        Value::Number(n) => n.as_f64().map(|f| f != 0.0),
        Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "1" => Some(true),
            "false" | "0" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

/// Spawn a detached task that runs one forced scan for `inst`. Keeps the trigger handler from
/// awaiting a full (possibly multi-GB) replication batch, so the reply returns promptly and no
/// command-subscription slot is held for a transfer's duration (DESIGN §16 / FR-CTL-3).
/// `ignore_window` is threaded through so a forced trigger can bypass the P4 schedule gate.
fn spawn_trigger(inst: Arc<dyn InstanceControl>, now: i64, ignore_window: bool) {
    tokio::spawn(async move {
        inst.trigger_scan(now, ignore_window).await;
    });
}

/// The already-fetched inputs to a per-instance status document (pure — no store access), so the
/// serialization is unit-tested directly.
struct StatusInputs<'a> {
    id: &'a str,
    active: bool,
    configured_enabled: bool,
    /// Configured schedule mode (`"immediate"`/`"cron"`/`"window"`) — reported verbatim (§16).
    schedule_mode: &'a str,
    dest_label: &'a str,
    stats: Stats,
    awaiting: &'a [WorkItem],
    in_progress: &'a [WorkItem],
    failed: &'a [WorkItem],
}

/// Build the compact per-instance state/status document from the durable store (DESIGN §16). Used by
/// the control plane's `get-status` reply (`crate::instance::Instance` no longer republishes a
/// parallel retained snapshot — see the module docs' "`state/…` — dropped" note). Side-effect-free
/// aside from the store reads (each falling back to empty/default on a store fault, so status is
/// best-effort and never panics).
pub(crate) fn instance_state_snapshot(
    store: &dyn StateStore,
    id: &str,
    active: bool,
    configured_enabled: bool,
    schedule_mode: &str,
    dest_label: &str,
    now: i64,
) -> Value {
    let stats = store.stats(id).unwrap_or_default();
    let awaiting = store.list_by_state(id, ItemState::Ready).unwrap_or_default();
    let in_progress = store
        .list_by_state(id, ItemState::InProgress)
        .unwrap_or_default();
    let mut failed = store.list_by_state(id, ItemState::Failed).unwrap_or_default();
    failed.extend(store.list_by_state(id, ItemState::Exhausted).unwrap_or_default());
    failed.extend(
        store
            .list_by_state(id, ItemState::Quarantined)
            .unwrap_or_default(),
    );
    instance_status_json(
        &StatusInputs {
            id,
            active,
            configured_enabled,
            schedule_mode,
            dest_label,
            stats,
            awaiting: &awaiting,
            in_progress: &in_progress,
            failed: &failed,
        },
        now,
    )
}

/// Build the per-instance `get-status` document (DESIGN §16). Pure: `now` (Unix ms) is injected so
/// `ageSecs`/`quarantinedAt` are deterministic in tests.
fn instance_status_json(inp: &StatusInputs, now: i64) -> Value {
    let awaiting_bytes: u64 = inp.awaiting.iter().map(|w| w.size).sum();
    let awaiting_files: Vec<Value> = inp
        .awaiting
        .iter()
        .take(MAX_LISTED)
        .map(|w| {
            json!({ "path": w.relpath, "size": w.size, "ageSecs": age_secs(now, w.discovered_at) })
        })
        .collect();

    let in_progress: Vec<Value> = inp
        .in_progress
        .iter()
        .take(MAX_LISTED)
        .map(|w| {
            json!({
                "path": w.relpath, "size": w.size, "bytesDone": w.bytes_done,
                "percent": percent(w.bytes_done, w.size),
                "destination": inp.dest_label, "attempt": w.attempts + 1
            })
        })
        .collect();

    let failed_items: Vec<Value> = inp
        .failed
        .iter()
        .take(MAX_LISTED)
        .map(|w| {
            let mut m = json!({
                "path": w.relpath, "attempts": w.attempts,
                "lastError": w.last_error.clone().unwrap_or_default(),
                "state": w.state.as_str()
            });
            if w.state == ItemState::Quarantined {
                m["quarantinedAt"] = json!(rfc3339_ms(w.updated_at));
            }
            m
        })
        .collect();

    // `schedule.mode` reports the CONFIGURED mode (§16), never a hardcoded literal. `link` is
    // deliberately OMITTED until the P4 destination circuit-breaker exists — asserting "Connected"
    // for an instance failing every transfer against an unreachable endpoint would be a lie.
    json!({
        "instance": inp.id,
        "active": inp.active,
        "configuredEnabled": inp.configured_enabled,
        "schedule": { "mode": inp.schedule_mode },
        "awaiting": { "count": inp.awaiting.len(), "bytes": awaiting_bytes, "files": awaiting_files },
        "inProgress": in_progress,
        "replicated": { "count": inp.stats.replicated, "bytes": inp.stats.bytes },
        "failed": { "count": inp.failed.len(), "items": failed_items },
    })
}

/// Percent complete (`0.0..=100.0`, one decimal). A zero-length file is `0%` until any byte lands,
/// then `100%`.
fn percent(bytes_done: u64, size: u64) -> f64 {
    if size == 0 {
        return if bytes_done == 0 { 0.0 } else { 100.0 };
    }
    let p = (bytes_done as f64 / size as f64) * 100.0;
    ((p * 10.0).round() / 10.0).clamp(0.0, 100.0)
}

/// Whole seconds a work item has been awaiting (never negative).
fn age_secs(now: i64, discovered_at: i64) -> i64 {
    (now - discovered_at).max(0) / 1000
}

/// Format a Unix-millis instant as an RFC3339 UTC string; epoch on the (practically impossible)
/// out-of-range/format failure.
fn rfc3339_ms(now_ms: i64) -> String {
    OffsetDateTime::from_unix_timestamp_nanos((now_ms as i128) * 1_000_000)
        .ok()
        .and_then(|dt| dt.format(&Rfc3339).ok())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    use crate::events::test_support::{recording_events, EventRecorder};
    use crate::state::SqliteStore;

    /// Poll `pred` (yielding to the runtime between checks) until it holds or a short deadline elapses.
    /// `trigger` dispatches the scan as a detached task (so the reply never blocks on a transfer), so a
    /// trigger's side effect (the `FakeInstance` counter) is observed just after the handler returns,
    /// not synchronously.
    async fn eventually(pred: impl Fn() -> bool) {
        for _ in 0..200 {
            if pred() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("condition not met within the deadline");
    }

    // ---- fake instance handle -------------------------------------------------------------------

    struct FakeInstance {
        id: String,
        active: AtomicBool,
        configured_enabled: bool,
        dest: String,
        schedule_mode: &'static str,
        triggers: AtomicUsize,
        /// The `ignore_window` flag of the most recent `trigger_scan` (verifies FR-CTL-3 threading).
        last_ignore_window: AtomicBool,
        last_apply: Mutex<Option<(bool, bool)>>,
        resets: AtomicUsize,
        fail: bool,
        /// Every event `notify`d on this instance (module docs: the control plane delegates
        /// instance-scoped events here instead of holding a duplicate emitter).
        notified: Mutex<Vec<Event>>,
    }

    impl FakeInstance {
        fn new(id: &str, active: bool, configured_enabled: bool) -> Arc<Self> {
            Self::with_schedule(id, active, configured_enabled, "immediate")
        }
        fn with_schedule(
            id: &str,
            active: bool,
            configured_enabled: bool,
            schedule_mode: &'static str,
        ) -> Arc<Self> {
            Arc::new(Self {
                id: id.to_string(),
                active: AtomicBool::new(active),
                configured_enabled,
                dest: "s3".to_string(),
                schedule_mode,
                triggers: AtomicUsize::new(0),
                last_ignore_window: AtomicBool::new(false),
                last_apply: Mutex::new(None),
                resets: AtomicUsize::new(0),
                fail: false,
                notified: Mutex::new(Vec::new()),
            })
        }
        fn failing(id: &str) -> Arc<Self> {
            Arc::new(Self {
                id: id.to_string(),
                active: AtomicBool::new(true),
                configured_enabled: true,
                dest: "s3".to_string(),
                schedule_mode: "immediate",
                triggers: AtomicUsize::new(0),
                last_ignore_window: AtomicBool::new(false),
                last_apply: Mutex::new(None),
                resets: AtomicUsize::new(0),
                fail: true,
                notified: Mutex::new(Vec::new()),
            })
        }
        fn notified_named(&self, name: &str) -> Vec<Event> {
            self.notified
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.name() == name)
                .cloned()
                .collect()
        }
    }

    #[async_trait]
    impl InstanceControl for FakeInstance {
        fn id(&self) -> &str {
            &self.id
        }
        fn is_active(&self) -> bool {
            self.active.load(Ordering::SeqCst)
        }
        fn configured_enabled(&self) -> bool {
            self.configured_enabled
        }
        fn dest_label(&self) -> &str {
            &self.dest
        }
        fn schedule_mode(&self) -> &str {
            self.schedule_mode
        }
        async fn trigger_scan(&self, _now: i64, ignore_window: bool) {
            self.triggers.fetch_add(1, Ordering::SeqCst);
            self.last_ignore_window.store(ignore_window, Ordering::SeqCst);
        }
        fn apply_activation(
            &self,
            active: bool,
            persist: bool,
            _source: &str,
            _now: i64,
        ) -> anyhow::Result<bool> {
            if self.fail {
                anyhow::bail!("apply failed");
            }
            *self.last_apply.lock().unwrap() = Some((active, persist));
            self.active.store(active, Ordering::SeqCst);
            Ok(active)
        }
        fn reset_activation(&self, _now: i64) -> anyhow::Result<bool> {
            if self.fail {
                anyhow::bail!("reset failed");
            }
            self.resets.fetch_add(1, Ordering::SeqCst);
            self.active.store(self.configured_enabled, Ordering::SeqCst);
            Ok(self.configured_enabled)
        }
        async fn notify(&self, ev: Event) {
            self.notified.lock().unwrap().push(ev);
        }
    }

    // ---- fixtures -------------------------------------------------------------------------------

    fn cfg() -> Arc<Config> {
        Arc::new(
            Config::from_value(
                "com.mbreissi.edgecommons.FileReplicator",
                "gw-01",
                json!({ "tags": { "site": "factory-1" }, "logging": { "level": "INFO" } }),
            )
            .unwrap(),
        )
    }

    /// A dispatcher over a real in-memory store + the given instances, with a recording component-
    /// level `Events` emitter.
    fn plane(
        store: Arc<dyn StateStore>,
        instances: Vec<Arc<dyn InstanceControl>>,
    ) -> (Arc<EventRecorder>, ControlPlane) {
        let (rec, events) = recording_events();
        (rec, ControlPlane::new(cfg(), store, instances, events))
    }

    fn mem_store() -> Arc<dyn StateStore> {
        Arc::new(SqliteStore::open_in_memory().unwrap())
    }

    fn req(name: &str, body: Value) -> Message {
        ggcommons::messaging::MessageBuilder::new(name, "1.0")
            .reply_to("reply/here")
            .correlation_id("corr-1")
            .payload(body)
            .build()
    }

    fn work_item(relpath: &str, state: ItemState, size: u64) -> WorkItem {
        WorkItem {
            instance: "i1".into(),
            relpath: relpath.into(),
            abs_source: PathBuf::from(relpath),
            state,
            size,
            discovered_at: 0,
            attempts: 0,
            next_attempt_at: 0,
            last_error: None,
            bytes_done: 0,
            updated_at: 0,
        }
    }

    // ---- pure helpers ---------------------------------------------------------------------------

    #[test]
    fn percent_rounds_and_handles_zero_length() {
        assert_eq!(percent(0, 0), 0.0);
        assert_eq!(percent(1, 0), 100.0);
        assert_eq!(percent(30, 100), 30.0);
        assert_eq!(percent(1, 3), 33.3);
        assert_eq!(percent(2, 3), 66.7);
        assert_eq!(percent(200, 100), 100.0, "clamped to 100");
    }

    #[test]
    fn age_secs_is_whole_seconds_and_never_negative() {
        assert_eq!(age_secs(5_000, 2_000), 3);
        assert_eq!(age_secs(1_000, 2_000), 0, "clamped to 0 for a future discovered_at");
        assert_eq!(age_secs(9_999, 0), 9);
    }

    #[test]
    fn rfc3339_formats_epoch_and_known_instant() {
        assert_eq!(rfc3339_ms(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339_ms(1_782_897_322_000), "2026-07-01T09:15:22Z");
    }

    #[test]
    fn instance_status_json_shapes_every_section() {
        let awaiting = vec![work_item("a.csv", ItemState::Ready, 40)];
        let mut ip = work_item("big.parquet", ItemState::InProgress, 100);
        ip.bytes_done = 30;
        ip.attempts = 0;
        let in_progress = vec![ip];
        let mut q = work_item("x.csv", ItemState::Quarantined, 5);
        q.attempts = 24;
        q.last_error = Some("boom".into());
        q.updated_at = 1_782_897_322_000;
        let mut f = work_item("y.csv", ItemState::Failed, 7);
        f.attempts = 2;
        f.last_error = Some("later".into());
        let failed = vec![f, q];

        let inp = StatusInputs {
            id: "i1",
            active: true,
            configured_enabled: false,
            schedule_mode: "window",
            dest_label: "s3",
            stats: Stats { replicated: 4310, failed: 2, bytes: 90 },
            awaiting: &awaiting,
            in_progress: &in_progress,
            failed: &failed,
        };
        let v = instance_status_json(&inp, 100_000);

        assert_eq!(v["instance"], json!("i1"));
        assert_eq!(v["active"], json!(true));
        assert_eq!(v["configuredEnabled"], json!(false));
        assert_eq!(v["schedule"]["mode"], json!("window"), "reports the configured mode, not a literal");
        assert!(v.get("link").is_none(), "link omitted until the P4 circuit-breaker exists");
        // awaiting
        assert_eq!(v["awaiting"]["count"], json!(1));
        assert_eq!(v["awaiting"]["bytes"], json!(40));
        assert_eq!(v["awaiting"]["files"][0]["path"], json!("a.csv"));
        assert_eq!(v["awaiting"]["files"][0]["ageSecs"], json!(100));
        // inProgress
        assert_eq!(v["inProgress"][0]["percent"], json!(30.0));
        assert_eq!(v["inProgress"][0]["bytesDone"], json!(30));
        assert_eq!(v["inProgress"][0]["destination"], json!("s3"));
        assert_eq!(v["inProgress"][0]["attempt"], json!(1));
        // replicated
        assert_eq!(v["replicated"]["count"], json!(4310));
        assert_eq!(v["replicated"]["bytes"], json!(90));
        // failed
        assert_eq!(v["failed"]["count"], json!(2));
        assert_eq!(v["failed"]["items"][0]["lastError"], json!("later"));
        assert_eq!(v["failed"]["items"][0]["state"], json!("failed"));
        assert!(v["failed"]["items"][0].get("quarantinedAt").is_none());
        assert_eq!(v["failed"]["items"][1]["state"], json!("quarantined"));
        assert_eq!(v["failed"]["items"][1]["quarantinedAt"], json!("2026-07-01T09:15:22Z"));
    }

    // ---- get-status -----------------------------------------------------------------------------

    fn seed_status(store: &Arc<dyn StateStore>) {
        // 2 awaiting, 1 in-progress (30%), 1 failed, 1 quarantined; plus rollup stats.
        store.upsert_ready("i1", "a.csv", 40, 0, 1_000).unwrap();
        store.upsert_ready("i1", "b.csv", 60, 0, 2_000).unwrap();
        store.upsert_ready("i1", "big.bin", 100, 0, 3_000).unwrap();
        store.set_state("i1", "big.bin", ItemState::InProgress, 3_500).unwrap();
        store.set_bytes_done("i1", "big.bin", 30, 3_500).unwrap();
        store.upsert_ready("i1", "bad.csv", 7, 0, 4_000).unwrap();
        store.record_attempt("i1", "bad.csv", "boom", ItemState::Failed, 0, 4_500).unwrap();
        store.upsert_ready("i1", "gone.csv", 9, 0, 5_000).unwrap();
        store.set_state("i1", "gone.csv", ItemState::Quarantined, 5_500).unwrap();
        store.bump_stats("i1", crate::state::StatsDelta { replicated: 10, failed: 3, bytes: 999 }).unwrap();
    }

    #[tokio::test]
    async fn get_status_for_one_instance_reports_store_and_live_state() {
        let store = mem_store();
        seed_status(&store);
        let inst = FakeInstance::new("i1", true, true);
        let (_rec, cp) = plane(store, vec![inst]);

        let body = cp.on_get_status(req("get-status", json!({ "instance": "i1" }))).await.unwrap();
        let body = body.unwrap();
        assert_eq!(body["instance"], json!("i1"));
        assert_eq!(body["active"], json!(true));
        assert_eq!(body["awaiting"]["count"], json!(2));
        assert_eq!(body["awaiting"]["bytes"], json!(100));
        assert_eq!(body["inProgress"][0]["path"], json!("big.bin"));
        assert_eq!(body["inProgress"][0]["percent"], json!(30.0));
        assert_eq!(body["replicated"]["count"], json!(10));
        assert_eq!(body["replicated"]["bytes"], json!(999));
        assert_eq!(body["failed"]["count"], json!(2), "failed + quarantined");
    }

    #[tokio::test]
    async fn get_status_all_wraps_every_instance_with_summary() {
        let store = mem_store();
        seed_status(&store);
        let instances: Vec<Arc<dyn InstanceControl>> = vec![
            FakeInstance::new("i1", true, true),
            FakeInstance::new("i2", false, true),
        ];
        let (_rec, cp) = plane(store, instances);

        let body = cp.on_get_status(req("get-status", json!({}))).await.unwrap().unwrap();
        assert_eq!(body["component"], json!("com.mbreissi.edgecommons.FileReplicator"));
        assert_eq!(body["thing"], json!("gw-01"));
        assert_eq!(body["instances"].as_array().unwrap().len(), 2);
        assert_eq!(body["summary"]["instances"], json!(2));
        assert_eq!(body["summary"]["active"], json!(1));
    }

    #[tokio::test]
    async fn get_status_unknown_instance_is_a_coded_error() {
        let (_rec, cp) = plane(mem_store(), vec![FakeInstance::new("i1", true, true)]);
        let err = cp
            .on_get_status(req("get-status", json!({ "instance": "nope" })))
            .await
            .unwrap_err();
        assert_eq!(err.code, ERR_UNKNOWN_INSTANCE);
    }

    // ---- Feature A: disabled-instance visibility in get-status (`src/permission.rs`) ---------------

    fn disabled_info(id: &str) -> DisabledInstanceInfo {
        DisabledInstanceInfo {
            id: id.to_string(),
            reason: "permission denied: /data/in".to_string(),
            role: "ingress".to_string(),
            path: "/data/in".to_string(),
        }
    }

    #[tokio::test]
    async fn get_status_scoped_reports_disabled_instance_with_reason() {
        let (_rec, cp) = plane(mem_store(), vec![]);
        let cp = cp.with_disabled(vec![disabled_info("bad")]);
        let body = cp
            .on_get_status(req("get-status", json!({ "instance": "bad" })))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(body["instance"], json!("bad"));
        assert_eq!(body["active"], json!(false));
        assert_eq!(body["disabled"], json!(true));
        assert_eq!(body["disabledReason"], json!("permission denied: /data/in"));
        assert_eq!(body["disabledRole"], json!("ingress"));
        assert_eq!(body["awaiting"]["count"], json!(0));
    }

    #[tokio::test]
    async fn get_status_scoped_still_errors_for_a_truly_unknown_id() {
        // A disabled-instance entry for "bad" must not make every OTHER unknown id resolve too.
        let (_rec, cp) = plane(mem_store(), vec![]);
        let cp = cp.with_disabled(vec![disabled_info("bad")]);
        let err = cp
            .on_get_status(req("get-status", json!({ "instance": "nope" })))
            .await
            .unwrap_err();
        assert_eq!(err.code, ERR_UNKNOWN_INSTANCE);
    }

    #[tokio::test]
    async fn get_status_component_roster_includes_disabled() {
        let store = mem_store();
        seed_status(&store);
        let (_rec, cp) = plane(store, vec![FakeInstance::new("i1", true, true)]);
        let cp = cp.with_disabled(vec![disabled_info("bad")]);

        let body = cp.on_get_status(req("get-status", json!({}))).await.unwrap().unwrap();
        assert_eq!(body["instances"].as_array().unwrap().len(), 2, "live + disabled");
        assert_eq!(body["summary"]["instances"], json!(2));
        assert_eq!(body["summary"]["active"], json!(1), "the disabled instance is never active");
        assert_eq!(body["summary"]["disabled"], json!(1));
        let disabled_doc = body["instances"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["instance"] == json!("bad"))
            .expect("disabled instance present in the roster");
        assert_eq!(disabled_doc["disabled"], json!(true));
    }

    #[tokio::test]
    async fn live_instance_status_shape_unchanged_by_the_disabled_feature() {
        // Regression: a live instance's status doc must not grow a `disabled` field just because
        // OTHER instances in the component are disabled.
        let store = mem_store();
        seed_status(&store);
        let (_rec, cp) = plane(store, vec![FakeInstance::new("i1", true, true)]);
        let cp = cp.with_disabled(vec![disabled_info("bad")]);
        let body = cp
            .on_get_status(req("get-status", json!({ "instance": "i1" })))
            .await
            .unwrap()
            .unwrap();
        assert!(body.get("disabled").is_none(), "a live instance's doc has no `disabled` key at all");
    }

    // ---- trigger --------------------------------------------------------------------------------

    #[tokio::test]
    async fn trigger_all_scans_every_instance_and_emits_component_event() {
        let i1 = FakeInstance::new("i1", true, true);
        let i2 = FakeInstance::new("i2", true, true);
        let (rec, cp) = plane(mem_store(), vec![i1.clone(), i2.clone()]);

        let body = cp
            .on_trigger(req("trigger", json!({ "ignoreWindow": true })))
            .await
            .unwrap()
            .unwrap();

        // The reply is sent immediately ("accepted + counts"); the scans run as detached tasks.
        assert_eq!(body["scope"], json!("all"));
        assert_eq!(body["triggered"], json!(2));
        assert_eq!(body["skipped"], json!([]));
        assert_eq!(body["ignoreWindow"], json!(true));
        assert_eq!(rec.events_named("ScheduleTriggered").len(), 1, "one component-level event");
        eventually(|| {
            i1.triggers.load(Ordering::SeqCst) == 1 && i2.triggers.load(Ordering::SeqCst) == 1
        })
        .await;
        // FR-CTL-3: `ignoreWindow: true` from the request is threaded down to each instance's scan.
        assert!(i1.last_ignore_window.load(Ordering::SeqCst));
        assert!(i2.last_ignore_window.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn trigger_one_scans_only_that_instance_and_notifies_it() {
        let i1 = FakeInstance::new("i1", true, true);
        let i2 = FakeInstance::new("i2", true, true);
        let (rec, cp) = plane(mem_store(), vec![i1.clone(), i2.clone()]);

        let body = cp
            .on_trigger(req("trigger", json!({ "instance": "i1" })))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(body["scope"], json!("i1"));
        assert_eq!(body["triggered"], json!(1));
        assert_eq!(i1.notified_named("ScheduleTriggered").len(), 1, "notified via InstanceControl, not a duplicate emitter");
        assert!(rec.events_named("ScheduleTriggered").is_empty(), "the component-level emitter is untouched for a scoped trigger");
        eventually(|| i1.triggers.load(Ordering::SeqCst) == 1).await;
        assert_eq!(i2.triggers.load(Ordering::SeqCst), 0, "only i1 triggered");
        // No `ignoreWindow` in the request → the scan respects the schedule gate (default false).
        assert!(!i1.last_ignore_window.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn trigger_unknown_instance_is_a_coded_error_and_scans_nothing() {
        let i1 = FakeInstance::new("i1", true, true);
        let (_rec, cp) = plane(mem_store(), vec![i1.clone()]);
        let err = cp
            .on_trigger(req("trigger", json!({ "instance": "nope" })))
            .await
            .unwrap_err();
        assert_eq!(i1.triggers.load(Ordering::SeqCst), 0);
        assert_eq!(err.code, ERR_UNKNOWN_INSTANCE);
    }

    #[tokio::test]
    async fn trigger_deactivated_instance_reports_skipped_and_scans_nothing() {
        // Triggering a deactivated instance must not falsely ack `triggered:1` — its tick is a no-op.
        let i1 = FakeInstance::new("i1", false, true); // deactivated
        let (_rec, cp) = plane(mem_store(), vec![i1.clone()]);
        let body = cp
            .on_trigger(req("trigger", json!({ "instance": "i1" })))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(body["triggered"], json!(0));
        assert_eq!(body["skipped"], json!("inactive"));
        assert_eq!(i1.triggers.load(Ordering::SeqCst), 0, "no scan forced on a deactivated instance");
        assert!(
            i1.notified_named("ScheduleTriggered").is_empty(),
            "no ScheduleTriggered notified for a skipped instance"
        );
    }

    #[tokio::test]
    async fn trigger_all_counts_only_active_instances() {
        let on = FakeInstance::new("on", true, true);
        let off = FakeInstance::new("off", false, true);
        let (_rec, cp) = plane(mem_store(), vec![on.clone(), off.clone()]);
        let body = cp.on_trigger(req("trigger", json!({}))).await.unwrap().unwrap();
        assert_eq!(body["triggered"], json!(1));
        assert_eq!(body["instances"], json!(["on"]));
        assert_eq!(body["skipped"], json!(["off"]));
        eventually(|| on.triggers.load(Ordering::SeqCst) == 1).await;
        assert_eq!(off.triggers.load(Ordering::SeqCst), 0, "inactive instance never triggered");
    }

    // ---- set-activation -------------------------------------------------------------------------

    #[tokio::test]
    async fn set_activation_deactivate_persists_by_default_and_notifies() {
        let i1 = FakeInstance::new("i1", true, true);
        let (_rec, cp) = plane(mem_store(), vec![i1.clone()]);

        let body = cp
            .on_set_activation(req("set-activation", json!({ "instance": "i1", "active": false })))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(*i1.last_apply.lock().unwrap(), Some((false, true)), "persist defaults true");
        assert!(!i1.is_active());
        assert_eq!(body["active"], json!(false));
        assert_eq!(body["persisted"], json!(true));
        assert_eq!(body["configuredEnabled"], json!(true));
        assert_eq!(i1.notified_named("InstanceDeactivated").len(), 1);
    }

    #[tokio::test]
    async fn set_activation_activate_non_persistent_flips_runtime_only() {
        let i1 = FakeInstance::new("i1", false, false);
        let (_rec, cp) = plane(mem_store(), vec![i1.clone()]);

        let body = cp
            .on_set_activation(req(
                "set-activation",
                json!({ "instance": "i1", "active": true, "persist": false }),
            ))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(*i1.last_apply.lock().unwrap(), Some((true, false)));
        assert!(i1.is_active());
        assert_eq!(body["persisted"], json!(false));
        assert_eq!(i1.notified_named("InstanceActivated").len(), 1);
    }

    #[tokio::test]
    async fn set_activation_reset_reverts_to_configured() {
        let i1 = FakeInstance::new("i1", false, true); // runtime off, config on
        let (_rec, cp) = plane(mem_store(), vec![i1.clone()]);

        let body = cp
            .on_set_activation(req("set-activation", json!({ "instance": "i1", "reset": true })))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(i1.resets.load(Ordering::SeqCst), 1);
        assert!(i1.is_active(), "reverted to configured enabled=true");
        assert_eq!(body["reset"], json!(true));
        assert_eq!(body["active"], json!(true));
    }

    #[tokio::test]
    async fn set_activation_missing_active_and_reset_is_a_coded_error() {
        let i1 = FakeInstance::new("i1", true, true);
        let (_rec, cp) = plane(mem_store(), vec![i1]);
        let err = cp
            .on_set_activation(req("set-activation", json!({ "instance": "i1" })))
            .await
            .unwrap_err();
        assert_eq!(err.code, ERR_INVALID_REQUEST);
        assert!(err.message.contains("active"));
    }

    #[tokio::test]
    async fn set_activation_missing_instance_is_a_coded_error() {
        let (_rec, cp) = plane(mem_store(), vec![FakeInstance::new("i1", true, true)]);
        let err = cp
            .on_set_activation(req("set-activation", json!({ "active": true })))
            .await
            .unwrap_err();
        assert_eq!(err.code, ERR_INSTANCE_REQUIRED, "set-activation has no \"all\" form");
    }

    #[tokio::test]
    async fn set_activation_unknown_instance_is_a_coded_error() {
        let (_rec, cp) = plane(mem_store(), vec![FakeInstance::new("i1", true, true)]);
        let err = cp
            .on_set_activation(req("set-activation", json!({ "instance": "nope", "active": true })))
            .await
            .unwrap_err();
        assert_eq!(err.code, ERR_UNKNOWN_INSTANCE);
    }

    #[tokio::test]
    async fn set_activation_handler_error_is_reported() {
        let (_rec, cp) = plane(mem_store(), vec![FakeInstance::failing("i1")]);

        // apply path error
        let err = cp
            .on_set_activation(req("set-activation", json!({ "instance": "i1", "active": true })))
            .await
            .unwrap_err();
        assert_eq!(err.code, ERR_ACTIVATION_FAILED);
        assert!(err.message.contains("apply failed"));

        // reset path error
        let (_rec, cp) = plane(mem_store(), vec![FakeInstance::failing("i1")]);
        let err = cp
            .on_set_activation(req("set-activation", json!({ "instance": "i1", "reset": true })))
            .await
            .unwrap_err();
        assert!(err.message.contains("reset failed"));
    }

    #[tokio::test]
    async fn set_activation_tolerates_greengrass_numeric_bools() {
        // A numeric `persist`/`active` (Greengrass delivers config/message numbers as doubles) must NOT
        // fail whole-body deserialization and silently drop the command (which used to reply a
        // misleading "requires active" while the instance kept replicating).
        let i1 = FakeInstance::new("i1", true, true);
        let (_rec, cp) = plane(mem_store(), vec![i1.clone()]);
        let body = cp
            .on_set_activation(req(
                "set-activation",
                json!({ "instance": "i1", "active": false, "persist": 1 }),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*i1.last_apply.lock().unwrap(), Some((false, true)), "numeric persist:1 → true");
        assert!(!i1.is_active(), "numeric active:false honored, not dropped");
        assert_eq!(body["active"], json!(false));
        assert_eq!(body["persisted"], json!(true));
    }

    #[tokio::test]
    async fn get_status_reports_configured_schedule_mode_and_omits_link() {
        let i1 = FakeInstance::with_schedule("i1", true, true, "window");
        let (_rec, cp) = plane(mem_store(), vec![i1]);
        let body = cp
            .on_get_status(req("get-status", json!({ "instance": "i1" })))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(body["schedule"]["mode"], json!("window"), "configured mode, not a literal");
        assert!(body.get("link").is_none(), "link omitted until the P4 circuit-breaker exists");
    }
}

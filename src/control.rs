//! # file-replicator — control-message dispatcher (DESIGN §16)
//!
//! **One-liner purpose**: The inbound half of the control plane — subscribe the `cmd/#` topic space
//! on the Unified Namespace, route each request (`get-config` · `get-status` · `trigger` ·
//! `set-activation`) to a handler, and answer via the request's `reply_to` (DESIGN §15/§16).
//!
//! ## Shape
//! [`ControlPlane`] wraps the ggcommons [`MessagingService`] (the request/reply seam), the resolved
//! [`Topics`] (from [`crate::uns`]), the config snapshot, the shared durable [`StateStore`] (the
//! source of per-instance statistics), the outbound [`Events`] emitter (for the control-triggered
//! `InstanceActivated`/`InstanceDeactivated`/`ScheduleTriggered` events), and the set of live
//! instances behind the [`InstanceControl`] seam. [`ControlPlane::start`] registers the subscription;
//! every request is dispatched through the pure [`ControlPlane::route`] router (from [`crate::uns`])
//! and answered with a `FileReplicatorReply` envelope.
//!
//! ## Instance seam
//! The dispatcher never touches the engine directly — it drives instances through [`InstanceControl`]
//! (id / activation / trigger / destination label). The real [`crate::instance::Instance`] implements
//! it; unit tests substitute a fake, so routing, reply construction, and status serialization are
//! tested with a fake messaging service and a real (temp) [`StateStore`].
//!
//! ## Graceful degradation (DESIGN §6)
//! Messaging may be **absent** on a platform (`gg.messaging()` is `Err`). The control layer then
//! degrades to a no-op: [`ControlPlane::start`] warns once and subscribes nothing, and every reply is
//! silently skipped. The P1/P2 replication engine is unaffected.
//!
//! ## get-config & secrets
//! `get-config` returns the **effective configuration document** (`config.raw`), reusing the core
//! `GetConfiguration` contract. Because `$secret` references are stored *unresolved* in the config
//! document (FR-CFG-5), returning it never leaks a credential value — only the reference name travels.
//! An optional legacy alias topic (`ggcommons/{thing}/config/get/{ComponentName}`, DESIGN §15.6) is
//! answered too when enabled via `component.global.topics.legacyConfigTopic`.

use std::sync::Arc;

use async_trait::async_trait;
use ggcommons::messaging::{message_handler, Message, MessageBuilder, MessagingService};
use ggcommons::prelude::Config;
use serde_json::{json, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::config::GlobalCfg;
use crate::domain::{ItemState, WorkItem};
use crate::events::{Event, Events};
use crate::instance::now_ms;
use crate::state::{Stats, StateStore};
use crate::uns::{legacy_config_topic, parse_cmd, Command, Scope, Topics};

/// `header.name` for every control reply envelope (DESIGN §16).
const REPLY_MESSAGE_NAME: &str = "FileReplicatorReply";
/// Envelope schema version for control replies (matches the event/state envelope).
const MESSAGE_VERSION: &str = "1.0";
/// Client-side queue depth for the command subscription.
const CMD_QUEUE: usize = 32;
/// Concurrency for the command subscription — control commands are short, independent reads.
const CMD_CONCURRENCY: usize = 4;
/// Upper bound on the per-array file/item listing in a `get-status` reply, so a 100k-file spool never
/// produces a multi-megabyte reply (the `count`/`bytes` totals are always exact regardless).
const MAX_LISTED: usize = 100;

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
    /// `get-status`/state so an instance an operator set to a window is not misrepresented as
    /// `immediate` (cron/window execution is P4; the *reported* mode reflects config today).
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
}

/// The inbound control-message dispatcher (DESIGN §16).
pub struct ControlPlane {
    msg: Option<Arc<dyn MessagingService>>,
    topics: Arc<Topics>,
    config: Arc<Config>,
    store: Arc<dyn StateStore>,
    instances: Vec<Arc<dyn InstanceControl>>,
    events: Events,
    legacy_enabled: bool,
    legacy_topic: String,
}

impl ControlPlane {
    /// Assemble the dispatcher from raw parts (the seam unit tests use).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        msg: Option<Arc<dyn MessagingService>>,
        topics: Arc<Topics>,
        config: Arc<Config>,
        store: Arc<dyn StateStore>,
        instances: Vec<Arc<dyn InstanceControl>>,
        events: Events,
        legacy_enabled: bool,
    ) -> Self {
        let legacy_topic = legacy_config_topic(&config);
        Self {
            msg,
            topics,
            config,
            store,
            instances,
            events,
            legacy_enabled,
            legacy_topic,
        }
    }

    /// Build the dispatcher from the component config + parsed `component.global` (the wiring path).
    /// Resolves the component-level topic prefix (`component.global.topics.prefix`), the legacy-alias
    /// flag, and an [`Events`] emitter sharing that prefix.
    pub fn build(
        msg: Option<Arc<dyn MessagingService>>,
        config: &Arc<Config>,
        global: &GlobalCfg,
        store: Arc<dyn StateStore>,
        instances: Vec<Arc<dyn InstanceControl>>,
    ) -> Self {
        let global_prefix = global.topics.as_ref().and_then(|t| t.prefix.as_deref());
        let legacy_enabled = global
            .topics
            .as_ref()
            .and_then(|t| t.legacy_config_topic)
            .unwrap_or(false);
        let topics = Arc::new(Topics::from_config(config, global_prefix, None));
        let events = Events::new(
            msg.clone(),
            topics.clone(),
            config.thing_name.clone(),
            config.parsed.tags.clone(),
        );
        Self::new(
            msg,
            topics,
            config.clone(),
            store,
            instances,
            events,
            legacy_enabled,
        )
    }

    /// Subscribe the `cmd/#` topic space (plus the legacy `GetConfiguration` alias when enabled) and
    /// dispatch each request. No-op (warn once) when messaging is unavailable (DESIGN §6).
    pub async fn start(self: Arc<Self>) -> anyhow::Result<()> {
        let Some(msg) = self.msg.clone() else {
            tracing::warn!("messaging unavailable; control plane disabled (no command subscription)");
            return Ok(());
        };

        let filter = self.topics.cmd_filter();
        {
            let me = self.clone();
            let handler = message_handler(move |topic, message| {
                let me = me.clone();
                async move { me.handle(topic, message).await }
            });
            msg.subscribe(&filter, handler, CMD_QUEUE, CMD_CONCURRENCY)
                .await
                .map_err(|e| anyhow::anyhow!("subscribe {filter}: {e}"))?;
        }

        if self.legacy_enabled {
            let me = self.clone();
            let legacy = self.legacy_topic.clone();
            let handler = message_handler(move |topic, message| {
                let me = me.clone();
                async move { me.handle(topic, message).await }
            });
            msg.subscribe(&legacy, handler, 4, 1)
                .await
                .map_err(|e| anyhow::anyhow!("subscribe {legacy}: {e}"))?;
            tracing::info!(topic = %legacy, "legacy GetConfiguration alias enabled");
        }

        tracing::info!(filter = %filter, instances = self.instances.len(), "control plane subscribed");
        Ok(())
    }

    /// Unsubscribe every control topic (call before shutdown so no broker subscription is orphaned).
    pub async fn stop(&self) {
        let Some(msg) = &self.msg else { return };
        let _ = msg.unsubscribe(&self.topics.cmd_filter()).await;
        if self.legacy_enabled {
            let _ = msg.unsubscribe(&self.legacy_topic).await;
        }
    }

    /// Route an inbound request `topic` to its [`Command`], honoring the legacy config-alias topic.
    fn route(&self, topic: &str) -> Option<Command> {
        if self.legacy_enabled && topic == self.legacy_topic {
            return Some(Command::GetConfig);
        }
        parse_cmd(self.topics.prefix(), topic)
    }

    /// Dispatch one received request. An unroutable topic is logged and ignored (no reply).
    async fn handle(&self, topic: String, req: Message) {
        match self.route(&topic) {
            Some(Command::GetConfig) => self.on_get_config(&req).await,
            Some(Command::GetStatus(scope)) => self.on_get_status(&req, scope).await,
            Some(Command::Trigger(scope)) => self.on_trigger(&req, scope).await,
            Some(Command::SetActivation(id)) => self.on_set_activation(&req, &id).await,
            None => tracing::warn!(topic = %topic, "unroutable control command; ignoring"),
        }
    }

    // ---- handlers -------------------------------------------------------------------------------

    /// `get-config` → the effective configuration document (unresolved `$secret` refs are safe).
    async fn on_get_config(&self, req: &Message) {
        self.reply(req, json!({ "ok": true, "config": self.config.raw })).await;
    }

    /// `get-status` → per-instance (scoped) or component-wide statistics (DESIGN §16).
    async fn on_get_status(&self, req: &Message, scope: Scope) {
        let now = now_ms();
        let body = match scope {
            Scope::All => self.component_status_json(now),
            Scope::Instance(id) => match self.find(&id) {
                Some(inst) => self.instance_status(inst.as_ref(), now),
                None => unknown_instance(&id),
            },
        };
        self.reply(req, body).await;
    }

    /// `trigger` → force a scan now for one instance or all (FR-CTL-3), emitting `ScheduleTriggered`.
    ///
    /// The scan is run as a **detached task** and the reply is sent immediately ("accepted + counts",
    /// DESIGN §16): a scan is a full deliver→verify→complete batch (potentially a multi-GB/slow-S3
    /// upload), so awaiting it here would time out the request/reply client AND hold a command
    /// subscription slot for the whole transfer, starving get-status / an emergency set-activation. An
    /// inactive instance is **skipped** (its `tick` is a no-op), not falsely reported as triggered.
    ///
    /// `ignoreWindow` (default `false`) is threaded into [`InstanceControl::trigger_scan`]: when `true`
    /// the forced tick bypasses the P4 schedule gate and replicates all ready work regardless of the
    /// instance's cron/window state; when `false` the trigger respects the gate (a cron/window instance
    /// between its open spans discovers/enqueues but replicates nothing this tick). `triggered` counts
    /// the instances whose scan was **dispatched**, not files sent (the async batch's outcome is not
    /// known at reply time).
    async fn on_trigger(&self, req: &Message, scope: Scope) {
        let now = now_ms();
        let ignore_window = lenient_bool(req.body.get("ignoreWindow")).unwrap_or(false);

        let body = match scope {
            Scope::All => {
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
                self.events
                    .component_event(Event::ScheduleTriggered { scope: "all".into() }, now)
                    .await;
                json!({
                    "ok": true, "scope": "all", "triggered": triggered.len(),
                    "instances": triggered, "skipped": skipped, "ignoreWindow": ignore_window
                })
            }
            Scope::Instance(id) => match self.find(&id) {
                Some(inst) => {
                    if !inst.is_active() {
                        // A deactivated instance's tick is a no-op — report the truth, emit nothing.
                        json!({
                            "ok": true, "scope": id, "triggered": 0, "skipped": "inactive",
                            "ignoreWindow": ignore_window
                        })
                    } else {
                        spawn_trigger(inst.clone(), now, ignore_window);
                        self.events
                            .instance_event(&id, Event::ScheduleTriggered { scope: id.clone() }, now)
                            .await;
                        json!({ "ok": true, "scope": id, "triggered": 1, "ignoreWindow": ignore_window })
                    }
                }
                None => unknown_instance(&id),
            },
        };
        self.reply(req, body).await;
    }

    /// `set-activation` → activate/deactivate/reset an instance (FR-CTL-4 / DESIGN §7.5), emitting
    /// `InstanceActivated` / `InstanceDeactivated`. The action runs even when there is no `reply_to`.
    async fn on_set_activation(&self, req: &Message, id: &str) {
        let now = now_ms();
        let Some(inst) = self.find(id) else {
            self.reply(req, unknown_instance(id)).await;
            return;
        };
        // Field-by-field lenient parse (NOT whole-body `serde_json::from_value`): a Greengrass-style
        // numeric double (`{"active": false, "persist": 1}`) must not fail deserialization and get
        // silently discarded — that would drop a real deactivate and reply a misleading "requires
        // active" while the instance keeps replicating (FR-CFG-3 lenient-number convention).
        let reset = lenient_bool(req.body.get("reset")).unwrap_or(false);
        let active = lenient_bool(req.body.get("active"));

        let body = if reset {
            match inst.reset_activation(now) {
                Ok(effective) => {
                    self.emit_activation(id, effective, now).await;
                    self.republish_after_transition(inst.as_ref(), now).await;
                    json!({
                        "ok": true, "instance": id, "active": effective,
                        "configuredEnabled": inst.configured_enabled(), "reset": true
                    })
                }
                Err(e) => error_body(id, &e.to_string()),
            }
        } else if let Some(active) = active {
            let persist = lenient_bool(req.body.get("persist")).unwrap_or(true);
            match inst.apply_activation(active, persist, "control", now) {
                Ok(effective) => {
                    self.emit_activation(id, effective, now).await;
                    self.republish_after_transition(inst.as_ref(), now).await;
                    json!({
                        "ok": true, "instance": id, "active": effective,
                        "configuredEnabled": inst.configured_enabled(), "persisted": persist
                    })
                }
                Err(e) => error_body(id, &e.to_string()),
            }
        } else {
            json!({
                "ok": false, "instance": id,
                "error": "set-activation requires `active` (bool) or `reset: true`"
            })
        };
        self.reply(req, body).await;
    }

    /// Emit the activation lifecycle event matching the new effective state.
    async fn emit_activation(&self, id: &str, active: bool, now: i64) {
        let ev = if active {
            Event::InstanceActivated { source: "control".into() }
        } else {
            Event::InstanceDeactivated { source: "control".into() }
        };
        self.events.instance_event(id, ev, now).await;
    }

    /// Republish the retained current-state snapshots after an activation transition (DESIGN §17.2 /
    /// FR-EVT-4). A deactivated instance's `tick` early-returns and never republishes, so without this
    /// the last-published `state/instances/{id}` would stay `active:true` forever while `get-status`
    /// (which reads live state) correctly reports `active:false` — the two would permanently disagree.
    /// Also refreshes the component roster snapshot so its `active` count stays current.
    async fn republish_after_transition(&self, inst: &dyn InstanceControl, now: i64) {
        let snapshot = instance_state_snapshot(
            self.store.as_ref(),
            inst.id(),
            inst.is_active(),
            inst.configured_enabled(),
            inst.schedule_mode(),
            inst.dest_label(),
            now,
        );
        self.events.publish_instance_state(inst.id(), snapshot).await;
        self.events
            .publish_component_state(self.component_status_json(now))
            .await;
    }

    /// Publish the initial retained state snapshots at startup (DESIGN §15.3 / §17.2): one per
    /// instance plus the component roster, so a UI/cloud subscriber that connects to an idle component
    /// (nothing has ticked yet) still renders every instance's current state. Called once by
    /// [`crate::app`] after the control plane subscribes. No-op when messaging is absent.
    pub async fn publish_initial_state(&self, now: i64) {
        for inst in &self.instances {
            let snapshot = instance_state_snapshot(
                self.store.as_ref(),
                inst.id(),
                inst.is_active(),
                inst.configured_enabled(),
                inst.schedule_mode(),
                inst.dest_label(),
                now,
            );
            self.events.publish_instance_state(inst.id(), snapshot).await;
        }
        self.events
            .publish_component_state(self.component_status_json(now))
            .await;
    }

    // ---- status assembly ------------------------------------------------------------------------

    /// Component-wide status: every instance's snapshot plus a summary.
    fn component_status_json(&self, now: i64) -> Value {
        let instances: Vec<Value> = self
            .instances
            .iter()
            .map(|i| self.instance_status(i.as_ref(), now))
            .collect();
        let active = self.instances.iter().filter(|i| i.is_active()).count();
        json!({
            "component": self.config.component_name,
            "thing": self.config.thing_name,
            "instances": instances,
            "summary": { "instances": self.instances.len(), "active": active },
        })
    }

    /// One instance's status, sourced from the durable store (awaiting / in-progress / failed rows +
    /// running stats) and its live activation/destination state. Delegates to the shared
    /// [`instance_state_snapshot`] so `get-status` and the engine's retained `state/…` republish
    /// render identically.
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

    // ---- reply ----------------------------------------------------------------------------------

    /// Publish a reply to the request's `reply_to` (correlation is set by the messaging service).
    /// No-op when messaging is absent or the request carried no `reply_to`.
    async fn reply(&self, req: &Message, body: Value) {
        let Some(msg) = &self.msg else { return };
        if req.header.reply_to.is_none() {
            tracing::debug!(name = %req.header.name, "control request has no reply_to; not replying");
            return;
        }
        let reply = MessageBuilder::new(REPLY_MESSAGE_NAME, MESSAGE_VERSION)
            .from_config(&self.config)
            .payload(body)
            .build();
        if let Err(e) = msg.reply(req, reply).await {
            tracing::debug!(error = %e, "control reply publish failed (ignored)");
        }
    }

    /// Look up a live instance handle by id.
    fn find(&self, id: &str) -> Option<&Arc<dyn InstanceControl>> {
        self.instances.iter().find(|i| i.id() == id)
    }
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

/// Build the compact per-instance state/status document from the durable store (DESIGN §16 /
/// §17.2). Shared by the control plane's `get-status` reply and the engine's retained
/// `state/instances/{id}` republish (see [`crate::instance::Instance`]), so a request/reply status
/// and a retained snapshot render identically. Side-effect-free aside from the store reads (each
/// falling back to empty/default on a store fault, so status is best-effort and never panics).
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

/// A reply body for a command that named an instance id the component does not run.
fn unknown_instance(id: &str) -> Value {
    json!({ "ok": false, "instance": id, "error": "unknown instance" })
}

/// A reply body for a handler that failed while acting on `id`.
fn error_body(id: &str, err: &str) -> Value {
    json!({ "ok": false, "instance": id, "error": err })
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
    use ggcommons::messaging::{MessageHandler, Qos, ReplyFuture};
    use ggcommons::{GgError, Result};
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    use crate::state::SqliteStore;

    /// Poll `pred` (yielding to the runtime between checks) until it holds or a short deadline elapses.
    /// `trigger` now dispatches the scan as a detached task (so the reply never blocks on a transfer),
    /// so a trigger's side effect (the `FakeInstance` counter) is observed just after the handler
    /// returns, not synchronously.
    async fn eventually(pred: impl Fn() -> bool) {
        for _ in 0..200 {
            if pred() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("condition not met within the deadline");
    }

    // ---- fake messaging service (records publishes + replies + subscriptions) --------------------

    struct FakeMsg {
        published: Mutex<Vec<(String, Message)>>,
        replies: Mutex<Vec<(String, Message)>>,
        subscribed: Mutex<Vec<String>>,
        /// The `(filter, handler)` pairs registered via `subscribe` — kept (not discarded) so a test
        /// can drive the real subscribe→dispatch→reply wiring without a broker (see [`Self::deliver`]).
        handlers: Mutex<Vec<(String, Arc<dyn MessageHandler>)>>,
        unsubscribed: Mutex<Vec<String>>,
        fail_reply: AtomicBool,
    }

    impl FakeMsg {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                published: Mutex::new(Vec::new()),
                replies: Mutex::new(Vec::new()),
                subscribed: Mutex::new(Vec::new()),
                handlers: Mutex::new(Vec::new()),
                unsubscribed: Mutex::new(Vec::new()),
                fail_reply: AtomicBool::new(false),
            })
        }
        fn replies(&self) -> Vec<(String, Message)> {
            self.replies.lock().unwrap().clone()
        }
        fn published_topics(&self) -> Vec<String> {
            self.published.lock().unwrap().iter().map(|(t, _)| t.clone()).collect()
        }
        fn published_messages(&self) -> Vec<(String, Message)> {
            self.published.lock().unwrap().clone()
        }
        fn only_reply(&self) -> (String, Message) {
            let r = self.replies();
            assert_eq!(r.len(), 1, "expected exactly one reply, got {}", r.len());
            r.into_iter().next().unwrap()
        }
        /// Deliver `msg` on `topic` to every subscription whose filter matches — exercising the actual
        /// handler closures registered in [`ControlPlane::start`] (the dispatch→route→reply path that
        /// the direct `cp.handle(...)` tests bypass), so command delivery is covered without a broker.
        async fn deliver(&self, topic: &str, msg: Message) {
            let handlers = self.handlers.lock().unwrap().clone();
            for (filter, h) in handlers {
                if filter_matches(&filter, topic) {
                    h.handle(topic.to_string(), msg.clone()).await;
                }
            }
        }
    }

    /// MQTT-style filter match sufficient for the tests: a trailing `/#` matches the parent and any
    /// descendant; otherwise an exact match.
    fn filter_matches(filter: &str, topic: &str) -> bool {
        match filter.strip_suffix("/#") {
            Some(parent) => topic == parent || topic.starts_with(&format!("{parent}/")),
            None => filter == topic,
        }
    }

    #[async_trait]
    impl MessagingService for FakeMsg {
        async fn publish(&self, topic: &str, msg: &Message) -> Result<()> {
            self.published.lock().unwrap().push((topic.to_string(), msg.clone()));
            Ok(())
        }
        async fn publish_to_iot_core(&self, topic: &str, msg: &Message, _q: Qos) -> Result<()> {
            self.publish(topic, msg).await
        }
        async fn publish_raw(&self, topic: &str, payload: &Value) -> Result<()> {
            self.publish(topic, &Message::raw(payload.clone())).await
        }
        async fn publish_to_iot_core_raw(&self, topic: &str, p: &Value, _q: Qos) -> Result<()> {
            self.publish_raw(topic, p).await
        }
        async fn subscribe(
            &self,
            filter: &str,
            h: Arc<dyn MessageHandler>,
            _m: usize,
            _c: usize,
        ) -> Result<()> {
            self.subscribed.lock().unwrap().push(filter.to_string());
            self.handlers.lock().unwrap().push((filter.to_string(), h));
            Ok(())
        }
        async fn subscribe_to_iot_core(
            &self,
            filter: &str,
            h: Arc<dyn MessageHandler>,
            _q: Qos,
            m: usize,
            c: usize,
        ) -> Result<()> {
            self.subscribe(filter, h, m, c).await
        }
        async fn unsubscribe(&self, filter: &str) -> Result<()> {
            self.unsubscribed.lock().unwrap().push(filter.to_string());
            Ok(())
        }
        async fn unsubscribe_from_iot_core(&self, filter: &str) -> Result<()> {
            self.unsubscribe(filter).await
        }
        async fn request(&self, _t: &str, _m: Message) -> Result<ReplyFuture> {
            unimplemented!("request is not used by the control-plane tests")
        }
        async fn request_from_iot_core(&self, _t: &str, _m: Message) -> Result<ReplyFuture> {
            unimplemented!("request is not used by the control-plane tests")
        }
        async fn reply(&self, req: &Message, reply: Message) -> Result<()> {
            if self.fail_reply.load(Ordering::SeqCst) {
                return Err(GgError::Messaging("fake reply failure".into()));
            }
            // Mirror DefaultMessagingService: correlate the reply and route it to `reply_to`.
            let mut reply = reply;
            reply.header.correlation_id = req.header.correlation_id.clone();
            let to = req.header.reply_to.clone().unwrap_or_default();
            self.replies.lock().unwrap().push((to, reply));
            Ok(())
        }
        async fn reply_to_iot_core(&self, req: &Message, reply: Message) -> Result<()> {
            self.reply(req, reply).await
        }
        fn cancel_request(&self, _rf: ReplyFuture) {}
        fn cancel_request_from_iot_core(&self, _rf: ReplyFuture) {}
        fn connected(&self) -> bool {
            true
        }
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
            })
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

    fn events(fake: Arc<FakeMsg>, topics: Arc<Topics>) -> Events {
        let mut tags = BTreeMap::new();
        tags.insert("site".to_string(), json!("factory-1"));
        Events::new(Some(fake as Arc<dyn MessagingService>), topics, "gw-01", tags)
    }

    /// A dispatcher over a real in-memory store + the given instances, wired to `fake`.
    fn plane(
        fake: Arc<FakeMsg>,
        store: Arc<dyn StateStore>,
        instances: Vec<Arc<dyn InstanceControl>>,
        legacy: bool,
    ) -> ControlPlane {
        let topics = Arc::new(Topics::from_prefix("gw-01/file-replicator"));
        let ev = events(fake.clone(), topics.clone());
        ControlPlane::new(
            Some(fake as Arc<dyn MessagingService>),
            topics,
            cfg(),
            store,
            instances,
            ev,
            legacy,
        )
    }

    fn mem_store() -> Arc<dyn StateStore> {
        Arc::new(SqliteStore::open_in_memory().unwrap())
    }

    fn req(name: &str, body: Value) -> Message {
        MessageBuilder::new(name, "1.0")
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

    // ---- get-config -----------------------------------------------------------------------------

    #[tokio::test]
    async fn get_config_returns_effective_document_with_correlation() {
        let fake = FakeMsg::new();
        let cp = plane(fake.clone(), mem_store(), vec![], false);
        cp.handle("gw-01/file-replicator/cmd/config".into(), req("GetConfig", json!({})))
            .await;

        let (to, reply) = fake.only_reply();
        assert_eq!(to, "reply/here");
        assert_eq!(reply.header.correlation_id, "corr-1", "correlation preserved");
        assert_eq!(reply.header.name, REPLY_MESSAGE_NAME);
        assert_eq!(reply.tags.thing_name, "gw-01");
        assert_eq!(reply.body["ok"], json!(true));
        assert_eq!(reply.body["config"], cp.config.raw);
    }

    #[tokio::test]
    async fn legacy_alias_routes_to_get_config_only_when_enabled() {
        let topic = "ggcommons/gw-01/config/get/FileReplicator";

        // Enabled → GetConfig reply.
        let fake = FakeMsg::new();
        let cp = plane(fake.clone(), mem_store(), vec![], true);
        cp.handle(topic.into(), req("GetConfig", json!({}))).await;
        assert_eq!(fake.replies().len(), 1);
        assert_eq!(fake.only_reply().1.body["config"], cp.config.raw);

        // Disabled → unroutable, no reply.
        let fake = FakeMsg::new();
        let cp = plane(fake.clone(), mem_store(), vec![], false);
        cp.handle(topic.into(), req("GetConfig", json!({}))).await;
        assert!(fake.replies().is_empty());
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
        let fake = FakeMsg::new();
        let inst = FakeInstance::new("i1", true, true);
        let cp = plane(fake.clone(), store, vec![inst], false);

        cp.handle(
            "gw-01/file-replicator/cmd/instances/i1/status".into(),
            req("GetStatus", json!({})),
        )
        .await;

        let body = fake.only_reply().1.body;
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
        let fake = FakeMsg::new();
        let instances: Vec<Arc<dyn InstanceControl>> = vec![
            FakeInstance::new("i1", true, true),
            FakeInstance::new("i2", false, true),
        ];
        let cp = plane(fake.clone(), store, instances, false);

        cp.handle("gw-01/file-replicator/cmd/status".into(), req("GetStatus", json!({})))
            .await;

        let body = fake.only_reply().1.body;
        assert_eq!(body["component"], json!("com.mbreissi.edgecommons.FileReplicator"));
        assert_eq!(body["thing"], json!("gw-01"));
        assert_eq!(body["instances"].as_array().unwrap().len(), 2);
        assert_eq!(body["summary"]["instances"], json!(2));
        assert_eq!(body["summary"]["active"], json!(1));
    }

    #[tokio::test]
    async fn get_status_unknown_instance_is_error() {
        let fake = FakeMsg::new();
        let cp = plane(fake.clone(), mem_store(), vec![FakeInstance::new("i1", true, true)], false);
        cp.handle(
            "gw-01/file-replicator/cmd/instances/nope/status".into(),
            req("GetStatus", json!({})),
        )
        .await;
        let body = fake.only_reply().1.body;
        assert_eq!(body["ok"], json!(false));
        assert_eq!(body["error"], json!("unknown instance"));
    }

    // ---- trigger --------------------------------------------------------------------------------

    #[tokio::test]
    async fn trigger_all_scans_every_instance_and_emits_component_event() {
        let fake = FakeMsg::new();
        let i1 = FakeInstance::new("i1", true, true);
        let i2 = FakeInstance::new("i2", true, true);
        let cp = plane(
            fake.clone(),
            mem_store(),
            vec![i1.clone(), i2.clone()],
            false,
        );

        cp.handle(
            "gw-01/file-replicator/cmd/trigger".into(),
            req("Trigger", json!({ "ignoreWindow": true })),
        )
        .await;

        // The reply is sent immediately ("accepted + counts"); the scans run as detached tasks.
        let body = fake.only_reply().1.body;
        assert_eq!(body["ok"], json!(true));
        assert_eq!(body["scope"], json!("all"));
        assert_eq!(body["triggered"], json!(2));
        assert_eq!(body["skipped"], json!([]));
        assert_eq!(body["ignoreWindow"], json!(true));
        assert!(fake
            .published_topics()
            .contains(&"gw-01/file-replicator/evt/ScheduleTriggered".to_string()));
        eventually(|| {
            i1.triggers.load(Ordering::SeqCst) == 1 && i2.triggers.load(Ordering::SeqCst) == 1
        })
        .await;
        // FR-CTL-3: `ignoreWindow: true` from the request is threaded down to each instance's scan.
        assert!(i1.last_ignore_window.load(Ordering::SeqCst));
        assert!(i2.last_ignore_window.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn trigger_one_scans_only_that_instance_and_emits_instance_event() {
        let fake = FakeMsg::new();
        let i1 = FakeInstance::new("i1", true, true);
        let i2 = FakeInstance::new("i2", true, true);
        let cp = plane(fake.clone(), mem_store(), vec![i1.clone(), i2.clone()], false);

        cp.handle(
            "gw-01/file-replicator/cmd/instances/i1/trigger".into(),
            req("Trigger", json!({})),
        )
        .await;

        let body = fake.only_reply().1.body;
        assert_eq!(body["scope"], json!("i1"));
        assert_eq!(body["triggered"], json!(1));
        assert!(fake
            .published_topics()
            .contains(&"gw-01/file-replicator/evt/instances/i1/ScheduleTriggered".to_string()));
        eventually(|| i1.triggers.load(Ordering::SeqCst) == 1).await;
        assert_eq!(i2.triggers.load(Ordering::SeqCst), 0, "only i1 triggered");
        // No `ignoreWindow` in the request → the scan respects the schedule gate (default false).
        assert!(!i1.last_ignore_window.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn trigger_unknown_instance_is_error_and_scans_nothing() {
        let fake = FakeMsg::new();
        let i1 = FakeInstance::new("i1", true, true);
        let cp = plane(fake.clone(), mem_store(), vec![i1.clone()], false);
        cp.handle(
            "gw-01/file-replicator/cmd/instances/nope/trigger".into(),
            req("Trigger", json!({})),
        )
        .await;
        assert_eq!(i1.triggers.load(Ordering::SeqCst), 0);
        assert_eq!(fake.only_reply().1.body["ok"], json!(false));
    }

    // ---- set-activation -------------------------------------------------------------------------

    #[tokio::test]
    async fn set_activation_deactivate_persists_by_default_and_emits_event() {
        let fake = FakeMsg::new();
        let i1 = FakeInstance::new("i1", true, true);
        let cp = plane(fake.clone(), mem_store(), vec![i1.clone()], false);

        cp.handle(
            "gw-01/file-replicator/cmd/instances/i1/activation".into(),
            req("SetActivation", json!({ "active": false })),
        )
        .await;

        assert_eq!(*i1.last_apply.lock().unwrap(), Some((false, true)), "persist defaults true");
        assert!(!i1.is_active());
        let body = fake.only_reply().1.body;
        assert_eq!(body["ok"], json!(true));
        assert_eq!(body["active"], json!(false));
        assert_eq!(body["persisted"], json!(true));
        assert_eq!(body["configuredEnabled"], json!(true));
        assert!(fake
            .published_topics()
            .contains(&"gw-01/file-replicator/evt/instances/i1/InstanceDeactivated".to_string()));
    }

    #[tokio::test]
    async fn set_activation_activate_non_persistent_flips_runtime_only() {
        let fake = FakeMsg::new();
        let i1 = FakeInstance::new("i1", false, false);
        let cp = plane(fake.clone(), mem_store(), vec![i1.clone()], false);

        cp.handle(
            "gw-01/file-replicator/cmd/instances/i1/activation".into(),
            req("SetActivation", json!({ "active": true, "persist": false })),
        )
        .await;

        assert_eq!(*i1.last_apply.lock().unwrap(), Some((true, false)));
        assert!(i1.is_active());
        let body = fake.only_reply().1.body;
        assert_eq!(body["persisted"], json!(false));
        assert!(fake
            .published_topics()
            .contains(&"gw-01/file-replicator/evt/instances/i1/InstanceActivated".to_string()));
    }

    #[tokio::test]
    async fn set_activation_reset_reverts_to_configured() {
        let fake = FakeMsg::new();
        let i1 = FakeInstance::new("i1", false, true); // runtime off, config on
        let cp = plane(fake.clone(), mem_store(), vec![i1.clone()], false);

        cp.handle(
            "gw-01/file-replicator/cmd/instances/i1/activation".into(),
            req("SetActivation", json!({ "reset": true })),
        )
        .await;

        assert_eq!(i1.resets.load(Ordering::SeqCst), 1);
        assert!(i1.is_active(), "reverted to configured enabled=true");
        let body = fake.only_reply().1.body;
        assert_eq!(body["reset"], json!(true));
        assert_eq!(body["active"], json!(true));
    }

    #[tokio::test]
    async fn set_activation_missing_active_and_reset_is_error() {
        let fake = FakeMsg::new();
        let i1 = FakeInstance::new("i1", true, true);
        let cp = plane(fake.clone(), mem_store(), vec![i1], false);
        cp.handle(
            "gw-01/file-replicator/cmd/instances/i1/activation".into(),
            req("SetActivation", json!({})),
        )
        .await;
        let body = fake.only_reply().1.body;
        assert_eq!(body["ok"], json!(false));
        assert!(body["error"].as_str().unwrap().contains("active"));
    }

    #[tokio::test]
    async fn set_activation_unknown_instance_is_error() {
        let fake = FakeMsg::new();
        let cp = plane(fake.clone(), mem_store(), vec![FakeInstance::new("i1", true, true)], false);
        cp.handle(
            "gw-01/file-replicator/cmd/instances/nope/activation".into(),
            req("SetActivation", json!({ "active": true })),
        )
        .await;
        assert_eq!(fake.only_reply().1.body["error"], json!("unknown instance"));
    }

    #[tokio::test]
    async fn set_activation_handler_error_is_reported() {
        let fake = FakeMsg::new();
        let cp = plane(fake.clone(), mem_store(), vec![FakeInstance::failing("i1")], false);

        // apply path error
        cp.handle(
            "gw-01/file-replicator/cmd/instances/i1/activation".into(),
            req("SetActivation", json!({ "active": true })),
        )
        .await;
        assert_eq!(fake.only_reply().1.body["error"], json!("apply failed"));

        // reset path error
        let fake = FakeMsg::new();
        let cp = plane(fake.clone(), mem_store(), vec![FakeInstance::failing("i1")], false);
        cp.handle(
            "gw-01/file-replicator/cmd/instances/i1/activation".into(),
            req("SetActivation", json!({ "reset": true })),
        )
        .await;
        assert_eq!(fake.only_reply().1.body["error"], json!("reset failed"));
    }

    // ---- routing / reply edge cases -------------------------------------------------------------

    #[tokio::test]
    async fn unroutable_topic_is_ignored_without_reply() {
        let fake = FakeMsg::new();
        let cp = plane(fake.clone(), mem_store(), vec![], false);
        cp.handle("gw-01/file-replicator/cmd/frobnicate".into(), req("X", json!({})))
            .await;
        cp.handle("other/thing/cmd/config".into(), req("X", json!({}))).await;
        assert!(fake.replies().is_empty());
    }

    #[tokio::test]
    async fn action_runs_without_reply_when_request_has_no_reply_to() {
        let fake = FakeMsg::new();
        let i1 = FakeInstance::new("i1", true, true);
        let cp = plane(fake.clone(), mem_store(), vec![i1.clone()], false);
        // No reply_to on this request.
        let request = MessageBuilder::new("Trigger", "1.0").payload(json!({})).build();
        cp.handle("gw-01/file-replicator/cmd/trigger".into(), request).await;
        eventually(|| i1.triggers.load(Ordering::SeqCst) == 1).await; // action still ran
        assert!(fake.replies().is_empty(), "no reply without reply_to");
    }

    #[tokio::test]
    async fn reply_publish_failure_is_swallowed() {
        let fake = FakeMsg::new();
        fake.fail_reply.store(true, Ordering::SeqCst);
        let cp = plane(fake.clone(), mem_store(), vec![], false);
        // Must not panic or propagate even though the transport returns Err.
        cp.handle("gw-01/file-replicator/cmd/config".into(), req("GetConfig", json!({})))
            .await;
        assert!(fake.replies().is_empty());
    }

    // ---- start / stop / degraded ----------------------------------------------------------------

    #[tokio::test]
    async fn start_subscribes_command_space_and_legacy_when_enabled() {
        let fake = FakeMsg::new();
        let cp = Arc::new(plane(fake.clone(), mem_store(), vec![], true));
        cp.clone().start().await.unwrap();
        let subs = fake.subscribed.lock().unwrap().clone();
        assert!(subs.contains(&"gw-01/file-replicator/cmd/#".to_string()));
        assert!(subs.contains(&"ggcommons/gw-01/config/get/FileReplicator".to_string()));

        cp.stop().await;
        let uns = fake.unsubscribed.lock().unwrap().clone();
        assert!(uns.contains(&"gw-01/file-replicator/cmd/#".to_string()));
        assert!(uns.contains(&"ggcommons/gw-01/config/get/FileReplicator".to_string()));
    }

    #[tokio::test]
    async fn start_without_legacy_subscribes_only_command_space() {
        let fake = FakeMsg::new();
        let cp = Arc::new(plane(fake.clone(), mem_store(), vec![], false));
        cp.clone().start().await.unwrap();
        assert_eq!(fake.subscribed.lock().unwrap().clone(), vec!["gw-01/file-replicator/cmd/#"]);
    }

    #[tokio::test]
    async fn messaging_absent_is_a_graceful_no_op() {
        // No messaging: start subscribes nothing, actions still run, replies are skipped.
        let topics = Arc::new(Topics::from_prefix("gw-01/file-replicator"));
        let i1 = FakeInstance::new("i1", true, true);
        let cp = Arc::new(ControlPlane::new(
            None,
            topics.clone(),
            cfg(),
            mem_store(),
            vec![i1.clone()],
            Events::disabled(),
            true,
        ));
        cp.clone().start().await.unwrap(); // no-op
        cp.stop().await; // no-op
        cp.handle("gw-01/file-replicator/cmd/instances/i1/trigger".into(), req("Trigger", json!({})))
            .await;
        eventually(|| i1.triggers.load(Ordering::SeqCst) == 1).await; // action ran even without messaging
    }

    #[tokio::test]
    async fn build_resolves_prefix_events_and_legacy_from_config() {
        let fake = FakeMsg::new();
        let config = cfg();
        let global = GlobalCfg {
            topics: Some(crate::config::TopicsCfg {
                prefix: Some("plant/{ThingName}/file-replicator".to_string()),
                legacy_config_topic: Some(true),
            }),
            ..Default::default()
        };
        let cp = ControlPlane::build(
            Some(fake.clone() as Arc<dyn MessagingService>),
            &config,
            &global,
            mem_store(),
            vec![],
        );
        assert_eq!(cp.topics.prefix(), "plant/gw-01/file-replicator");
        assert!(cp.legacy_enabled);
        // Config command under the resolved prefix routes.
        assert_eq!(
            cp.route("plant/gw-01/file-replicator/cmd/config"),
            Some(Command::GetConfig)
        );
    }

    // ---- fake trait coverage --------------------------------------------------------------------

    #[tokio::test]
    async fn fake_messaging_secondary_methods_are_exercised() {
        // Cover the fake's non-core surface so the control module's test-support does not drag its
        // own coverage down (mirrors the event module's approach).
        let fake = FakeMsg::new();
        let m = MessageBuilder::new("T", "1.0").reply_to("r").build();
        fake.publish_to_iot_core("t/iot", &m, Qos::AtLeastOnce).await.unwrap();
        fake.publish_raw("t/raw", &json!({ "x": 1 })).await.unwrap();
        fake.publish_to_iot_core_raw("t/rawiot", &json!({ "x": 2 }), Qos::AtMostOnce).await.unwrap();
        let h = message_handler(|_t, _m| async {});
        fake.subscribe_to_iot_core("f/#", h, Qos::AtLeastOnce, 8, 1).await.unwrap();
        fake.unsubscribe_from_iot_core("f/#").await.unwrap();
        fake.reply_to_iot_core(&m, MessageBuilder::new("R", "1.0").build()).await.unwrap();
        assert!(fake.connected());
        assert_eq!(fake.published.lock().unwrap().len(), 3);
        assert_eq!(fake.replies().len(), 1);
    }

    // ---- P3 review-finding regressions ----------------------------------------------------------

    #[tokio::test]
    async fn subscribed_handler_dispatches_command_and_replies() {
        // The subscribe→dispatch→reply wiring (the closure built in `start`) was previously only
        // proven by the self-skipping EMQX suite. Drive it through the recorded handler, no broker.
        let fake = FakeMsg::new();
        let cp = Arc::new(plane(fake.clone(), mem_store(), vec![], false));
        cp.clone().start().await.unwrap();
        fake.deliver("gw-01/file-replicator/cmd/config", req("GetConfig", json!({})))
            .await;
        let (to, reply) = fake.only_reply();
        assert_eq!(to, "reply/here");
        assert_eq!(reply.header.name, REPLY_MESSAGE_NAME);
        assert_eq!(reply.body["ok"], json!(true));
        assert_eq!(reply.body["config"], cp.config.raw);
    }

    #[tokio::test]
    async fn trigger_deactivated_instance_reports_skipped_and_scans_nothing() {
        // Triggering a deactivated instance must not falsely ack `triggered:1` — its tick is a no-op.
        let fake = FakeMsg::new();
        let i1 = FakeInstance::new("i1", false, true); // deactivated
        let cp = plane(fake.clone(), mem_store(), vec![i1.clone()], false);
        cp.handle(
            "gw-01/file-replicator/cmd/instances/i1/trigger".into(),
            req("Trigger", json!({})),
        )
        .await;
        let body = fake.only_reply().1.body;
        assert_eq!(body["ok"], json!(true));
        assert_eq!(body["triggered"], json!(0));
        assert_eq!(body["skipped"], json!("inactive"));
        assert_eq!(i1.triggers.load(Ordering::SeqCst), 0, "no scan forced on a deactivated instance");
        assert!(
            !fake.published_topics().iter().any(|t| t.ends_with("/ScheduleTriggered")),
            "no ScheduleTriggered emitted for a skipped instance"
        );
    }

    #[tokio::test]
    async fn trigger_all_counts_only_active_instances() {
        let fake = FakeMsg::new();
        let on = FakeInstance::new("on", true, true);
        let off = FakeInstance::new("off", false, true);
        let cp = plane(fake.clone(), mem_store(), vec![on.clone(), off.clone()], false);
        cp.handle("gw-01/file-replicator/cmd/trigger".into(), req("Trigger", json!({})))
            .await;
        let body = fake.only_reply().1.body;
        assert_eq!(body["triggered"], json!(1));
        assert_eq!(body["instances"], json!(["on"]));
        assert_eq!(body["skipped"], json!(["off"]));
        eventually(|| on.triggers.load(Ordering::SeqCst) == 1).await;
        assert_eq!(off.triggers.load(Ordering::SeqCst), 0, "inactive instance never triggered");
    }

    #[tokio::test]
    async fn set_activation_tolerates_greengrass_numeric_bools() {
        // A numeric `persist`/`active` (Greengrass delivers config/message numbers as doubles) must NOT
        // fail whole-body deserialization and silently drop the command (which used to reply a
        // misleading "requires active" while the instance kept replicating).
        let fake = FakeMsg::new();
        let i1 = FakeInstance::new("i1", true, true);
        let cp = plane(fake.clone(), mem_store(), vec![i1.clone()], false);
        cp.handle(
            "gw-01/file-replicator/cmd/instances/i1/activation".into(),
            req("SetActivation", json!({ "active": false, "persist": 1 })),
        )
        .await;
        assert_eq!(*i1.last_apply.lock().unwrap(), Some((false, true)), "numeric persist:1 → true");
        assert!(!i1.is_active(), "numeric active:false honored, not dropped");
        let body = fake.only_reply().1.body;
        assert_eq!(body["ok"], json!(true));
        assert_eq!(body["active"], json!(false));
        assert_eq!(body["persisted"], json!(true));
    }

    #[tokio::test]
    async fn set_activation_republishes_retained_state_snapshot() {
        // After a deactivate the retained `state/instances/{id}` must be republished (a deactivated
        // tick never republishes), or the snapshot would stay `active:true` forever.
        let fake = FakeMsg::new();
        let i1 = FakeInstance::new("i1", true, true);
        let cp = plane(fake.clone(), mem_store(), vec![i1], false);
        cp.handle(
            "gw-01/file-replicator/cmd/instances/i1/activation".into(),
            req("SetActivation", json!({ "active": false })),
        )
        .await;
        let inst_state = fake
            .published_messages()
            .into_iter()
            .rfind(|(t, m)| {
                t == "gw-01/file-replicator/state/instances/i1"
                    && m.header.name == crate::events::STATE_MESSAGE_NAME
            })
            .expect("a per-instance state snapshot republished after deactivation");
        assert_eq!(inst_state.1.body["instance"], json!("i1"));
        assert_eq!(inst_state.1.body["active"], json!(false), "snapshot reflects the new state");
        assert!(
            fake.published_messages().iter().any(|(t, m)| t == "gw-01/file-replicator/state"
                && m.header.name == crate::events::STATE_MESSAGE_NAME),
            "component roster snapshot also refreshed"
        );
    }

    #[tokio::test]
    async fn get_status_reports_configured_schedule_mode_and_omits_link() {
        let fake = FakeMsg::new();
        let i1 = FakeInstance::with_schedule("i1", true, true, "window");
        let cp = plane(fake.clone(), mem_store(), vec![i1], false);
        cp.handle(
            "gw-01/file-replicator/cmd/instances/i1/status".into(),
            req("GetStatus", json!({})),
        )
        .await;
        let body = fake.only_reply().1.body;
        assert_eq!(body["schedule"]["mode"], json!("window"), "configured mode, not a literal");
        assert!(body.get("link").is_none(), "link omitted until the P4 circuit-breaker exists");
    }

    #[tokio::test]
    async fn publish_initial_state_snapshots_every_instance_and_the_component() {
        // An idle component (nothing ticked yet) must still publish every instance's current state at
        // startup so a fresh subscriber renders it.
        let fake = FakeMsg::new();
        let instances: Vec<Arc<dyn InstanceControl>> = vec![
            FakeInstance::new("i1", true, true),
            FakeInstance::with_schedule("i2", false, true, "window"),
        ];
        let cp = plane(fake.clone(), mem_store(), instances, false);
        cp.publish_initial_state(now_ms()).await;
        let states: Vec<(String, Message)> = fake
            .published_messages()
            .into_iter()
            .filter(|(_, m)| m.header.name == crate::events::STATE_MESSAGE_NAME)
            .collect();
        assert!(states.iter().any(|(t, _)| t == "gw-01/file-replicator/state/instances/i1"));
        let i2 = states
            .iter()
            .find(|(t, _)| t == "gw-01/file-replicator/state/instances/i2")
            .expect("i2 state snapshot");
        assert_eq!(i2.1.body["schedule"]["mode"], json!("window"));
        assert_eq!(i2.1.body["active"], json!(false));
        assert!(
            states.iter().any(|(t, _)| t == "gw-01/file-replicator/state"),
            "component roster snapshot published"
        );
    }
}

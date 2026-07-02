//! # file-replicator — event catalog, envelope, throttle, and state publisher
//!
//! **One-liner purpose**: The outbound half of the control plane — the [`Event`] catalog (DESIGN
//! §17.1), the `FileReplicatorEvent` envelope, the progress [`ProgressThrottle`], and the current-
//! state snapshot publisher — all over the ggcommons [`MessagingService`], degrading to a no-op when
//! messaging is unavailable.
//!
//! ## Envelope (DESIGN §17)
//! Every event is a standard ggcommons [`Message`] with `header.name = "FileReplicatorEvent"`,
//! `version = "1.0"`, the ThingName + config tags in `tags`, and a `body` object whose `event` key
//! discriminates. State snapshots use `header.name = "FileReplicatorState"` so a consumer can tell
//! the two apart on the wire.
//!
//! ## Throttling (DESIGN §17.2 / FR-EVT-2)
//! `ReplicationProgress` is gated by [`ProgressThrottle`]: emit on a ≥`progressPercentStep` (default
//! 10) jump OR every `progressIntervalSecs` (default 5s); `0%` (first observation) and `100%` are
//! always emitted.
//!
//! ## Retained state — the ggcommons gap (DESIGN §0 / §17.2, FR-EVT-4)
//! DESIGN §17.2 asks the `state/…` snapshots to be **retained** so a late/reconnecting subscriber
//! renders correctly on connect. **The current ggcommons `MessagingService` cannot publish a
//! retained message** — `publish`/`publish_to_iot_core` expose no retain flag and the MQTT provider
//! hardcodes the retain bit to `false`. So this module publishes state snapshots as **normal
//! (non-retained)** messages via the single private [`Events::publish_state`] seam. Consequences:
//! - Live subscribers see current state on every transition (works today).
//! - A subscriber that connects *after* the last snapshot will NOT receive it — that is the one thing
//!   retain buys. The reliable "current state on demand" path is the `cmd/status` request/reply
//!   (implemented in the control layer), which works regardless.
//! - We do NOT fake retention (no periodic republish spam).
//!
//! The minimal ggcommons enhancement that closes this gap (four-language parity): add a retain flag
//! to `MessagingProvider::publish` (MQTT passes it through; IPC ignores it) and surface
//! `publish_retained` on `MessagingService`. The same change unlocks a QoS-capable local publish
//! (progress at `AtMostOnce`, §17). When it lands, only [`Events::publish_state`] changes — one line.
//!
//! ## QoS deviation
//! §17 asks `AtMostOnce` for progress and `AtLeastOnce` for lifecycle. All UNS traffic goes to the
//! **local broker** via `publish`, whose QoS is fixed at `AtLeastOnce`; progress therefore rides at
//! `AtLeastOnce` too. Switching to `publish_to_iot_core` just to get `AtMostOnce` would change the
//! destination, so we keep the local publish and document this — it is closed by the same retain/QoS
//! enhancement above.

use std::collections::BTreeMap;
use std::sync::Arc;

use ggcommons::messaging::{Message, MessageBuilder, MessagingService};
use serde_json::{json, Map, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::uns::Topics;

/// Message-envelope `header.name` for the event stream (DESIGN §17).
pub const EVENT_MESSAGE_NAME: &str = "FileReplicatorEvent";
/// Message-envelope `header.name` for current-state snapshots (distinct from events on the wire).
pub const STATE_MESSAGE_NAME: &str = "FileReplicatorState";
/// Envelope schema version for both events and state (DESIGN §17).
pub const MESSAGE_VERSION: &str = "1.0";

/// Default `progressPercentStep` for [`ProgressThrottle`] (DESIGN §17.2 / FR-EVT-2).
pub const DEFAULT_PROGRESS_PERCENT_STEP: i32 = 10;
/// Default `progressIntervalSecs` (as milliseconds) for [`ProgressThrottle`].
pub const DEFAULT_PROGRESS_INTERVAL_MS: i64 = 5_000;

/// The event catalog (DESIGN §17.1). Each variant carries the fields that become its `body` (beyond
/// the emitter-added `event`, `instance`, and `ts`).
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
    /// The component finished initializing (`set_ready(true)`). Component-level (no `instance`).
    ComponentReady { instances: u64, version: String },
    /// A trigger command forced a scan/replication. `scope` = `"all"` or the instance id.
    ScheduleTriggered { scope: String },
    /// **Deferred (P4 scheduler)**: a replication window opened.
    WindowOpened { window: String },
    /// **Deferred (P4 scheduler)**: a replication window closed.
    WindowClosed { window: String },
    /// **Deferred (circuit-breaker, §13.4)**: the destination link went down.
    Disconnected { link: String },
    /// **Deferred (circuit-breaker, §13.4)**: the destination link recovered.
    Reconnected { link: String },
}

impl Event {
    /// The wire discriminator (`body.event`), identical to the variant name (DESIGN §17.1).
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
            Event::Disconnected { .. } => "Disconnected",
            Event::Reconnected { .. } => "Reconnected",
        }
    }

    /// The event-specific body fields (beyond `event`/`instance`/`ts`, which the emitter adds).
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
                let mut m = obj(json!({
                    "path": path, "attempts": attempts, "lastError": last_error
                }));
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
            Event::Disconnected { link } | Event::Reconnected { link } => {
                obj(json!({ "link": link }))
            }
        }
    }
}

/// Build the full event `body` object: `{ event, instance?, <fields…>, ts }` (DESIGN §17).
///
/// Pure — `now_ms` (Unix ms) is injected so tests are deterministic. `instance` is `None` for
/// component-level events (e.g. `ComponentReady`).
pub fn event_body(ev: &Event, instance: Option<&str>, now_ms: i64) -> Value {
    let mut map = Map::new();
    map.insert("event".into(), Value::String(ev.name().to_string()));
    if let Some(id) = instance {
        map.insert("instance".into(), Value::String(id.to_string()));
    }
    for (k, v) in ev.fields() {
        map.insert(k, v);
    }
    map.insert("ts".into(), Value::String(rfc3339(now_ms)));
    Value::Object(map)
}

/// Format a Unix-millis instant as an RFC3339 UTC string (the `ts` / `nextAttemptAt` wire format).
/// Falls back to the epoch on the (practically impossible) out-of-range/format failure.
fn rfc3339(now_ms: i64) -> String {
    OffsetDateTime::from_unix_timestamp_nanos((now_ms as i128) * 1_000_000)
        .ok()
        .and_then(|dt| dt.format(&Rfc3339).ok())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}

/// Progress-emission throttle (DESIGN §17.2 / FR-EVT-2). One per in-flight transfer; pure logic,
/// fully unit-tested.
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

/// The cloneable event/state emitter every instance + worker holds (and the component wiring for
/// component-level events). When `msg` is `None` (messaging unavailable on this platform) every
/// method is a no-op, so the P1/P2 replication engine runs unchanged (graceful degradation, DESIGN
/// §6).
#[derive(Clone)]
pub struct Events {
    msg: Option<Arc<dyn MessagingService>>,
    topics: Arc<Topics>,
    thing: String,
    tags: BTreeMap<String, Value>,
}

impl Events {
    /// Build an emitter for one prefix domain. `tags` are the config `tags` copied into every
    /// envelope (typically `config.parsed.tags`).
    pub fn new(
        msg: Option<Arc<dyn MessagingService>>,
        topics: Arc<Topics>,
        thing: impl Into<String>,
        tags: BTreeMap<String, Value>,
    ) -> Self {
        Self {
            msg,
            topics,
            thing: thing.into(),
            tags,
        }
    }

    /// A disabled (no-op) emitter — no messaging, empty prefix. Used by the engine's unit tests and
    /// wherever messaging is absent so the whole surface keeps compiling and running.
    pub fn disabled() -> Self {
        Self {
            msg: None,
            topics: Arc::new(Topics::from_prefix("disabled")),
            thing: String::new(),
            tags: BTreeMap::new(),
        }
    }

    /// The topic set this emitter publishes to.
    pub fn topics(&self) -> &Arc<Topics> {
        &self.topics
    }

    /// Whether messaging is available (events actually publish).
    pub fn is_enabled(&self) -> bool {
        self.msg.is_some()
    }

    /// Wrap a body in the standard ggcommons envelope with the given `header.name` and the
    /// ThingName + config tags. Pure (no I/O).
    fn envelope(&self, name: &str, body: Value) -> Message {
        let mut b = MessageBuilder::new(name, MESSAGE_VERSION)
            .thing_name(&self.thing)
            .payload(body);
        for (k, v) in &self.tags {
            b = b.tag(k.clone(), v.clone());
        }
        b.build()
    }

    /// Emit a per-instance event on `{prefix}/evt/instances/{id}/{event}` (no-op if messaging is
    /// absent; a publish error is swallowed at `debug!` so it never disturbs the pipeline).
    pub async fn instance_event(&self, id: &str, ev: Event, now_ms: i64) {
        let Some(msg) = &self.msg else { return };
        let topic = self.topics.evt_instance(id, ev.name());
        let m = self.envelope(EVENT_MESSAGE_NAME, event_body(&ev, Some(id), now_ms));
        if let Err(e) = msg.publish(&topic, &m).await {
            tracing::debug!(topic = %topic, error = %e, "event publish failed (ignored)");
        }
    }

    /// Emit a component-level event on `{prefix}/evt/{event}` (no `instance` in the body).
    pub async fn component_event(&self, ev: Event, now_ms: i64) {
        let Some(msg) = &self.msg else { return };
        let topic = self.topics.evt_component(ev.name());
        let m = self.envelope(EVENT_MESSAGE_NAME, event_body(&ev, None, now_ms));
        if let Err(e) = msg.publish(&topic, &m).await {
            tracing::debug!(topic = %topic, error = %e, "component event publish failed (ignored)");
        }
    }

    /// Publish a per-instance current-state snapshot to `{prefix}/state/instances/{id}` (see the
    /// retained-state gap in the module docs — currently non-retained).
    pub async fn publish_instance_state(&self, id: &str, snapshot: Value) {
        let topic = self.topics.state_instance(id);
        self.publish_state(topic, snapshot).await;
    }

    /// Publish the component current-state snapshot to `{prefix}/state`.
    pub async fn publish_component_state(&self, snapshot: Value) {
        let topic = self.topics.state_component();
        self.publish_state(topic, snapshot).await;
    }

    /// The single state-publish seam (module docs, retain gap). Today it calls `publish` (non-
    /// retained, `AtLeastOnce`); once ggcommons grows `publish_retained` this is the one line to
    /// change. No-op when messaging is absent; a publish error is swallowed at `debug!`.
    async fn publish_state(&self, topic: String, snapshot: Value) {
        let Some(msg) = &self.msg else { return };
        let m = self.envelope(STATE_MESSAGE_NAME, snapshot);
        if let Err(e) = msg.publish(&topic, &m).await {
            tracing::debug!(topic = %topic, error = %e, "state publish failed (ignored)");
        }
    }
}

/// Test-only recording [`MessagingService`] + [`Events`] factory, shared by the control-plane's
/// engine-wiring emission tests (`crate::instance::worker`, `crate::instance`) so each does not
/// reinvent a full fake. Compiled only under `#[cfg(test)]`.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use async_trait::async_trait;
    use ggcommons::messaging::{MessageHandler, Qos, ReplyFuture};
    use ggcommons::{GgError, Result};
    use std::sync::Mutex;

    /// A [`MessagingService`] that records every published `(topic, message)`. The engine event
    /// emitter only ever calls `publish`; the remaining trait methods are trivial no-ops.
    pub(crate) struct RecordingMessaging {
        published: Mutex<Vec<(String, Message)>>,
    }

    impl RecordingMessaging {
        pub(crate) fn new() -> Arc<Self> {
            Arc::new(Self {
                published: Mutex::new(Vec::new()),
            })
        }
        /// Every published topic, in order.
        pub(crate) fn topics(&self) -> Vec<String> {
            self.published.lock().unwrap().iter().map(|(t, _)| t.clone()).collect()
        }
        /// The published messages whose `body.event` discriminator equals `name` (the evt stream).
        pub(crate) fn events_named(&self, name: &str) -> Vec<Message> {
            self.published
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, m)| m.body.get("event").and_then(|v| v.as_str()) == Some(name))
                .map(|(_, m)| m.clone())
                .collect()
        }
        /// The published `FileReplicatorState` snapshots (retained current-state topic).
        pub(crate) fn state_snapshots(&self) -> Vec<(String, Message)> {
            self.published
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, m)| m.header.name == STATE_MESSAGE_NAME)
                .cloned()
                .collect()
        }
    }

    #[async_trait]
    impl MessagingService for RecordingMessaging {
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
        async fn subscribe(&self, _f: &str, _h: Arc<dyn MessageHandler>, _m: usize, _c: usize) -> Result<()> {
            Ok(())
        }
        async fn subscribe_to_iot_core(
            &self,
            _f: &str,
            _h: Arc<dyn MessageHandler>,
            _q: Qos,
            _m: usize,
            _c: usize,
        ) -> Result<()> {
            Ok(())
        }
        async fn unsubscribe(&self, _f: &str) -> Result<()> {
            Ok(())
        }
        async fn unsubscribe_from_iot_core(&self, _f: &str) -> Result<()> {
            Ok(())
        }
        async fn request(&self, _t: &str, _m: Message) -> Result<ReplyFuture> {
            Err(GgError::Messaging("request unused".into()))
        }
        async fn request_from_iot_core(&self, _t: &str, _m: Message) -> Result<ReplyFuture> {
            Err(GgError::Messaging("request unused".into()))
        }
        async fn reply(&self, _req: &Message, _reply: Message) -> Result<()> {
            Ok(())
        }
        async fn reply_to_iot_core(&self, _req: &Message, _reply: Message) -> Result<()> {
            Ok(())
        }
        fn cancel_request(&self, _rf: ReplyFuture) {}
        fn cancel_request_from_iot_core(&self, _rf: ReplyFuture) {}
        fn connected(&self) -> bool {
            true
        }
    }

    /// An [`Events`] emitter over a fresh [`RecordingMessaging`], rooted at the canonical test prefix
    /// `gw-01/file-replicator`. Returns the fake (for assertions) and the emitter (to inject).
    pub(crate) fn recording_events() -> (Arc<RecordingMessaging>, Events) {
        let fake = RecordingMessaging::new();
        let events = Events::new(
            Some(fake.clone() as Arc<dyn MessagingService>),
            Arc::new(Topics::from_prefix("gw-01/file-replicator")),
            "gw-01",
            BTreeMap::new(),
        );
        (fake, events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use ggcommons::messaging::{MessageHandler, Qos, ReplyFuture};
    use ggcommons::{GgError, Result};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    /// A fake [`MessagingService`] that records every published `(topic, message, retained)` tuple.
    /// `retained` is always `false` — the current API cannot publish retained (module docs). Set
    /// `fail` to exercise the swallowed-error arm.
    struct FakeMessaging {
        published: Mutex<Vec<(String, Message, bool)>>,
        fail: AtomicBool,
    }

    impl FakeMessaging {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                published: Mutex::new(Vec::new()),
                fail: AtomicBool::new(false),
            })
        }
        fn records(&self) -> Vec<(String, Message, bool)> {
            self.published.lock().unwrap().clone()
        }
        fn topics(&self) -> Vec<String> {
            self.records().into_iter().map(|(t, _, _)| t).collect()
        }
    }

    #[async_trait]
    impl MessagingService for FakeMessaging {
        async fn publish(&self, topic: &str, msg: &Message) -> Result<()> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(GgError::Messaging("fake publish failure".into()));
            }
            self.published
                .lock()
                .unwrap()
                .push((topic.to_string(), msg.clone(), false));
            Ok(())
        }
        async fn publish_to_iot_core(&self, topic: &str, msg: &Message, _q: Qos) -> Result<()> {
            self.publish(topic, msg).await
        }
        async fn publish_raw(&self, topic: &str, payload: &Value) -> Result<()> {
            self.publish(topic, &Message::raw(payload.clone())).await
        }
        async fn publish_to_iot_core_raw(
            &self,
            topic: &str,
            payload: &Value,
            _q: Qos,
        ) -> Result<()> {
            self.publish_raw(topic, payload).await
        }
        async fn subscribe(
            &self,
            _f: &str,
            _h: Arc<dyn MessageHandler>,
            _m: usize,
            _c: usize,
        ) -> Result<()> {
            Ok(())
        }
        async fn subscribe_to_iot_core(
            &self,
            _f: &str,
            _h: Arc<dyn MessageHandler>,
            _q: Qos,
            _m: usize,
            _c: usize,
        ) -> Result<()> {
            Ok(())
        }
        async fn unsubscribe(&self, _f: &str) -> Result<()> {
            Ok(())
        }
        async fn unsubscribe_from_iot_core(&self, _f: &str) -> Result<()> {
            Ok(())
        }
        async fn request(&self, _t: &str, _m: Message) -> Result<ReplyFuture> {
            unimplemented!("request is not used by the event-emitter unit tests")
        }
        async fn request_from_iot_core(&self, _t: &str, _m: Message) -> Result<ReplyFuture> {
            unimplemented!("request is not used by the event-emitter unit tests")
        }
        async fn reply(&self, _req: &Message, _reply: Message) -> Result<()> {
            Ok(())
        }
        async fn reply_to_iot_core(&self, _req: &Message, _reply: Message) -> Result<()> {
            Ok(())
        }
        fn cancel_request(&self, _rf: ReplyFuture) {}
        fn cancel_request_from_iot_core(&self, _rf: ReplyFuture) {}
        fn connected(&self) -> bool {
            true
        }
    }

    fn emitter(msg: Arc<FakeMessaging>) -> Events {
        let mut tags = BTreeMap::new();
        tags.insert("site".to_string(), json!("factory-1"));
        Events::new(
            Some(msg as Arc<dyn MessagingService>),
            Arc::new(Topics::from_prefix("gw-01/file-replicator")),
            "gw-01",
            tags,
        )
    }

    // ---- rfc3339 -------------------------------------------------------------------------------

    #[test]
    fn rfc3339_formats_epoch_and_a_known_instant() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
        // 2026-07-01T09:15:22Z == 1_782_897_322 s (whole seconds → no fractional part).
        assert_eq!(rfc3339(1_782_897_322_000), "2026-07-01T09:15:22Z");
    }

    // ---- event bodies --------------------------------------------------------------------------

    #[test]
    fn progress_body_matches_design_example() {
        let ev = Event::ReplicationProgress {
            path: "big.parquet".into(),
            size: 734_003_200,
            bytes_done: 220_200_960,
            percent: 30.0,
            destination: "s3".into(),
            attempt: 1,
        };
        let body = event_body(&ev, Some("plant-csv-to-s3"), 1_782_897_322_000);
        assert_eq!(
            body,
            json!({
                "event": "ReplicationProgress",
                "instance": "plant-csv-to-s3",
                "path": "big.parquet",
                "size": 734_003_200u64,
                "bytesDone": 220_200_960u64,
                "percent": 30.0,
                "destination": "s3",
                "attempt": 1,
                "ts": "2026-07-01T09:15:22Z"
            })
        );
    }

    #[test]
    fn failed_body_carries_will_retry_and_optional_next_attempt() {
        let with = Event::ReplicationFailed {
            path: "x.csv".into(),
            destination: "local".into(),
            attempt: 3,
            error: "boom".into(),
            next_attempt_at_ms: Some(0),
        };
        let b = event_body(&with, Some("i1"), 0);
        assert_eq!(b["willRetry"], json!(true));
        assert_eq!(b["nextAttemptAt"], json!("1970-01-01T00:00:00Z"));

        let without = Event::ReplicationFailed {
            path: "x.csv".into(),
            destination: "local".into(),
            attempt: 3,
            error: "boom".into(),
            next_attempt_at_ms: None,
        };
        let b = event_body(&without, Some("i1"), 0);
        assert!(b.get("nextAttemptAt").is_none(), "omitted when None");
    }

    #[test]
    fn optional_paths_omitted_when_absent_present_when_set() {
        let arch_none = Event::FileArchived {
            path: "a".into(),
            archive_path: None,
        };
        assert!(event_body(&arch_none, Some("i"), 0)
            .get("archivePath")
            .is_none());
        let arch_some = Event::FileArchived {
            path: "a".into(),
            archive_path: Some("/arch/a".into()),
        };
        assert_eq!(
            event_body(&arch_some, Some("i"), 0)["archivePath"],
            json!("/arch/a")
        );

        let q = Event::FileQuarantined {
            path: "a".into(),
            attempts: 5,
            last_error: "e".into(),
            quarantine_path: Some("/f/a".into()),
        };
        let b = event_body(&q, Some("i"), 0);
        assert_eq!(b["quarantinePath"], json!("/f/a"));
        assert_eq!(b["attempts"], json!(5));
        assert_eq!(b["lastError"], json!("e"));
    }

    #[test]
    fn component_event_body_has_no_instance() {
        let ev = Event::ComponentReady {
            instances: 3,
            version: "0.1.0".into(),
        };
        let b = event_body(&ev, None, 0);
        assert_eq!(b["event"], json!("ComponentReady"));
        assert_eq!(b["instances"], json!(3));
        assert_eq!(b["version"], json!("0.1.0"));
        assert!(b.get("instance").is_none());
    }

    #[test]
    fn names_cover_the_whole_catalog() {
        // Spot-check that name() matches the wire discriminator across shared-field variants.
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

    #[test]
    fn deferred_and_link_bodies_shape() {
        assert_eq!(
            event_body(&Event::WindowOpened { window: "02:00-04:00".into() }, Some("i"), 0)["window"],
            json!("02:00-04:00")
        );
        assert_eq!(
            event_body(&Event::Disconnected { link: "s3".into() }, Some("i"), 0)["link"],
            json!("s3")
        );
        assert_eq!(
            event_body(&Event::ScanComplete { discovered: 4, awaiting: 12 }, Some("i"), 0)["discovered"],
            json!(4)
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

    // ---- envelope + emit (through the fake) ----------------------------------------------------

    #[tokio::test]
    async fn instance_event_publishes_enveloped_message_to_the_right_topic() {
        let fake = FakeMessaging::new();
        let ev = emitter(fake.clone());
        ev.instance_event(
            "plant-1",
            Event::FileReady {
                path: "a/b.csv".into(),
                size: 42,
            },
            0,
        )
        .await;

        let recs = fake.records();
        assert_eq!(recs.len(), 1);
        let (topic, msg, retained) = &recs[0];
        assert_eq!(topic, "gw-01/file-replicator/evt/instances/plant-1/FileReady");
        assert!(!retained, "the API cannot publish retained (documented gap)");
        // Envelope: name/version/thing/tags + discriminated body.
        assert_eq!(msg.header.name, EVENT_MESSAGE_NAME);
        assert_eq!(msg.header.version, "1.0");
        assert_eq!(msg.tags.thing_name, "gw-01");
        assert_eq!(msg.tags.extra.get("site"), Some(&json!("factory-1")));
        assert_eq!(msg.body["event"], json!("FileReady"));
        assert_eq!(msg.body["instance"], json!("plant-1"));
        assert_eq!(msg.body["path"], json!("a/b.csv"));
        assert_eq!(msg.body["size"], json!(42));
    }

    #[tokio::test]
    async fn component_event_publishes_to_component_topic() {
        let fake = FakeMessaging::new();
        let ev = emitter(fake.clone());
        ev.component_event(
            Event::ComponentReady {
                instances: 2,
                version: "0.1.0".into(),
            },
            0,
        )
        .await;
        assert_eq!(fake.topics(), vec!["gw-01/file-replicator/evt/ComponentReady"]);
        assert!(fake.records()[0].1.body.get("instance").is_none());
    }

    #[tokio::test]
    async fn state_publishes_with_state_envelope_name() {
        let fake = FakeMessaging::new();
        let ev = emitter(fake.clone());
        ev.publish_instance_state("plant-1", json!({ "active": true }))
            .await;
        ev.publish_component_state(json!({ "instances": 1 })).await;

        let recs = fake.records();
        assert_eq!(recs[0].0, "gw-01/file-replicator/state/instances/plant-1");
        assert_eq!(recs[0].1.header.name, STATE_MESSAGE_NAME);
        assert_eq!(recs[0].1.body, json!({ "active": true }));
        assert_eq!(recs[1].0, "gw-01/file-replicator/state");
        assert_eq!(recs[1].1.header.name, STATE_MESSAGE_NAME);
    }

    #[tokio::test]
    async fn disabled_emitter_publishes_nothing() {
        // The no-messaging path: every method is a silent no-op (graceful degradation).
        let ev = Events::disabled();
        assert!(!ev.is_enabled());
        ev.instance_event("i", Event::FileDeleted { path: "p".into() }, 0)
            .await;
        ev.component_event(Event::ComponentReady { instances: 0, version: "v".into() }, 0)
            .await;
        ev.publish_instance_state("i", json!({})).await;
        ev.publish_component_state(json!({})).await;
        // No panic, and topics() is still queryable.
        assert_eq!(ev.topics().prefix(), "disabled");
    }

    #[tokio::test]
    async fn publish_failure_is_swallowed_not_propagated() {
        let fake = FakeMessaging::new();
        fake.fail.store(true, Ordering::SeqCst);
        let ev = emitter(fake.clone());
        // None of these panic or propagate despite the transport returning Err.
        ev.instance_event("i", Event::FileDeleted { path: "p".into() }, 0)
            .await;
        ev.component_event(Event::ComponentReady { instances: 0, version: "v".into() }, 0)
            .await;
        ev.publish_instance_state("i", json!({})).await;
        assert!(fake.records().is_empty(), "nothing recorded on failure");
    }

    #[tokio::test]
    async fn fake_secondary_methods_are_exercised() {
        // Cover the fake's non-`publish` trait surface (used by the control-plane slice) so the
        // event module's test-support does not drag its own coverage down.
        let fake = FakeMessaging::new();
        let m = MessageBuilder::new("T", "1.0").build();
        fake.publish_to_iot_core("t/iot", &m, Qos::AtLeastOnce).await.unwrap();
        fake.publish_raw("t/raw", &json!({ "x": 1 })).await.unwrap();
        fake.publish_to_iot_core_raw("t/rawiot", &json!({ "x": 2 }), Qos::AtMostOnce)
            .await
            .unwrap();
        let h = ggcommons::messaging::message_handler(|_t, _m| async {});
        fake.subscribe("f/#", h.clone(), 8, 1).await.unwrap();
        fake.subscribe_to_iot_core("f/#", h, Qos::AtLeastOnce, 8, 1).await.unwrap();
        fake.unsubscribe("f/#").await.unwrap();
        fake.unsubscribe_from_iot_core("f/#").await.unwrap();
        fake.reply(&m, MessageBuilder::new("R", "1.0").build()).await.unwrap();
        fake.reply_to_iot_core(&m, MessageBuilder::new("R", "1.0").build())
            .await
            .unwrap();
        assert!(fake.connected());
        assert_eq!(fake.records().len(), 3, "the three publishes were recorded");
    }

    #[tokio::test]
    async fn progress_event_rides_through_the_emitter() {
        let fake = FakeMessaging::new();
        let ev = emitter(fake.clone());
        ev.instance_event(
            "plant-1",
            Event::ReplicationProgress {
                path: "big.parquet".into(),
                size: 100,
                bytes_done: 30,
                percent: 30.0,
                destination: "s3".into(),
                attempt: 1,
            },
            0,
        )
        .await;
        assert_eq!(
            fake.topics(),
            vec!["gw-01/file-replicator/evt/instances/plant-1/ReplicationProgress"]
        );
    }

    #[tokio::test]
    async fn recording_support_fake_and_helpers_are_exercised() {
        // Cover the shared `test_support` fake's non-`publish` trait surface and its query helpers +
        // factory (used by the engine-wiring tests in `instance`/`instance::worker`), so the shared
        // support does not drag the crate's coverage down (mirrors `fake_secondary_methods…`).
        use super::test_support::{recording_events, RecordingMessaging};
        let fake = RecordingMessaging::new();
        let m = MessageBuilder::new("T", "1.0").build();
        fake.publish_to_iot_core("t/iot", &m, Qos::AtLeastOnce).await.unwrap();
        fake.publish_raw("t/raw", &json!({ "x": 1 })).await.unwrap();
        fake.publish_to_iot_core_raw("t/rawiot", &json!({ "x": 2 }), Qos::AtMostOnce)
            .await
            .unwrap();
        let h = ggcommons::messaging::message_handler(|_t, _m| async {});
        fake.subscribe("f/#", h.clone(), 8, 1).await.unwrap();
        fake.subscribe_to_iot_core("f/#", h, Qos::AtLeastOnce, 8, 1).await.unwrap();
        fake.unsubscribe("f/#").await.unwrap();
        fake.unsubscribe_from_iot_core("f/#").await.unwrap();
        assert!(fake.request("t", m.clone()).await.is_err());
        assert!(fake.request_from_iot_core("t", m.clone()).await.is_err());
        fake.reply(&m, MessageBuilder::new("R", "1.0").build()).await.unwrap();
        fake.reply_to_iot_core(&m, MessageBuilder::new("R", "1.0").build())
            .await
            .unwrap();
        assert!(fake.connected());

        // Query helpers + the `recording_events` factory used across the engine-wiring tests.
        let (fake2, ev) = recording_events();
        ev.component_event(Event::ComponentReady { instances: 0, version: "v".into() }, 0)
            .await;
        ev.publish_component_state(json!({ "ok": true })).await;
        assert_eq!(fake2.events_named("ComponentReady").len(), 1);
        assert_eq!(fake2.state_snapshots().len(), 1, "one FileReplicatorState snapshot");
        assert!(fake2.topics().iter().any(|t| t.ends_with("/state")));
    }
}

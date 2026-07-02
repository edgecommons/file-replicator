//! # file-replicator — P3 control-plane / event-stream integration tests against EMQX (DESIGN §15-§17)
//!
//! Drives the real Unified-Namespace control plane end-to-end over a **live MQTT broker** (the local
//! `ggcommons-emqx`, plaintext `localhost:1883`, no auth): the same [`MessagingService`] the component
//! uses in production ([`DefaultMessagingService`] over the standalone [`MqttProvider`]) carries the
//! event stream, the retained-ish `state/…` snapshots, and the `cmd/…` request/reply suite through the
//! broker and back. Everything the unit tests exercise with a fake is here proven on the wire.
//!
//! Every test **self-skips** when the broker is unreachable (a fast TCP probe → `eprintln!` + return),
//! so `cargo test` stays green on CI where EMQX is absent and the 90% coverage gate rests on the unit
//! tests alone. When EMQX is up (`docker start ggcommons-emqx`), the suite asserts:
//!   1. `driving_replication_publishes_lifecycle_events_and_state` — a real replication publishes the
//!      lifecycle events (`FileReady`/`ReplicationStarted`/`…Progress`/`…Completed`/`FileDeleted`) to
//!      the correct `{thing}/file-replicator/evt/instances/{id}/…` topics with correct bodies, the
//!      progress is **throttled** (few events, not one per chunk), and a `FileReplicatorState` snapshot
//!      lands on `state/instances/{id}` (see the retain gap note below).
//!   2. `get_status_request_receives_reply` — a `cmd/…/status` request gets a well-formed
//!      `FileReplicatorReply` back on its `reply_to`.
//!   3. `set_activation_deactivate_stops_work_and_reflects_in_status` — a `set-activation {active:false}`
//!      request stops new work AND is reflected in a subsequent `get-status`.
//!   4. `trigger_request_forces_a_scan` — a `cmd/…/trigger` request forces a scan that replicates a
//!      pending file.
//!
//! ## Retained-state gap (DESIGN §17.2 / FR-EVT-4)
//! ggcommons' `MessagingService` cannot set the MQTT retain flag today (documented in
//! `src/events.rs`), so the `state/…` snapshot is published **non-retained**. This test therefore
//! subscribes to `state/#` **before** driving the tick and asserts the snapshot is delivered live —
//! the interim behavior. A subscriber that connects *after* the last snapshot would not receive it;
//! the reliable current-state path is the `cmd/…/status` request/reply (test 2). Closing the gap needs
//! the one-line ggcommons enhancement (`publish_retained`) noted in `src/events.rs`.

#![cfg(feature = "standalone")]

use std::collections::BTreeMap;
use std::net::TcpStream;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ggcommons::messaging::config::MessagingConfig;
use ggcommons::messaging::provider::mqtt::MqttProvider;
use ggcommons::messaging::{
    message_handler, DefaultMessagingService, Message, MessageBuilder, MessagingService,
};
use ggcommons::prelude::Config;
use serde_json::{json, Value};
use tokio::sync::Semaphore;

use file_replicator::config::{
    CompletionCfg, EgressCfg, GlobReadiness, GlobalCfg, IngressCfg, InstanceCfg, LocalEgress,
    OnSuccess, ReadinessCfg, ScheduleCfg,
};
use file_replicator::control::{ControlPlane, InstanceControl};
use file_replicator::dest::DestDeps;
use file_replicator::events::Events;
use file_replicator::instance::{now_ms, Instance};
use file_replicator::ratelimit::TokenBucket;
use file_replicator::state::{SqliteStore, StateStore};
use file_replicator::uns::Topics;

/// The component's reverse-DNS full name (the `component_name` on every envelope).
const COMPONENT: &str = "com.mbreissi.greengrass.FileReplicator";
/// Broker address the standalone MQTT provider connects to (the local `ggcommons-emqx`).
const BROKER: &str = "127.0.0.1:1883";

/// True when EMQX's plaintext MQTT port answers a TCP connect within a short timeout.
fn emqx_up() -> bool {
    let addr = BROKER.parse().expect("addr");
    TcpStream::connect_timeout(&addr, Duration::from_millis(400)).is_ok()
}

/// Skip guard: run the test body only when EMQX is reachable, else print + return (test passes).
macro_rules! require_emqx {
    () => {
        if !emqx_up() {
            eprintln!("skipping P3 EMQX integration test: broker unreachable at {BROKER}");
            return;
        }
    };
}

/// A process-unique suffix so parallel tests never collide on client ids or topic prefixes.
fn unique(tag: &str) -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("fr-it-{tag}-{nanos:x}-{n}")
}

/// Connect a real [`MessagingService`] to the local EMQX broker (plaintext, no auth), with a unique
/// client id so concurrent tests don't evict each other.
async fn service(client_id: &str) -> Arc<dyn MessagingService> {
    let cfg = format!(
        r#"{{ "messaging": {{ "local": {{ "host": "localhost", "port": 1883, "clientId": "{client_id}" }} }} }}"#
    );
    let mc: MessagingConfig = serde_json::from_str(&cfg).expect("messaging config parses");
    let provider = MqttProvider::connect(&mc)
        .await
        .expect("connect to EMQX (is ggcommons-emqx up?)");
    Arc::new(DefaultMessagingService::new(Arc::new(provider)))
}

/// A thread-safe record of every `(topic, message)` a subscription received.
type Recorder = Arc<Mutex<Vec<(String, Message)>>>;

/// Subscribe `filter` on `svc`, recording every delivered `(topic, message)` into `rec`. Blocks
/// until the broker confirms the subscription (the provider awaits SUBACK), so a publish issued right
/// after this returns is not missed.
async fn subscribe_record(svc: &Arc<dyn MessagingService>, filter: &str, rec: Recorder) {
    let sink = rec.clone();
    let handler = message_handler(move |topic, msg| {
        let sink = sink.clone();
        async move {
            sink.lock().expect("recorder mutex").push((topic, msg));
        }
    });
    svc.subscribe(filter, handler, 256, 4)
        .await
        .expect("subscribe on EMQX");
}

/// The recorded events whose `body.event` discriminator equals `name`, in arrival order.
fn events_named(rec: &Recorder, name: &str) -> Vec<(String, Message)> {
    rec.lock()
        .expect("recorder mutex")
        .iter()
        .filter(|(_, m)| m.body.get("event").and_then(Value::as_str) == Some(name))
        .cloned()
        .collect()
}

/// Poll until `pred` holds over the recorded messages, up to ~10 s (broker round-trips are async).
async fn wait_until<F: Fn(&[(String, Message)]) -> bool>(rec: &Recorder, pred: F) -> bool {
    for _ in 0..400 {
        {
            let guard = rec.lock().expect("recorder mutex");
            if pred(&guard) {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    false
}

/// A component config snapshot for `thing` (empty user subtree — the tests exercise the control plane,
/// not config parsing).
fn config(thing: &str) -> Arc<Config> {
    Arc::new(Config::from_value(COMPONENT, thing, json!({})).expect("config"))
}

/// An immediate-mode, delete-on-success local-egress instance config (ready-immediately glob so a
/// single tick discovers + processes with no readiness-window timing).
fn instance_cfg(id: &str, src: &Path, dst: &Path) -> InstanceCfg {
    InstanceCfg {
        id: id.to_string(),
        enabled: true,
        ingress: IngressCfg {
            path: src.to_path_buf(),
            recursive: true,
            include: vec![],
            exclude: vec![],
            rescan_secs: Some(1),
            readiness: ReadinessCfg::Glob(GlobReadiness { ready: vec![] }),
        },
        egress: vec![EgressCfg::Local(LocalEgress {
            path: dst.to_path_buf(),
            fsync: false,
        })],
        schedule: ScheduleCfg::Immediate,
        completion: CompletionCfg {
            on_success: OnSuccess::Delete,
            ..Default::default()
        },
        retry: None,
        limits: None,
        topics: None,
    }
}

/// Build one real [`Instance`] wired to a live event emitter over `svc`, rooted at
/// `{thing}/file-replicator`. Returns the instance handle + the resolved topic set (for assertions).
fn build_instance(
    svc: &Arc<dyn MessagingService>,
    thing: &str,
    id: &str,
    src: &Path,
    dst: &Path,
    store: Arc<dyn StateStore>,
) -> (Arc<Instance>, Arc<Topics>) {
    let topics = Arc::new(Topics::from_prefix(format!("{thing}/file-replicator")));
    let events = Events::new(
        Some(svc.clone()),
        topics.clone(),
        thing.to_string(),
        BTreeMap::new(),
    );
    let inst = Instance::build(
        instance_cfg(id, src, dst),
        &GlobalCfg::default(),
        store,
        Arc::new(Semaphore::new(16)),
        Arc::new(TokenBucket::unlimited()),
        None,
        &DestDeps::default(),
        events,
    )
    .expect("instance builds");
    (Arc::new(inst), topics)
}

/// Start a control plane over `svc` driving `inst`, subscribed to its `cmd/#` space on the broker.
async fn start_control(
    svc: &Arc<dyn MessagingService>,
    thing: &str,
    topics: Arc<Topics>,
    store: Arc<dyn StateStore>,
    inst: Arc<Instance>,
) -> Arc<ControlPlane> {
    let events = Events::new(
        Some(svc.clone()),
        topics.clone(),
        thing.to_string(),
        BTreeMap::new(),
    );
    let control = Arc::new(ControlPlane::new(
        Some(svc.clone()),
        topics,
        config(thing),
        store,
        vec![inst as Arc<dyn InstanceControl>],
        events,
        false,
    ));
    control.clone().start().await.expect("control plane subscribes");
    control
}

/// Issue a control request and await its reply on the ephemeral `reply_to` topic (5 s deadline).
async fn request(svc: &Arc<dyn MessagingService>, topic: &str, name: &str, body: Value) -> Message {
    let req = MessageBuilder::new(name, "1.0").payload(body).build();
    let fut = svc.request(topic, req).await.expect("publish request");
    tokio::time::timeout(Duration::from_secs(5), fut)
        .await
        .expect("reply within 5s")
        .expect("reply is a valid message")
}

fn write_file(path: &Path, bytes: &[u8]) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, bytes).unwrap();
}

// ---- 1. events + throttled progress + retained-ish state on the wire ----------------------------

#[tokio::test]
async fn driving_replication_publishes_lifecycle_events_and_state() {
    require_emqx!();
    let thing = unique("evt");
    let id = "plant-1";
    let svc = service(&unique("evt-cli")).await;

    // Subscribe to the whole component namespace BEFORE any work, so no event/state publish is missed
    // (nothing is retained — see the module-level gap note).
    let rec: Recorder = Arc::new(Mutex::new(Vec::new()));
    subscribe_record(&svc, &format!("{thing}/file-replicator/#"), rec.clone()).await;

    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    // A 13 MiB file crosses the worker's 4 MiB progress-checkpoint step several times, so the throttle
    // has multiple observations to gate (proving it emits *few* events, not one per 64 KiB chunk).
    let size = 13 * 1024 * 1024usize;
    write_file(&src.path().join("big.bin"), &vec![0x5au8; size]);

    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
    let (inst, _topics) = build_instance(&svc, &thing, id, src.path(), dst.path(), store);

    inst.tick(now_ms()).await;

    // Wait for the terminal per-file event to arrive over the broker; everything else has been
    // published (and awaited) before it, so it is a safe barrier.
    let evt_root = format!("{thing}/file-replicator/evt/instances/{id}");
    assert!(
        wait_until(&rec, |ms| ms
            .iter()
            .any(|(_, m)| m.body.get("event").and_then(Value::as_str) == Some("FileDeleted")))
            .await,
        "the FileDeleted lifecycle event should arrive on the broker"
    );
    // Give any trailing state snapshot a beat to land.
    assert!(
        wait_until(&rec, |ms| ms
            .iter()
            .any(|(_, m)| m.header.name == "FileReplicatorState"))
            .await,
        "a state snapshot should be published"
    );

    // FileReady (discovery) with the right body + topic.
    let ready = events_named(&rec, "FileReady");
    assert_eq!(ready.len(), 1, "exactly one FileReady");
    assert_eq!(ready[0].0, format!("{evt_root}/FileReady"), "FileReady topic");
    assert_eq!(ready[0].1.body["path"], json!("big.bin"));
    assert_eq!(ready[0].1.body["size"], json!(size));
    assert_eq!(ready[0].1.header.name, "FileReplicatorEvent");
    assert_eq!(ready[0].1.tags.thing_name, thing);

    // ReplicationStarted body.
    let started = events_named(&rec, "ReplicationStarted");
    assert_eq!(started.len(), 1);
    assert_eq!(started[0].0, format!("{evt_root}/ReplicationStarted"));
    assert_eq!(started[0].1.body["destination"], json!("local"));
    assert_eq!(started[0].1.body["attempt"], json!(1));

    // Throttled progress: far fewer than the ~208 chunk-reports a 13 MiB file makes, but the FR-EVT-2
    // 0%/100% endpoints are always present (the sink forwards the first + terminal report past the
    // 4 MiB persist gate). See the unit tests in `worker.rs`/`events.rs` for the throttle logic itself.
    let progress = events_named(&rec, "ReplicationProgress");
    assert!(!progress.is_empty(), "at least one throttled progress event");
    assert!(
        progress.len() <= 50,
        "progress must be throttled, not one per chunk (got {})",
        progress.len()
    );
    for (topic, m) in &progress {
        assert_eq!(topic, &format!("{evt_root}/ReplicationProgress"));
        let pct = m.body["percent"].as_f64().expect("percent is a number");
        assert!((0.0..=100.0).contains(&pct), "percent {pct} out of range");
        assert!(m.body["bytesDone"].as_u64().unwrap() <= size as u64);
        assert_eq!(m.body["destination"], json!("local"));
    }
    let percents: Vec<f64> = progress.iter().filter_map(|(_, m)| m.body["percent"].as_f64()).collect();
    assert!(percents.contains(&0.0), "the 0% endpoint is always emitted (FR-EVT-2)");
    assert!(percents.contains(&100.0), "the 100% endpoint is always emitted (FR-EVT-2)");

    // ReplicationCompleted body.
    let completed = events_named(&rec, "ReplicationCompleted");
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].1.body["bytes"], json!(size));
    assert_eq!(completed[0].1.body["destination"], json!("local"));

    // FileDeleted (delete-on-success side effect).
    let deleted = events_named(&rec, "FileDeleted");
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].1.body["path"], json!("big.bin"));

    // The state snapshot on state/instances/{id} reflects the completed transfer, and renders like the
    // get-status document (same source), proving the retained-ish current-state path (interim: live).
    let state: Vec<(String, Message)> = rec
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, m)| m.header.name == "FileReplicatorState")
        .cloned()
        .collect();
    let (state_topic, state_msg) = state.last().expect("a state snapshot was captured");
    assert_eq!(state_topic, &format!("{thing}/file-replicator/state/instances/{id}"));
    assert_eq!(state_msg.body["instance"], json!(id));
    assert_eq!(state_msg.body["active"], json!(true));
    assert_eq!(state_msg.body["replicated"]["count"], json!(1));

    svc.unsubscribe(&format!("{thing}/file-replicator/#")).await.ok();
}

// ---- 2. get-status request/reply ----------------------------------------------------------------

#[tokio::test]
async fn get_status_request_receives_reply() {
    require_emqx!();
    let thing = unique("status");
    let id = "plant-1";
    let svc = service(&unique("status-cli")).await;

    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    // Seed one file and replicate it so the status has a non-trivial `replicated` count.
    write_file(&src.path().join("done.csv"), b"a,b,c\n1,2,3\n");

    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
    let (inst, topics) = build_instance(&svc, &thing, id, src.path(), dst.path(), store.clone());
    inst.tick(now_ms()).await;
    let control = start_control(&svc, &thing, topics, store, inst).await;

    // Per-instance get-status via request/reply on the broker.
    let reply = request(
        &svc,
        &format!("{thing}/file-replicator/cmd/instances/{id}/status"),
        "GetStatus",
        json!({}),
    )
    .await;
    assert_eq!(reply.header.name, "FileReplicatorReply");
    assert_eq!(reply.tags.thing_name, thing);
    assert_eq!(reply.body["instance"], json!(id));
    assert_eq!(reply.body["active"], json!(true));
    assert_eq!(reply.body["replicated"]["count"], json!(1), "the replicated file is counted");

    // Component-wide get-status wraps every instance with a summary.
    let all = request(
        &svc,
        &format!("{thing}/file-replicator/cmd/status"),
        "GetStatus",
        json!({}),
    )
    .await;
    assert_eq!(all.body["thing"], json!(thing));
    assert_eq!(all.body["summary"]["instances"], json!(1));
    assert_eq!(all.body["summary"]["active"], json!(1));

    control.stop().await;
}

// ---- 3. set-activation deactivate stops new work + shows in get-status ---------------------------

#[tokio::test]
async fn set_activation_deactivate_stops_work_and_reflects_in_status() {
    require_emqx!();
    let thing = unique("deact");
    let id = "plant-1";
    let svc = service(&unique("deact-cli")).await;

    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
    let (inst, topics) = build_instance(&svc, &thing, id, src.path(), dst.path(), store.clone());
    let control = start_control(&svc, &thing, topics, store.clone(), inst.clone()).await;

    // Deactivate over the wire (persist defaults true).
    let reply = request(
        &svc,
        &format!("{thing}/file-replicator/cmd/instances/{id}/activation"),
        "SetActivation",
        json!({ "active": false }),
    )
    .await;
    assert_eq!(reply.body["ok"], json!(true));
    assert_eq!(reply.body["active"], json!(false));
    assert_eq!(reply.body["persisted"], json!(true));
    assert!(!inst.is_active(), "runtime flag flipped off");
    assert!(
        !store.load_activation(id).unwrap().unwrap().active,
        "deactivation persisted to durable state"
    );

    // New work must NOT flow while deactivated: a file dropped now is not replicated on a tick.
    write_file(&src.path().join("ignored.txt"), b"should not be copied while off");
    inst.tick(now_ms()).await;
    assert!(
        !dst.path().join("ignored.txt").exists(),
        "a deactivated instance replicates nothing"
    );
    assert!(store.get(id, "ignored.txt").unwrap().is_none(), "no work item enqueued while off");

    // ...and the deactivation is reflected in a subsequent get-status.
    let status = request(
        &svc,
        &format!("{thing}/file-replicator/cmd/instances/{id}/status"),
        "GetStatus",
        json!({}),
    )
    .await;
    assert_eq!(status.body["active"], json!(false), "get-status shows the instance is off");
    assert_eq!(status.body["replicated"]["count"], json!(0));

    control.stop().await;
}

// ---- 4. trigger forces a scan -------------------------------------------------------------------

#[tokio::test]
async fn trigger_request_forces_a_scan() {
    require_emqx!();
    let thing = unique("trig");
    let id = "plant-1";
    let svc = service(&unique("trig-cli")).await;

    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    // A pending file that no tick has processed yet — only the trigger will move it.
    write_file(&src.path().join("pending.dat"), b"replicate me on trigger");

    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
    let (inst, topics) = build_instance(&svc, &thing, id, src.path(), dst.path(), store.clone());
    let control = start_control(&svc, &thing, topics, store.clone(), inst).await;

    // Force a scan for this instance over the wire. The handler replies "accepted + counts"
    // IMMEDIATELY and runs the scan as a detached task (so a long/slow batch never times out the
    // request or holds a command slot), so we poll for the replication to land after the reply.
    let reply = request(
        &svc,
        &format!("{thing}/file-replicator/cmd/instances/{id}/trigger"),
        "Trigger",
        json!({ "ignoreWindow": true }),
    )
    .await;
    assert_eq!(reply.body["ok"], json!(true));
    assert_eq!(reply.body["scope"], json!(id));
    assert_eq!(reply.body["triggered"], json!(1));

    // The detached scan replicates the pending file shortly after the (immediate) reply.
    let out = dst.path().join("pending.dat");
    for _ in 0..200 {
        if out.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        std::fs::read(&out).unwrap(),
        b"replicate me on trigger",
        "trigger forced a scan that replicated the pending file"
    );
    assert_eq!(store.get(id, "pending.dat").unwrap().unwrap().state.as_str(), "completed");
    assert_eq!(store.stats(id).unwrap().replicated, 1);

    control.stop().await;
}

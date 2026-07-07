//! # file-replicator — control-plane / event-stream integration tests against EMQX (DESIGN §15-§17)
//!
//! Drives the real Unified-Namespace control plane end-to-end over a **live MQTT broker** (the local
//! `edgecommons-emqx`, plaintext `localhost:1883`, no auth): a REAL [`EdgeCommons`] runtime (`--platform
//! HOST --transport MQTT`, built via [`EdgeCommonsBuilder`]) carries the `evt` stream and the `cmd`
//! request/reply suite through the broker and back, through the edgecommons `events()` facade and
//! `commands()` inbox — not a hand-rolled topic scheme. Everything the unit tests exercise with a
//! recorder is here proven through the normal EdgeCommons protobuf message path, on the real UNS grammar
//! (`ecv1/{thing}/FileReplicator/{instance}/{class}…` — `FileReplicator` is the short component token
//! edgecommons derives from `com.mbreissi.edgecommons.FileReplicator`, DESIGN-uns §2 D-U18).
//!
//! Every test **self-skips** when the broker is unreachable (a fast TCP probe → `eprintln!` + return),
//! so `cargo test` stays green on CI where EMQX is absent and the 90% coverage gate rests on the unit
//! tests alone. When EMQX is up (`docker start edgecommons-emqx`), the suite asserts:
//!   1. `driving_replication_publishes_lifecycle_events` — a real replication publishes the lifecycle
//!      events (`file-ready`/`replication-started`/`…-progress`/`…-completed`/`file-deleted`) on
//!      `evt/{severity}/{type}` with the facade's body contract, and progress is **throttled** (few
//!      events, not one per chunk).
//!   2. `get_status_request_receives_reply` — a `get-status` command gets the decoded diagnostic body
//!      shape `{"ok":true,"result":{…}}` back on its `reply_to`.
//!   3. `set_activation_deactivate_stops_work_and_reflects_in_status` — a `set-activation
//!      {instance,active:false}` request stops new work AND is reflected in a subsequent
//!      `get-status`.
//!   4. `trigger_request_forces_a_scan` — a `trigger {instance}` request forces a scan that replicates
//!      a pending file.
//!
//! ## `state/…` — dropped, not migrated
//! The old retained-ish `state/…` snapshot publish no longer exists (see `crate::events`/
//! `crate::control` module docs: `state` is now a reserved, library-owned UNS class carrying only the
//! RUNNING/STOPPED keepalive). `get-status` (test 2) is the reliable current-state-on-demand path.

#![cfg(feature = "standalone")]

use std::net::TcpStream;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use edgecommons::messaging::{message_handler, Message, MessageBuilder, MessagingService};
use edgecommons::prelude::*;
use edgecommons::uns::UnsClass;
use serde_json::{json, Value};

use file_replicator::admission::PriorityGate;
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

/// The component's reverse-DNS full name — edgecommons derives the SHORT UNS `component` token
/// (`FileReplicator`) from the segment after the last `.` (DESIGN-uns §2, D-U18).
const COMPONENT: &str = "com.mbreissi.edgecommons.FileReplicator";
/// Broker address the standalone MQTT provider connects to (the local `edgecommons-emqx`).
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
            eprintln!("skipping control-plane EMQX integration test: broker unreachable at {BROKER}");
            return;
        }
    };
}

/// A process-unique suffix so parallel tests never collide on client ids or thing names.
fn unique(tag: &str) -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("fr-it-{tag}-{nanos:x}-{n}")
}

/// Build a REAL [`EdgeCommons`] runtime connected to the local EMQX broker over standalone MQTT, with a
/// unique thing name + MQTT client id so parallel tests never collide. Writes the messaging/component
/// config to a temp dir (the standard CLI contract needs real files for `-c FILE`), leaked for the
/// process lifetime (read once at startup).
async fn build_gg(thing: &str) -> EdgeCommons {
    let dir = tempfile::tempdir().expect("tempdir");
    let msg_path = dir.path().join("messaging.json");
    std::fs::write(
        &msg_path,
        format!(
            r#"{{"messaging":{{"local":{{"host":"localhost","port":1883,"clientId":"{}"}}}}}}"#,
            unique("cli")
        ),
    )
    .expect("write messaging config");
    let cfg_path = dir.path().join("config.json");
    std::fs::write(
        &cfg_path,
        r#"{"tags":{"site":"file-replicator-it","suite":"p3-control-emqx"},"component":{}}"#,
    )
    .expect("write component config");
    std::mem::forget(dir); // keep the temp files alive for the process (read once at startup)

    EdgeCommonsBuilder::new(COMPONENT)
        .args([
            "file-replicator",
            "--platform",
            "HOST",
            "--transport",
            "MQTT",
            msg_path.to_str().unwrap(),
            "-c",
            "FILE",
            cfg_path.to_str().unwrap(),
            "-t",
            thing,
        ])
        .build()
        .await
        .expect("gg builds (is edgecommons-emqx up?)")
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
    svc.subscribe(filter, handler, 256, 4).await.expect("subscribe on EMQX");
}

/// The recorded `evt` messages whose `body.type` (the facade's channel-token discriminator) equals
/// `wire_type` (kebab-case — e.g. `"file-ready"`, `"replication-progress"`), in arrival order.
fn events_named(rec: &Recorder, wire_type: &str) -> Vec<(String, Message)> {
    rec.lock()
        .expect("recorder mutex")
        .iter()
        .filter(|(_, m)| m.body.get("type").and_then(Value::as_str) == Some(wire_type))
        .cloned()
        .collect()
}

fn assert_core_protobuf_round_trip(msg: &Message, thing: &str, instance: &str) {
    let bytes = msg.to_vec().expect("message encodes as EdgeCommons protobuf");
    assert!(
        serde_json::from_slice::<Value>(&bytes).is_err(),
        "normal EdgeCommons MQTT payloads must be protobuf bytes, not JSON text"
    );

    let decoded = Message::from_slice(&bytes).expect("message decodes from EdgeCommons protobuf");
    assert_eq!(decoded.header.name, msg.header.name);
    assert_eq!(decoded.body, msg.body);
    assert_eq!(decoded.identity, msg.identity);
    assert_eq!(decoded.tags, msg.tags);

    let identity = decoded.identity.as_ref().expect("protobuf envelope preserves identity");
    assert_eq!(identity.device(), thing);
    assert_eq!(identity.component(), "FileReplicator");
    assert_eq!(identity.instance(), instance);

    let tags = decoded.tags.as_ref().expect("protobuf envelope preserves config tags");
    assert_eq!(tags.extra.get("site"), Some(&json!("file-replicator-it")));
    assert_eq!(tags.extra.get("suite"), Some(&json!("p3-control-emqx")));
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
        on_permission_error: None,
        priority: 100,
    }
}

/// Build one real [`Instance`] wired to `gg`'s own bound `events()` facade for `id` (minted once,
/// mirroring `crate::app`'s wiring).
fn build_instance(gg: &EdgeCommons, id: &str, src: &Path, dst: &Path, store: Arc<dyn StateStore>) -> Arc<Instance> {
    let events = Events::new(gg.instance(id).expect("valid instance id").events());
    let inst = Instance::build(
        instance_cfg(id, src, dst),
        &GlobalCfg::default(),
        store,
        Arc::new(PriorityGate::new(16)),
        Arc::new(TokenBucket::unlimited()),
        None,
        &DestDeps::default(),
        events,
    )
    .expect("instance builds");
    Arc::new(inst)
}

/// Start a control plane over `gg`'s command inbox driving `inst`.
fn start_control(gg: &EdgeCommons, store: Arc<dyn StateStore>, inst: Arc<Instance>) -> Arc<ControlPlane> {
    let main_events = Events::new(gg.events());
    let control = Arc::new(ControlPlane::new(
        gg.config(),
        store,
        vec![inst as Arc<dyn InstanceControl>],
        main_events,
    ));
    let commands = gg.commands().expect("command inbox available over MQTT");
    control.clone().register(&commands);
    control
}

/// Issue a command request (topic minted via `gg.uns()`, `header.name` = the verb — the CommandInbox
/// contract) and await its reply on the ephemeral `reply_to` topic (5 s deadline).
async fn request(gg: &EdgeCommons, verb: &str, body: Value) -> Message {
    let topic = gg.uns().topic_with_channel(UnsClass::Cmd, verb).expect("cmd topic");
    let req = MessageBuilder::new(verb, "1.0").payload(body).build();
    let fut = gg.messaging().expect("messaging").request(&topic, req).await.expect("publish request");
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

// ---- 1. events + throttled progress through the protobuf messaging path -------------------------

#[tokio::test]
async fn driving_replication_publishes_lifecycle_events() {
    require_emqx!();
    let thing = unique("evt");
    let id = "plant-1";
    let gg = build_gg(&thing).await;
    let svc = gg.messaging().expect("messaging");

    // Subscribe the whole component namespace BEFORE any work, so no `evt` publish is missed (`evt`
    // is not retained — DESIGN-class-facades; there is no retain concept in the new UNS core either).
    let rec: Recorder = Arc::new(Mutex::new(Vec::new()));
    subscribe_record(&svc, &format!("ecv1/{thing}/FileReplicator/#"), rec.clone()).await;

    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    // A 13 MiB file crosses the worker's 4 MiB progress-checkpoint step several times, so the throttle
    // has multiple observations to gate (proving it emits *few* events, not one per 64 KiB chunk).
    let size = 13 * 1024 * 1024usize;
    write_file(&src.path().join("big.bin"), &vec![0x5au8; size]);

    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
    let inst = build_instance(&gg, id, src.path(), dst.path(), store);

    inst.tick(now_ms()).await;

    // Wait for the terminal per-file event to arrive over the broker; everything else has been
    // published (and awaited) before it, so it is a safe barrier.
    let evt_root = format!("ecv1/{thing}/FileReplicator/{id}/evt");
    assert!(
        wait_until(&rec, |ms| ms
            .iter()
            .any(|(_, m)| m.body.get("type").and_then(Value::as_str) == Some("file-deleted")))
            .await,
        "the file-deleted lifecycle event should arrive on the broker"
    );

    // file-ready (discovery) with the right topic + facade body contract (severity/type/timestamp/
    // context — DESIGN-class-facades §2.2).
    let ready = events_named(&rec, "file-ready");
    assert_eq!(ready.len(), 1, "exactly one file-ready");
    assert_eq!(ready[0].0, format!("{evt_root}/info/file-ready"), "severity derives the channel");
    assert_eq!(ready[0].1.body["severity"], json!("info"));
    assert_eq!(ready[0].1.body["context"]["path"], json!("big.bin"));
    assert_eq!(ready[0].1.body["context"]["size"], json!(size));
    assert!(ready[0].1.body.get("timestamp").is_some());
    assert_eq!(ready[0].1.identity.as_ref().unwrap().device(), thing);
    assert_eq!(ready[0].1.identity.as_ref().unwrap().instance(), id);
    assert_core_protobuf_round_trip(&ready[0].1, &thing, id);

    // replication-started body.
    let started = events_named(&rec, "replication-started");
    assert_eq!(started.len(), 1);
    assert_eq!(started[0].0, format!("{evt_root}/info/replication-started"));
    assert_eq!(started[0].1.body["context"]["destination"], json!("local"));
    assert_eq!(started[0].1.body["context"]["attempt"], json!(1));

    // Throttled progress: far fewer than the ~208 chunk-reports a 13 MiB file makes, but the FR-EVT-2
    // 0%/100% endpoints are always present (the sink forwards the first + terminal report past the
    // 4 MiB persist gate). See the unit tests in `worker.rs`/`events.rs` for the throttle logic itself.
    let progress = events_named(&rec, "replication-progress");
    assert!(!progress.is_empty(), "at least one throttled progress event");
    assert!(
        progress.len() <= 50,
        "progress must be throttled, not one per chunk (got {})",
        progress.len()
    );
    for (topic, m) in &progress {
        assert_eq!(topic, &format!("{evt_root}/info/replication-progress"));
        let pct = m.body["context"]["percent"].as_f64().expect("percent is a number");
        assert!((0.0..=100.0).contains(&pct), "percent {pct} out of range");
        assert!(m.body["context"]["bytesDone"].as_u64().unwrap() <= size as u64);
        assert_eq!(m.body["context"]["destination"], json!("local"));
    }
    let percents: Vec<f64> =
        progress.iter().filter_map(|(_, m)| m.body["context"]["percent"].as_f64()).collect();
    assert!(percents.contains(&0.0), "the 0% endpoint is always emitted (FR-EVT-2)");
    assert!(percents.contains(&100.0), "the 100% endpoint is always emitted (FR-EVT-2)");

    // replication-completed body.
    let completed = events_named(&rec, "replication-completed");
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].1.body["context"]["bytes"], json!(size));
    assert_eq!(completed[0].1.body["context"]["destination"], json!("local"));

    // file-deleted (delete-on-success side effect).
    let deleted = events_named(&rec, "file-deleted");
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].1.body["context"]["path"], json!("big.bin"));

    svc.unsubscribe(&format!("ecv1/{thing}/FileReplicator/#")).await.ok();
}

// ---- 2. get-status request/reply ----------------------------------------------------------------

#[tokio::test]
async fn get_status_request_receives_reply() {
    require_emqx!();
    let thing = unique("status");
    let id = "plant-1";
    let gg = build_gg(&thing).await;

    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    // Seed one file and replicate it so the status has a non-trivial `replicated` count.
    write_file(&src.path().join("done.csv"), b"a,b,c\n1,2,3\n");

    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
    let inst = build_instance(&gg, id, src.path(), dst.path(), store.clone());
    inst.tick(now_ms()).await;
    let _control = start_control(&gg, store, inst);

    // Per-instance get-status via request/reply on the broker.
    let reply = request(&gg, "get-status", json!({ "instance": id })).await;
    assert_eq!(reply.header.name, "get-status");
    assert_eq!(reply.identity.as_ref().unwrap().device(), thing);
    assert_eq!(reply.body["ok"], json!(true));
    assert_core_protobuf_round_trip(&reply, &thing, "main");
    let result = &reply.body["result"];
    assert_eq!(result["instance"], json!(id));
    assert_eq!(result["active"], json!(true));
    assert_eq!(result["replicated"]["count"], json!(1), "the replicated file is counted");

    // Component-wide get-status wraps every instance with a summary.
    let all = request(&gg, "get-status", json!({})).await;
    let all_result = &all.body["result"];
    assert_eq!(all_result["thing"], json!(thing));
    assert_eq!(all_result["summary"]["instances"], json!(1));
    assert_eq!(all_result["summary"]["active"], json!(1));
}

// ---- 3. set-activation deactivate stops new work + shows in get-status ---------------------------

#[tokio::test]
async fn set_activation_deactivate_stops_work_and_reflects_in_status() {
    require_emqx!();
    let thing = unique("deact");
    let id = "plant-1";
    let gg = build_gg(&thing).await;

    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
    let inst = build_instance(&gg, id, src.path(), dst.path(), store.clone());
    let _control = start_control(&gg, store.clone(), inst.clone());

    // Deactivate over the wire (persist defaults true).
    let reply = request(&gg, "set-activation", json!({ "instance": id, "active": false })).await;
    assert_eq!(reply.body["ok"], json!(true));
    let result = &reply.body["result"];
    assert_eq!(result["active"], json!(false));
    assert_eq!(result["persisted"], json!(true));
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
    let status = request(&gg, "get-status", json!({ "instance": id })).await;
    let status_result = &status.body["result"];
    assert_eq!(status_result["active"], json!(false), "get-status shows the instance is off");
    assert_eq!(status_result["replicated"]["count"], json!(0));
}

// ---- 4. trigger forces a scan -------------------------------------------------------------------

#[tokio::test]
async fn trigger_request_forces_a_scan() {
    require_emqx!();
    let thing = unique("trig");
    let id = "plant-1";
    let gg = build_gg(&thing).await;

    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    // A pending file that no tick has processed yet — only the trigger will move it.
    write_file(&src.path().join("pending.dat"), b"replicate me on trigger");

    let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
    let inst = build_instance(&gg, id, src.path(), dst.path(), store.clone());
    let _control = start_control(&gg, store.clone(), inst);

    // Force a scan for this instance over the wire. The handler replies "accepted + counts"
    // IMMEDIATELY and runs the scan as a detached task (so a long/slow batch never times out the
    // request or holds a command slot), so we poll for the replication to land after the reply.
    let reply = request(&gg, "trigger", json!({ "instance": id, "ignoreWindow": true })).await;
    assert_eq!(reply.body["ok"], json!(true));
    let result = &reply.body["result"];
    assert_eq!(result["scope"], json!(id));
    assert_eq!(result["triggered"], json!(1));

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
}

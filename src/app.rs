//! # file-replicator — application wiring (DESIGN §6.1)
//!
//! Builds the process-wide shared state — one durable [`SqliteStore`], the **global** concurrency
//! semaphore and the **global** bandwidth token bucket (aggregate caps from `component.global.limits`),
//! and the completion/failure metric definition — then assembles one [`Instance`] per parsed
//! `component.instances[]` entry and spawns its run loop. [`App::run`] marks the component ready and
//! awaits [`GgCommons::shutdown_signal`]; on shutdown it cancels every instance, waits for the loops to
//! drain, and flushes metrics. Durable state is written ahead of every side effect, so a clean stop and
//! a crash recover identically (DESIGN §13.2) — there is no separate checkpoint step.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use ggcommons::prelude::*;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::config::{self, InstanceCfg};
use crate::control::{ControlPlane, InstanceControl};
use crate::dest::DestDeps;
use crate::events::{Event, Events};
use crate::instance::{now_ms, Instance};
use crate::ratelimit::{parse_byte_rate, SystemClock, TokenBucket};
use crate::state::{SqliteStore, StateStore};
use crate::uns::Topics;

/// Split instance configs into the ones to keep (first occurrence of each id) and the ids that were
/// dropped as duplicates. Instance identity keys every durable-state table, so a duplicate id must
/// never start a second instance (see [`App::run`]).
fn dedup_instance_ids(cfgs: Vec<InstanceCfg>) -> (Vec<InstanceCfg>, Vec<String>) {
    let mut seen: HashSet<String> = HashSet::new();
    let mut kept = Vec::with_capacity(cfgs.len());
    let mut dropped = Vec::new();
    for cfg in cfgs {
        if seen.insert(cfg.id.clone()) {
            kept.push(cfg);
        } else {
            dropped.push(cfg.id.clone());
        }
    }
    (kept, dropped)
}

/// Global in-flight cap when `component.global.limits.maxConcurrentFiles` is unset (a generous
/// ceiling; per-instance caps do the fine-grained limiting).
const DEFAULT_GLOBAL_SLOTS: usize = 64;
/// The durable state DB filename, created under the component's working directory.
const STATE_DB_FILE: &str = "file-replicator-state.db";

/// The component's parsed configuration and service handles.
pub struct App {
    config: Arc<Config>,
    metrics: Arc<dyn MetricService>,
}

/// A [`ConfigurationChangeListener`] invoked when the component configuration is hot-reloaded. P1 logs
/// the change; per-instance re-application (drain/rebuild the changed instance) lands with the control
/// surface in P3 (FR-CFG-6).
struct ConfigListener;

#[async_trait::async_trait]
impl ConfigurationChangeListener for ConfigListener {
    async fn on_configuration_change(&self, config: Arc<Config>) -> bool {
        let n = config::load_instances(&config).len();
        tracing::info!(thing = %config.thing_name, instances = n, "configuration changed (rebuild is P3)");
        true
    }
}

impl App {
    /// Capture the service handles and register for config hot-reload.
    pub fn new(gg: &GgCommons) -> anyhow::Result<Self> {
        gg.add_config_change_listener(Arc::new(ConfigListener));
        Ok(Self {
            config: gg.config(),
            metrics: gg.metrics(),
        })
    }

    /// Build the shared governors + store, start every instance, then run until shutdown.
    pub async fn run(&self, gg: &GgCommons) -> anyhow::Result<()> {
        let instances_cfg: Vec<InstanceCfg> = config::load_instances(&self.config);
        let global = config::load_global(&self.config);

        // One durable store shared by all instances, under the component work dir.
        let db_path = std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(STATE_DB_FILE);
        let store: Arc<dyn StateStore> = Arc::new(
            SqliteStore::open(&db_path)
                .map_err(|e| anyhow::anyhow!("open state db {}: {e}", db_path.display()))?,
        );
        tracing::info!(db = %db_path.display(), "durable state opened");

        // Global concurrency + bandwidth caps (aggregate across instances).
        let global_slots = global
            .limits
            .as_ref()
            .and_then(|l| l.max_concurrent_files)
            .unwrap_or(DEFAULT_GLOBAL_SLOTS)
            .max(1);
        let global_sem = Arc::new(Semaphore::new(global_slots));
        let global_rate = global
            .limits
            .as_ref()
            .and_then(|l| l.max_bandwidth.as_deref())
            .map(parse_byte_rate)
            .transpose()
            .map_err(|e| anyhow::anyhow!("component.global maxBandwidth: {e}"))?
            .unwrap_or(0);
        let global_bw = Arc::new(TokenBucket::new(global_rate, Arc::new(SystemClock)));

        // Define the completion/failure metric once; instances emit against it (undefined metrics are
        // ignored by the library, so a metric-less target is harmless).
        self.metrics.define_metric(
            MetricBuilder::create("fileReplicator")
                .with_config(&self.config)
                .add_measure("filesReplicated", "Count", 60)
                .add_measure("bytesReplicated", "Bytes", 60)
                .add_measure("filesFailed", "Count", 60)
                .build(),
        );

        // Reject duplicate instance ids up front: all durable state is keyed by (id, relpath), so two
        // instances sharing an id would clobber each other's work rows/stats/activation and silently
        // drop one instance's files. Keep the first occurrence, log+drop the rest.
        let (instances_cfg, dropped) = dedup_instance_ids(instances_cfg);
        for id in &dropped {
            tracing::error!(instance = %id, "duplicate instance id; skipping (ids must be unique)");
        }

        // Cross-cutting backend deps (DESIGN §11.5): the credential service (for a `{"$secret":"…"}`
        // egress) rides in here; each instance threads its own store in `Instance::build`. Building it
        // once and cloning per instance keeps a single shared credential handle.
        let deps = DestDeps::default();
        #[cfg(feature = "dest-s3")]
        let deps = deps.with_credentials(gg.credentials());

        // P3 control/event plane (DESIGN §15-§17). Resolve the shared messaging handle once —
        // absent on some platforms, in which case the whole UNS layer degrades to a no-op and the
        // P1/P2 engine runs unchanged (DESIGN §6) — plus the component-level topic prefix and the
        // envelope thing/tags. Every instance's [`Events`] emitter uses the SAME component-level prefix
        // so an instance's whole namespace — its `cmd` surface, `evt` stream, and retained `state` — is
        // consistent and reachable at one root. A per-instance `topics.prefix` override (§15.7) is
        // parsed but NOT honored in P3 (it would split an instance's command surface from its event
        // stream — see the warning below); it is deferred to a later phase.
        let msg = gg.messaging().ok();
        if msg.is_none() {
            tracing::warn!("messaging unavailable; UNS control/event plane disabled (engine runs normally)");
        }
        let global_prefix = global.topics.as_ref().and_then(|t| t.prefix.clone());
        let thing = self.config.thing_name.clone();
        let tags = self.config.parsed.tags.clone();

        // Build + spawn each instance under a shared cancellation token, keeping a control-plane
        // handle per instance (the same `Arc`, so the dispatcher drives the live instance).
        let cancel = CancellationToken::new();
        let mut handles = Vec::new();
        let mut control_instances: Vec<Arc<dyn InstanceControl>> = Vec::new();
        for cfg in instances_cfg {
            let id = cfg.id.clone();
            // A per-instance `topics.prefix` override is deferred (§15.7): honoring it here would put
            // this instance's events/state on its override root while its `cmd/instances/{id}/…`
            // control surface stays on the component root — a split, half-unreachable namespace. In P3
            // the whole component shares the component prefix; warn and ignore an instance override.
            if cfg.topics.as_ref().and_then(|t| t.prefix.as_deref()).is_some() {
                tracing::warn!(
                    instance = %id,
                    "per-instance topics.prefix override is not honored in P3 (the component shares one \
                     prefix for cmd/evt/state so the namespace stays consistent); ignoring"
                );
            }
            let topics = Arc::new(Topics::from_config(&self.config, global_prefix.as_deref(), None));
            let events = Events::new(msg.clone(), topics, thing.clone(), tags.clone());
            match Instance::build(
                cfg,
                &global,
                store.clone(),
                global_sem.clone(),
                global_bw.clone(),
                Some(self.metrics.clone()),
                &deps,
                events,
            ) {
                Ok(inst) => {
                    tracing::info!(instance = %id, active = inst.is_active(), "instance started");
                    let inst = Arc::new(inst);
                    control_instances.push(inst.clone());
                    let child = cancel.child_token();
                    let run_inst = inst.clone();
                    handles.push(tokio::spawn(async move { run_inst.run(child).await }));
                }
                Err(e) => tracing::error!(instance = %id, error = %e, "failed to build instance; skipping"),
            }
        }

        // FR-CFG-4: skip-bad-instance, but fail fast if NOTHING starts — an all-malformed (or empty)
        // instance set would otherwise launch a component that silently does nothing.
        if handles.is_empty() {
            anyhow::bail!(
                "no replication instances could be started \
                 (component.instances[] is empty, or every entry was malformed or a duplicate id)"
            );
        }

        // P3 control plane (DESIGN §15/§16): subscribe the `cmd/#` topic space on the Unified
        // Namespace and answer get-config / get-status / trigger / set-activation via `reply_to`.
        // Messaging may be absent on some platforms — the plane then degrades to a no-op (DESIGN §6).
        let control = Arc::new(ControlPlane::build(
            msg.clone(),
            &self.config,
            &global,
            store.clone(),
            control_instances,
        ));
        if let Err(e) = control.clone().start().await {
            tracing::warn!(error = %e, "control plane failed to subscribe; continuing without it");
        }

        tracing::info!(
            thing = %self.config.thing_name,
            instances = handles.len(),
            global_slots,
            "file-replicator running (P1 engine: local destination, immediate mode)"
        );
        gg.set_ready(true);

        // Component-ready lifecycle event on the UNS (DESIGN §17.1), on the component-level prefix.
        // No-op when messaging is absent.
        Events::new(
            msg.clone(),
            Arc::new(Topics::from_config(&self.config, global_prefix.as_deref(), None)),
            thing.clone(),
            tags.clone(),
        )
        .component_event(
            Event::ComponentReady {
                instances: handles.len() as u64,
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            now_ms(),
        )
        .await;

        // Seed the retained current-state topics (DESIGN §15.3 / §17.2 / FR-EVT-4) so a UI/cloud
        // subscriber that connects to an idle component still renders every instance's state and the
        // component roster — otherwise an instance that has not ticked yet appears on no state topic.
        // No-op when messaging is absent.
        control.publish_initial_state(now_ms()).await;

        gg.shutdown_signal().await;
        tracing::info!("shutdown signal received; stopping instances");
        // Unsubscribe the control topics first so no command is accepted mid-shutdown (and no broker
        // subscription is orphaned), then cancel the instance run loops.
        control.stop().await;
        cancel.cancel();
        for h in handles {
            if let Err(e) = h.await {
                tracing::warn!(error = %e, "instance task join failed");
            }
        }
        let _ = self.metrics.flush_metrics().await;
        tracing::info!("all instances stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EgressCfg, IngressCfg, LocalEgress, ScheduleCfg};

    fn cfg(id: &str, path: &str) -> InstanceCfg {
        InstanceCfg {
            id: id.to_string(),
            enabled: true,
            ingress: IngressCfg {
                path: path.into(),
                recursive: false,
                include: vec![],
                exclude: vec![],
                rescan_secs: None,
                readiness: Default::default(),
            },
            egress: vec![EgressCfg::Local(LocalEgress {
                path: "/out".into(),
                fsync: false,
            })],
            schedule: ScheduleCfg::Immediate,
            completion: Default::default(),
            retry: None,
            limits: None,
            topics: None,
        }
    }

    #[test]
    fn dedup_keeps_first_and_reports_duplicates() {
        let (kept, dropped) = dedup_instance_ids(vec![
            cfg("a", "/1"),
            cfg("b", "/2"),
            cfg("a", "/3"), // duplicate id, different ingress → dropped
            cfg("a", "/4"), // duplicate again
        ]);
        // First "a" (ingress /1) and "b" survive; the later "a"s are dropped.
        let kept_ids: Vec<&str> = kept.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(kept_ids, vec!["a", "b"]);
        assert_eq!(kept[0].ingress.path, std::path::PathBuf::from("/1"), "first occurrence kept");
        assert_eq!(dropped, vec!["a".to_string(), "a".to_string()]);
    }

    #[test]
    fn dedup_no_duplicates_is_identity() {
        let (kept, dropped) = dedup_instance_ids(vec![cfg("x", "/1"), cfg("y", "/2")]);
        assert_eq!(kept.len(), 2);
        assert!(dropped.is_empty());
    }

    #[test]
    fn dedup_empty_is_empty() {
        let (kept, dropped) = dedup_instance_ids(vec![]);
        assert!(kept.is_empty());
        assert!(dropped.is_empty());
    }
}

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
use crate::instance::Instance;
use crate::ratelimit::{parse_byte_rate, SystemClock, TokenBucket};
use crate::state::{SqliteStore, StateStore};

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

        // Build + spawn each instance under a shared cancellation token.
        let cancel = CancellationToken::new();
        let mut handles = Vec::new();
        for cfg in instances_cfg {
            let id = cfg.id.clone();
            match Instance::build(
                cfg,
                &global,
                store.clone(),
                global_sem.clone(),
                global_bw.clone(),
                Some(self.metrics.clone()),
            ) {
                Ok(inst) => {
                    tracing::info!(instance = %id, active = inst.is_active(), "instance started");
                    let inst = Arc::new(inst);
                    let child = cancel.child_token();
                    handles.push(tokio::spawn(async move { inst.run(child).await }));
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

        tracing::info!(
            thing = %self.config.thing_name,
            instances = handles.len(),
            global_slots,
            "file-replicator running (P1 engine: local destination, immediate mode)"
        );
        gg.set_ready(true);

        gg.shutdown_signal().await;
        tracing::info!("shutdown signal received; stopping instances");
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

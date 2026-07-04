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
use tokio_util::sync::CancellationToken;

use crate::admission::PriorityGate;
use crate::config::{self, InstanceCfg};
use crate::control::{ControlPlane, InstanceControl};
use crate::dest::DestDeps;
use crate::events::{Event, Events};
use crate::instance::{now_ms, Instance};
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

        // Global concurrency + bandwidth caps (aggregate across instances). The concurrency governor is
        // a priority-aware admission gate (Feature B, `src/admission.rs`), not a plain semaphore: under
        // cross-instance contention for this shared cap, a lower-`priority`-number instance is admitted
        // first (FIFO among equal priorities); an idle high-priority instance reserves nothing.
        let global_slots = global
            .limits
            .as_ref()
            .and_then(|l| l.max_concurrent_files)
            .unwrap_or(DEFAULT_GLOBAL_SLOTS)
            .max(1);
        let global_gate = Arc::new(PriorityGate::new(global_slots));
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

        // Control/event plane (DESIGN §15-§17, migrated onto the ggcommons UNS core). Messaging may be
        // absent on some platforms, in which case every `events()`/`commands()` call below degrades to
        // a no-op (or is simply never registered) and the P1/P2 engine runs unchanged (DESIGN §6).
        if gg.messaging().is_err() {
            tracing::warn!("messaging unavailable; UNS control/event plane disabled (engine runs normally)");
        }
        // The component-level ("main" instance) event emitter — `ComponentReady` and the control
        // plane's scope-`"all"` `ScheduleTriggered` (crate::control module docs).
        let main_events = Events::new(gg.events());

        // Build + spawn each instance under a shared cancellation token, keeping a control-plane
        // handle per instance (the same `Arc`, so the dispatcher drives the live instance).
        let cancel = CancellationToken::new();
        let mut handles = Vec::new();
        let mut control_instances: Vec<Arc<dyn InstanceControl>> = Vec::new();
        // Feature A (`src/permission.rs`): instances a startup permission/access violation disabled
        // under the (default) `onPermissionError: disableInstance` policy — surfaced to `get-status`
        // via `ControlPlane::with_disabled` below, so a configured-but-never-started instance is
        // reported with its reason instead of "unknown instance".
        let mut disabled_infos: Vec<crate::control::DisabledInstanceInfo> = Vec::new();
        for cfg in instances_cfg {
            let id = cfg.id.clone();
            // Mint this instance's own bound `events()` facade once, while `gg` is in scope (an owned
            // value — `EventsFacade` does not borrow `GgCommons` — so it can be handed into the
            // long-lived `Instance`/`Worker`; see `crate::events` module docs). A UNS-token-invalid id
            // (forbidden chars — `/ + # \` or control chars) degrades to a disabled emitter rather than
            // aborting the whole component over a naming quirk (DESIGN §6-style graceful degradation).
            let events = match gg.instance(&id) {
                Ok(inst) => Events::new(inst.events()),
                Err(e) => {
                    tracing::error!(instance = %id, error = %e, "invalid UNS instance id; events disabled for this instance");
                    Events::disabled()
                }
            };

            // Feature A startup validation (§2 of the design): probe the instance's ingress/egress/
            // archive/failed directories BEFORE building it, and apply the resolved policy. A violation
            // is ALWAYS logged once (startup is inherently a single occurrence, so no separate dedup
            // state is needed here — the runtime dedup log covers the ongoing "retain" case) and gets a
            // `PermissionDenied` event, regardless of what the policy then does with it.
            let policy = config::resolve_permission_policy(&cfg, &global);
            let violations = crate::permission::validate_instance(&cfg);
            for v in &violations {
                tracing::error!(
                    instance = %id, path = %v.path.display(), role = v.role.as_str(), error = %v.msg,
                    "startup permission/access check failed"
                );
                events
                    .emit(Event::PermissionDenied {
                        path: v.path.display().to_string(),
                        role: v.role.as_str().to_string(),
                        error: v.msg.clone(),
                    })
                    .await;
            }
            match crate::permission::apply_policy(policy, &violations) {
                crate::permission::PolicyOutcome::Fatal => {
                    // Earlier, viable instances in this loop were already `tokio::spawn`ed. `fatal` aborts
                    // partway through, so cancel + drain them before returning the error — otherwise, if
                    // `App::run`'s error is ever handled without an immediate process exit, those instance
                    // tasks would keep running orphaned (with the process exiting the runtime tears them
                    // down anyway, but we don't rely on that).
                    cancel.cancel();
                    for h in handles {
                        let _ = h.await;
                    }
                    anyhow::bail!(
                        "instance '{id}': startup permission check failed and onPermissionError=fatal \
                         ({} violation(s)); aborting the component",
                        violations.len()
                    );
                }
                crate::permission::PolicyOutcome::Skip => {
                    let first = &violations[0];
                    tracing::error!(
                        instance = %id,
                        "instance disabled by onPermissionError=disableInstance (default); \
                         see the PermissionDenied event(s) above for every violation"
                    );
                    disabled_infos.push(crate::control::DisabledInstanceInfo {
                        id: id.clone(),
                        reason: violations
                            .iter()
                            .map(|v| format!("{}: {} ({})", v.role.as_str(), v.path.display(), v.msg))
                            .collect::<Vec<_>>()
                            .join("; "),
                        role: first.role.as_str().to_string(),
                        path: first.path.display().to_string(),
                    });
                    continue;
                }
                // `Start`: either no violations, or the policy is `retain` — build the instance
                // normally. A `retain` instance leans on its own runtime `PermissionLog` (Instance/
                // Worker) for ongoing dedup-logging; nothing further to do here.
                crate::permission::PolicyOutcome::Start => {}
            }

            match Instance::build(
                cfg,
                &global,
                store.clone(),
                global_gate.clone(),
                global_bw.clone(),
                Some(self.metrics.clone()),
                &deps,
                events,
            ) {
                Ok(inst) => {
                    tracing::info!(instance = %id, active = inst.is_active(), "instance started");
                    let inst = Arc::new(inst);
                    // Feature A: this instance started despite startup violation(s) only under `retain`
                    // (otherwise it would have been skipped/aborted above). The startup loop already
                    // emitted one `PermissionDenied` per violation; seed the runtime ingress dedup-log so
                    // the first rescan does not emit a duplicate for the same (path, kind) — preserving
                    // "emitted once per distinct (path, error kind)".
                    if !violations.is_empty() {
                        inst.seed_permission_log(&violations, now_ms());
                    }
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

        // Control plane (DESIGN §15/§16, migrated onto `gg.commands()`): registers `get-status` /
        // `trigger` / `set-activation` on the component command inbox — the built-in `ping` /
        // `reload-config` / `get-configuration` verbs answer everything the old custom `cmd/config`
        // verb used to (see `crate::control` module docs). No-op when no messaging transport was wired
        // (`gg.commands()` is then `None` — DESIGN §6).
        let control = Arc::new(
            ControlPlane::new(self.config.clone(), store.clone(), control_instances, main_events.clone())
                .with_disabled(disabled_infos),
        );
        if let Some(commands) = gg.commands() {
            control.clone().register(&commands);
        } else {
            tracing::warn!("no command inbox available; control plane verbs not registered");
        }

        tracing::info!(
            thing = %self.config.thing_name,
            instances = handles.len(),
            global_slots,
            "file-replicator running (P1 engine: local destination, immediate mode)"
        );
        gg.set_ready(true);

        // Component-ready lifecycle event (DESIGN §17.1), on the component-level ("main" instance)
        // emitter. No-op when messaging is absent.
        main_events
            .emit(Event::ComponentReady {
                instances: handles.len() as u64,
                version: env!("CARGO_PKG_VERSION").to_string(),
            })
            .await;

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
            on_permission_error: None,
            priority: 100,
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

    // ---- Feature A: the same startup-validation composition `App::run`'s per-instance loop uses -----
    //
    // `App::run` itself needs a full `GgCommons` harness to unit-test (this crate's convention — see
    // the integration tests under `tests/`, none of which drive `App::run` directly). These tests
    // instead exercise the EXACT decision pipeline that loop applies to each `cfg` — resolve the policy,
    // validate, apply the policy — with real tempdirs, so `disableInstance`/`fatal`/`retain` and the
    // "every instance skipped → nothing would start" invariant that feeds the existing `handles.is_empty
    // ()` bail (unchanged by this feature) are all verified end-to-end.

    use crate::config::PermissionPolicy;
    use crate::permission::{self, PolicyOutcome};

    fn bad_ingress_cfg(id: &str, policy: Option<PermissionPolicy>) -> InstanceCfg {
        // A FILE where the ingress dir is expected makes startup validation fail deterministically.
        let dir = tempfile::tempdir().unwrap();
        let not_a_dir = dir.path().join("f.txt");
        std::fs::write(&not_a_dir, b"x").unwrap();
        std::mem::forget(dir); // keep the tempdir alive for the duration of the test
        let mut c = cfg(id, not_a_dir.to_str().unwrap());
        c.on_permission_error = policy;
        c
    }

    fn outcome_for(cfg: &InstanceCfg, global: &config::GlobalCfg) -> PolicyOutcome {
        let policy = config::resolve_permission_policy(cfg, global);
        let violations = permission::validate_instance(cfg);
        permission::apply_policy(policy, &violations)
    }

    #[test]
    fn startup_policy_disable_instance_skips_just_that_instance() {
        let bad = bad_ingress_cfg("bad", None); // inherits the component default
        let global = config::GlobalCfg::default(); // disableInstance
        assert_eq!(outcome_for(&bad, &global), PolicyOutcome::Skip);
    }

    #[test]
    fn startup_policy_fatal_would_abort_the_whole_component() {
        let bad = bad_ingress_cfg("bad", Some(PermissionPolicy::Fatal));
        let global = config::GlobalCfg::default();
        assert_eq!(outcome_for(&bad, &global), PolicyOutcome::Fatal);
    }

    #[test]
    fn startup_policy_retain_starts_the_instance_anyway() {
        let bad = bad_ingress_cfg("bad", Some(PermissionPolicy::Retain));
        let global = config::GlobalCfg::default();
        assert_eq!(outcome_for(&bad, &global), PolicyOutcome::Start);
    }

    #[test]
    fn startup_policy_clean_instance_always_starts_regardless_of_policy() {
        // Real, per-test-unique tempdirs for BOTH ingress and egress — the shared `cfg()` helper's
        // hardcoded `/out` egress path is fine for the other tests here (they only care that ANY
        // violation exists), but this test needs a genuinely clean instance, and a literal path shared
        // across parallel tests doing real create/write/remove probes is itself a race.
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let mut clean = cfg("ok", src.path().to_str().unwrap());
        clean.egress = vec![crate::config::EgressCfg::Local(crate::config::LocalEgress {
            path: dst.path().to_path_buf(),
            fsync: false,
        })];
        for policy in [
            PermissionPolicy::DisableInstance,
            PermissionPolicy::Fatal,
            PermissionPolicy::Retain,
        ] {
            clean.on_permission_error = Some(policy);
            assert_eq!(outcome_for(&clean, &config::GlobalCfg::default()), PolicyOutcome::Start);
        }
    }

    #[test]
    fn startup_policy_zero_viable_instances_is_what_feeds_the_existing_bail() {
        // Every instance in the set resolves to `Skip` under the default policy → the count of `Start`
        // outcomes is zero, which is exactly the condition `App::run`'s pre-existing `handles.is_empty()`
        // guard (unchanged by this feature) turns into a process-fatal error (FR-CFG-4's "fail only if
        // zero instances start" rule, now also reachable via an all-inaccessible instance set).
        let global = config::GlobalCfg::default();
        let cfgs = [bad_ingress_cfg("a", None), bad_ingress_cfg("b", None)];
        let started = cfgs
            .iter()
            .filter(|c| outcome_for(c, &global) == PolicyOutcome::Start)
            .count();
        assert_eq!(started, 0, "an all-inaccessible instance set starts nothing");
    }

    #[test]
    fn instance_level_override_wins_over_a_fatal_global_default() {
        // A per-instance `retain` override still starts even when the component default is `fatal` —
        // proving instance-scoped isolation: one bad/overridden instance's policy never escalates to
        // the whole component unless ITS OWN resolved policy says so.
        let bad = bad_ingress_cfg("bad", Some(PermissionPolicy::Retain));
        let global = config::GlobalCfg {
            on_permission_error: PermissionPolicy::Fatal,
            ..Default::default()
        };
        assert_eq!(outcome_for(&bad, &global), PolicyOutcome::Start);
    }
}

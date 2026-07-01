//! # file-replicator — application wiring
//!
//! P0 scaffold: holds the `ggcommons` service handles, parses the instance configuration (see
//! [`crate::config`]), registers a configuration-change listener, and runs an empty engine until
//! shutdown. The per-instance replication engine (watcher → queue → workers → destinations) is added
//! in P1+ per `DESIGN.md` §6/§21; for now [`App::run`] validates that instances parse and awaits the
//! shutdown signal so the component runs cleanly on HOST/GREENGRASS/KUBERNETES.

use std::sync::Arc;

use ggcommons::prelude::*;

use crate::config::{self, InstanceCfg};

/// The component's service handles and parsed configuration.
pub struct App {
    config: Arc<Config>,
    metrics: Arc<dyn MetricService>,
    /// `Some` when a messaging transport is available for the resolved platform
    /// (HOST/MQTT always; GREENGRASS/IPC with the `greengrass` feature).
    messaging: Option<Arc<dyn MessagingService>>,
    /// Parsed watched-directory instances (`component.instances[]`). Malformed instances are logged
    /// and skipped at parse time (FR-CFG-4); an empty list is tolerated at P0.
    instances: Vec<InstanceCfg>,
}

/// A [`ConfigurationChangeListener`] invoked when the component configuration is hot-reloaded.
/// P0 logs the change; per-instance re-application (drain/restart the changed instance) lands with
/// the engine in P1+ (FR-CFG-6).
struct ConfigListener;

#[async_trait::async_trait]
impl ConfigurationChangeListener for ConfigListener {
    async fn on_configuration_change(&self, config: Arc<Config>) -> bool {
        let n = config::load_instances(&config).len();
        tracing::info!(thing = %config.thing_name, instances = n, "configuration changed");
        true
    }
}

impl App {
    /// Build the app from an initialized [`ggcommons::GgCommons`] runtime: capture the service
    /// handles, parse the instances, and register for config hot-reload.
    pub fn new(gg: &GgCommons) -> anyhow::Result<Self> {
        gg.add_config_change_listener(Arc::new(ConfigListener));

        let config = gg.config();
        let instances = config::load_instances(&config);

        Ok(Self {
            config,
            metrics: gg.metrics(),
            messaging: gg.messaging().ok(),
            instances,
        })
    }

    /// Run until a shutdown signal (Ctrl-C / SIGTERM). The library owns signal handling — await
    /// [`GgCommons::shutdown_signal`] rather than re-implementing `tokio::signal`. Dropping the
    /// `GgCommons` runtime after this returns releases all resources (RAII).
    pub async fn run(&self, gg: &GgCommons) -> anyhow::Result<()> {
        tracing::info!(
            thing = %self.config.thing_name,
            instances = self.instances.len(),
            "file-replicator running (P0 scaffold — replication engine lands in P1)"
        );
        for inst in &self.instances {
            tracing::info!(
                instance = %inst.id,
                enabled = inst.enabled,
                recursive = inst.ingress.recursive,
                egress = inst.egress.len(),
                "instance loaded"
            );
        }

        // Touch the handles so the scaffold compiles warning-free; the engine wires them in P1+.
        let _ = (&self.metrics, &self.messaging);

        gg.shutdown_signal().await;
        tracing::info!("shutdown signal received; exiting");
        Ok(())
    }
}

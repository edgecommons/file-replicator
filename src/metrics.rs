//! File Replicator metric groups and emission helpers.

use std::collections::HashMap;
use std::sync::Arc;

use edgecommons::metrics::{MetricBuilder, MetricService};
use edgecommons::prelude::Config;

const STORAGE_RESOLUTION: u32 = 60;

pub const LEGACY_GROUP: &str = "fileReplicator";
pub const DISCOVERY_GROUP: &str = "FileReplicatorDiscovery";
pub const QUEUE_GROUP: &str = "FileReplicatorQueue";
pub const TRANSFER_GROUP: &str = "FileReplicatorTransfer";
pub const DESTINATION_GROUP: &str = "FileReplicatorDestination";
pub const SCHEDULE_GROUP: &str = "FileReplicatorSchedule";

#[derive(Clone)]
pub struct ReplicatorMetrics {
    inner: Arc<Inner>,
}

struct Inner {
    service: Arc<dyn MetricService>,
    config: Option<Arc<Config>>,
    emit_lock: tokio::sync::Mutex<()>,
}

impl ReplicatorMetrics {
    pub fn new(service: Arc<dyn MetricService>, config: Arc<Config>) -> Self {
        Self {
            inner: Arc::new(Inner {
                service,
                config: Some(config),
                emit_lock: tokio::sync::Mutex::new(()),
            }),
        }
    }

    #[cfg(test)]
    pub fn without_config(service: Arc<dyn MetricService>) -> Self {
        Self {
            inner: Arc::new(Inner {
                service,
                config: None,
                emit_lock: tokio::sync::Mutex::new(()),
            }),
        }
    }

    pub fn define_legacy(&self) {
        self.inner
            .service
            .define_metric(legacy_builder(self.inner.config.as_deref()).build());
    }

    pub fn define_static_groups(&self) {
        self.inner.service.define_metric(
            discovery_builder(self.inner.config.as_deref(), "unknown", "unknown").build(),
        );
        self.inner
            .service
            .define_metric(queue_builder(self.inner.config.as_deref(), "unknown").build());
        self.inner.service.define_metric(
            transfer_builder(
                self.inner.config.as_deref(),
                "unknown",
                "unknown",
                "unknown",
            )
            .build(),
        );
        self.inner.service.define_metric(
            destination_builder(self.inner.config.as_deref(), "unknown", "unknown").build(),
        );
        self.inner.service.define_metric(
            schedule_builder(self.inner.config.as_deref(), "unknown", "unknown").build(),
        );
    }

    pub async fn emit_legacy(&self, values: MetricValues) {
        let _guard = self.inner.emit_lock.lock().await;
        self.inner
            .service
            .define_metric(legacy_builder(self.inner.config.as_deref()).build());
        let _ = self
            .inner
            .service
            .emit_metric(LEGACY_GROUP, values.into_map())
            .await;
    }

    pub async fn emit_discovery(
        &self,
        instance: &str,
        readiness_strategy: &str,
        values: MetricValues,
    ) {
        let _guard = self.inner.emit_lock.lock().await;
        self.inner.service.define_metric(
            discovery_builder(self.inner.config.as_deref(), instance, readiness_strategy).build(),
        );
        let _ = self
            .inner
            .service
            .emit_metric(DISCOVERY_GROUP, values.into_map())
            .await;
    }

    pub async fn emit_queue(&self, instance: &str, values: MetricValues) {
        let _guard = self.inner.emit_lock.lock().await;
        self.inner
            .service
            .define_metric(queue_builder(self.inner.config.as_deref(), instance).build());
        let _ = self
            .inner
            .service
            .emit_metric(QUEUE_GROUP, values.into_map())
            .await;
    }

    pub async fn emit_transfer(
        &self,
        instance: &str,
        destination_type: &str,
        result: &str,
        values: MetricValues,
    ) {
        let _guard = self.inner.emit_lock.lock().await;
        self.inner.service.define_metric(
            transfer_builder(
                self.inner.config.as_deref(),
                instance,
                destination_type,
                result,
            )
            .build(),
        );
        let _ = self
            .inner
            .service
            .emit_metric(TRANSFER_GROUP, values.into_map())
            .await;
    }

    pub async fn emit_destination(
        &self,
        instance: &str,
        destination_type: &str,
        values: MetricValues,
    ) {
        let _guard = self.inner.emit_lock.lock().await;
        self.inner.service.define_metric(
            destination_builder(self.inner.config.as_deref(), instance, destination_type).build(),
        );
        let _ = self
            .inner
            .service
            .emit_metric(DESTINATION_GROUP, values.into_map())
            .await;
    }

    pub async fn emit_schedule(&self, instance: &str, mode: &str, values: MetricValues) {
        let _guard = self.inner.emit_lock.lock().await;
        self.inner
            .service
            .define_metric(schedule_builder(self.inner.config.as_deref(), instance, mode).build());
        let _ = self
            .inner
            .service
            .emit_metric(SCHEDULE_GROUP, values.into_map())
            .await;
    }
}

#[derive(Default)]
pub struct MetricValues(HashMap<String, f64>);

impl MetricValues {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(mut self, name: impl Into<String>, value: impl Into<f64>) -> Self {
        self.0.insert(name.into(), value.into());
        self
    }

    fn into_map(self) -> HashMap<String, f64> {
        self.0
    }
}

fn configured_builder(name: &str, config: Option<&Config>) -> MetricBuilder {
    let builder = MetricBuilder::create(name);
    match config {
        Some(config) => builder.with_config(config),
        None => builder,
    }
}

fn legacy_builder(config: Option<&Config>) -> MetricBuilder {
    configured_builder(LEGACY_GROUP, config)
        .add_measure("filesReplicated", "Count", STORAGE_RESOLUTION)
        .add_measure("bytesReplicated", "Bytes", STORAGE_RESOLUTION)
        .add_measure("filesFailed", "Count", STORAGE_RESOLUTION)
        .add_measure("filesReplicatedInterval", "Count", STORAGE_RESOLUTION)
        .add_measure("bytesReplicatedInterval", "Bytes", STORAGE_RESOLUTION)
        .add_measure("filesFailedInterval", "Count", STORAGE_RESOLUTION)
}

fn discovery_builder(
    config: Option<&Config>,
    instance: &str,
    readiness_strategy: &str,
) -> MetricBuilder {
    configured_builder(DISCOVERY_GROUP, config)
        .add_dimension("instance", instance)
        .add_dimension("readinessStrategy", readiness_strategy)
        .add_measure("scanCount", "Count", STORAGE_RESOLUTION)
        .add_measure("scanDurationMs", "Milliseconds", STORAGE_RESOLUTION)
        .add_measure("filesDiscovered", "Count", STORAGE_RESOLUTION)
        .add_measure("filesReady", "Count", STORAGE_RESOLUTION)
        .add_measure("filesIgnored", "Count", STORAGE_RESOLUTION)
        .add_measure("scanErrors", "Count", STORAGE_RESOLUTION)
        .add_measure("permissionDenied", "Count", STORAGE_RESOLUTION)
}

fn queue_builder(config: Option<&Config>, instance: &str) -> MetricBuilder {
    configured_builder(QUEUE_GROUP, config)
        .add_dimension("instance", instance)
        .add_measure("queueDepthReady", "Count", STORAGE_RESOLUTION)
        .add_measure("queueDepthInProgress", "Count", STORAGE_RESOLUTION)
        .add_measure("queueDepthFailed", "Count", STORAGE_RESOLUTION)
        .add_measure("queueDepthExhausted", "Count", STORAGE_RESOLUTION)
        .add_measure("oldestQueuedAgeMs", "Milliseconds", STORAGE_RESOLUTION)
        .add_measure("bytesQueued", "Bytes", STORAGE_RESOLUTION)
        .add_measure("retryBacklog", "Count", STORAGE_RESOLUTION)
        .add_measure("activeWorkers", "Count", STORAGE_RESOLUTION)
}

fn transfer_builder(
    config: Option<&Config>,
    instance: &str,
    destination_type: &str,
    result: &str,
) -> MetricBuilder {
    configured_builder(TRANSFER_GROUP, config)
        .add_dimension("instance", instance)
        .add_dimension("destinationType", destination_type)
        .add_dimension("result", result)
        .add_measure("filesStarted", "Count", STORAGE_RESOLUTION)
        .add_measure("filesReplicated", "Count", STORAGE_RESOLUTION)
        .add_measure("filesFailed", "Count", STORAGE_RESOLUTION)
        .add_measure("filesQuarantined", "Count", STORAGE_RESOLUTION)
        .add_measure("filesRetained", "Count", STORAGE_RESOLUTION)
        .add_measure("bytesReplicated", "Bytes", STORAGE_RESOLUTION)
        .add_measure("transferDurationMs", "Milliseconds", STORAGE_RESOLUTION)
        .add_measure("throughputBytesPerSec", "Bytes/Second", STORAGE_RESOLUTION)
        .add_measure("retryAttempts", "Count", STORAGE_RESOLUTION)
        .add_measure("verificationFailures", "Count", STORAGE_RESOLUTION)
        .add_measure("resumeRecoveries", "Count", STORAGE_RESOLUTION)
}

fn destination_builder(
    config: Option<&Config>,
    instance: &str,
    destination_type: &str,
) -> MetricBuilder {
    configured_builder(DESTINATION_GROUP, config)
        .add_dimension("instance", instance)
        .add_dimension("destinationType", destination_type)
        .add_measure("linkConnected", "Count", STORAGE_RESOLUTION)
        .add_measure("connectFailures", "Count", STORAGE_RESOLUTION)
        .add_measure("authFailures", "Count", STORAGE_RESOLUTION)
        .add_measure("writeFailures", "Count", STORAGE_RESOLUTION)
        .add_measure("throttleDelayMs", "Milliseconds", STORAGE_RESOLUTION)
        .add_measure(
            "bandwidthLimitBytesPerSec",
            "Bytes/Second",
            STORAGE_RESOLUTION,
        )
}

fn schedule_builder(config: Option<&Config>, instance: &str, mode: &str) -> MetricBuilder {
    configured_builder(SCHEDULE_GROUP, config)
        .add_dimension("instance", instance)
        .add_dimension("mode", mode)
        .add_measure("instanceActive", "Count", STORAGE_RESOLUTION)
        .add_measure("windowOpen", "Count", STORAGE_RESOLUTION)
        .add_measure("scheduleTriggers", "Count", STORAGE_RESOLUTION)
        .add_measure("scheduleSkipped", "Count", STORAGE_RESOLUTION)
        .add_measure("admissionBlocked", "Count", STORAGE_RESOLUTION)
        .add_measure("filesReleased", "Count", STORAGE_RESOLUTION)
}

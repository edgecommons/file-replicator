//! # file-replicator — entry point
//!
//! An EdgeCommons **sink** component (AWS IoT Greengrass v2 / HOST / Kubernetes) built on the
//! `edgecommons` Rust library. It watches local directories and replicates files to configured
//! destinations. It initializes the runtime from the standard CLI contract
//! (`-c`/`--platform`/`--transport`/`-t`), loads + parses the instance configuration, then hands
//! control to [`app::App`], which builds one replication [`instance::Instance`] per
//! `component.instances[]` and runs the P1 engine (watch → durable queue → deliver → verify →
//! complete, local destination, immediate mode) until shutdown. S3 egress, control/events, and
//! cron/window scheduling land in later phases (see `DESIGN.md` §21).
//!
//! ## Running locally (HOST platform, MQTT transport, against a local MQTT broker)
//! ```bash
//! cargo run -- \
//!   --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
//!   -c FILE ./test-configs/config.json \
//!   -t my-thing
//! ```

use file_replicator::app;
use edgecommons::prelude::*;

/// The component's full name (matches `recipe.yaml` / `gdk-config.json`).
const COMPONENT_NAME: &str = "com.mbreissi.edgecommons.FileReplicator";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let gg = EdgeCommonsBuilder::new(COMPONENT_NAME)
        .args(std::env::args_os())
        .build()
        .await?;

    tracing::info!(
        component = gg.component_name(),
        thing = %gg.config().thing_name,
        "file-replicator starting"
    );

    let app = app::App::new(&gg)?;
    app.run(&gg).await?;

    tracing::info!("file-replicator stopped");
    Ok(())
}

//! # file-replicator — entry point
//!
//! An EdgeCommons **sink** component (AWS IoT Greengrass v2 / HOST / Kubernetes) built on the
//! `ggcommons` Rust library. It watches local directories and replicates files to configured
//! destinations. This is the P0 scaffold: it initializes the runtime from the standard CLI contract
//! (`-c`/`--platform`/`--transport`/`-t`), loads + parses the instance configuration, then hands
//! control to [`app::App`], which currently runs an empty engine until shutdown. The replication
//! engine lands in P1+ (see `DESIGN.md` §21).
//!
//! ## Running locally (HOST platform, MQTT transport, against a local MQTT broker)
//! ```bash
//! cargo run -- \
//!   --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
//!   -c FILE ./test-configs/config.json \
//!   -t my-thing
//! ```

mod app;
mod config;

use ggcommons::prelude::*;

/// The component's full name (matches `recipe.yaml` / `gdk-config.json`).
const COMPONENT_NAME: &str = "com.mbreissi.greengrass.FileReplicator";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let gg = GgCommonsBuilder::new(COMPONENT_NAME)
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

//! # file-replicator — Unified Namespace (topic builder + budget guard)
//!
//! **One-liner purpose**: Build every `cmd`/`evt`/`state` topic the component uses on the Unified
//! Namespace, resolve the configurable prefix through the ggcommons template resolver, and enforce
//! AWS IoT Core's topic limits.
//!
//! ## Scheme (DESIGN §15.2)
//! ```text
//! {thing}/{component}/{class}/{resource…}
//! ```
//! - `{thing}` — the ggcommons ThingName (globally unique per account/region → collision-free).
//! - `{component}` — the short registry slug [`COMPONENT_SLUG`] (`file-replicator`), **not** the
//!   38-char reverse-DNS full name — it saves bytes and reads better.
//! - `{class}` ∈ `cmd` (inbound commands, request/reply) · `evt` (event stream, non-retained) ·
//!   `state` (current snapshot — see the retain gap in [`crate::events`]).
//!
//! The prefix defaults to `{ThingName}/file-replicator` and is resolved via
//! [`ggcommons::config::template::resolve`] (which substitutes `{ThingName}`/tag tokens and
//! sanitizes the injected values against `/ \ + #` / traversal). It is overridable per component
//! (`component.global.topics.prefix`) and per instance (`instances[].topics.prefix`); a per-instance
//! override replaces the whole `{thing}/file-replicator` root (§15.7).
//!
//! ## Budget (DESIGN §15.4 / §15.1)
//! Every builder — including the control-plane subscribe filter [`Topics::cmd_filter`] and the free
//! [`legacy_config_topic`] — passes its result through an internal guard: a topic must be ≤ 256 UTF-8
//! bytes, ≤ 7 forward slashes, and have no level beginning with `$` (reserved). A violation is logged
//! at `warn!` and the topic is still returned (a pathological ThingName/override prefix must not crash
//! the engine — at worst the broker rejects the publish/subscribe). The pure [`within_budget`] and
//! [`has_reserved_level`] predicates are exposed for direct testing.
//!
//! ## Purity
//! This module is pure (no I/O, no async): topic strings in, topic strings out, plus the
//! [`parse_cmd`] router. It is exhaustively unit-tested.

use ggcommons::config::template;
use ggcommons::prelude::Config;

/// The short registry slug used as the `{component}` path segment (DESIGN §15.2) — deliberately
/// *not* the reverse-DNS full name (`com.mbreissi.greengrass.FileReplicator`).
pub const COMPONENT_SLUG: &str = "file-replicator";

/// AWS IoT Core maximum topic length in UTF-8 bytes (DESIGN §15.1).
pub const IOT_CORE_MAX_BYTES: usize = 256;

/// AWS IoT Core maximum number of forward slashes in a topic (8 levels, DESIGN §15.1).
pub const IOT_CORE_MAX_SLASHES: usize = 7;

/// Default topic-prefix template when neither the instance nor `component.global` overrides it.
pub const DEFAULT_PREFIX_TEMPLATE: &str = "{ThingName}/file-replicator";

/// Legacy core `GetConfiguration` request topic template (DESIGN §15.6, opt-in via
/// `legacyConfigTopic`). `{ComponentName}` resolves to the SHORT component name (`FileReplicator`).
const LEGACY_CONFIG_TEMPLATE: &str = "ggcommons/{ThingName}/config/get/{ComponentName}";

/// Whether `topic` fits inside AWS IoT Core's limits (≤ 256 bytes AND ≤ 7 slashes, DESIGN §15.4).
///
/// Pure predicate, exposed for direct budget-conformance tests. The third §15.1 constraint — a level
/// MUST NOT start with `$` (reserved) — is checked separately by [`has_reserved_level`] (it is a
/// structural, not a size, rule).
pub fn within_budget(topic: &str) -> bool {
    topic.len() <= IOT_CORE_MAX_BYTES
        && topic.bytes().filter(|b| *b == b'/').count() <= IOT_CORE_MAX_SLASHES
}

/// Whether any level of `topic` begins with `$` — reserved by AWS IoT Core (DESIGN §15.1); such a
/// topic is rejected by the broker. The template resolver's `sanitize` neutralizes `/ \ + #` and
/// traversal in interpolated values but NOT a leading `$`, so a `-t '$x'` ThingName / tag can still
/// produce one. Pure predicate, exposed for direct conformance tests.
pub fn has_reserved_level(topic: &str) -> bool {
    topic.split('/').any(|level| level.starts_with('$'))
}

/// The resolved, sanitized topic prefix for one prefix domain (component-global or a per-instance
/// override), plus the builders for every `cmd`/`evt`/`state` topic beneath it.
#[derive(Debug, Clone)]
pub struct Topics {
    prefix: String,
}

impl Topics {
    /// Resolve the prefix by precedence (§15.7): per-instance `topics.prefix` ▸
    /// `component.global.topics.prefix` ▸ default [`DEFAULT_PREFIX_TEMPLATE`]. The chosen template is
    /// expanded through the ggcommons resolver (`{ThingName}`/tags substituted + sanitized).
    pub fn from_config(
        cfg: &Config,
        global_prefix: Option<&str>,
        instance_prefix: Option<&str>,
    ) -> Self {
        let tpl = instance_prefix
            .or(global_prefix)
            .unwrap_or(DEFAULT_PREFIX_TEMPLATE);
        Self {
            prefix: template::resolve(cfg, tpl),
        }
    }

    /// Construct directly from an already-resolved prefix (tests, disabled emitters). The prefix is
    /// stored verbatim — no further template resolution.
    pub fn from_prefix(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    /// The resolved prefix (`{thing}/file-replicator` by default).
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Warn if `topic` breaks the IoT Core budget (≤256 bytes / ≤7 slashes) or has a `$`-leading level
    /// (reserved), then return it unchanged. Never panics — a too-long/hostile ThingName or override
    /// prefix must not stop the engine (at worst the broker rejects the publish/subscribe).
    fn guard(&self, topic: String) -> String {
        guard_topic(&topic);
        topic
    }

    // ---- command topics (inbound; request/reply) ------------------------------------------------

    /// The single subscribe filter that covers every command resource: `{prefix}/cmd/#`. Guarded —
    /// an over-long override prefix that pushes this filter past the byte budget would otherwise be
    /// rejected by the broker with no warning, silently disabling the entire control plane.
    pub fn cmd_filter(&self) -> String {
        self.guard(format!("{}/cmd/#", self.prefix))
    }

    /// `{prefix}/cmd/config` — get-config.
    pub fn cmd_config(&self) -> String {
        self.guard(format!("{}/cmd/config", self.prefix))
    }

    /// `{prefix}/cmd/status` — get-status (all instances).
    pub fn cmd_status(&self) -> String {
        self.guard(format!("{}/cmd/status", self.prefix))
    }

    /// `{prefix}/cmd/trigger` — trigger (all instances).
    pub fn cmd_trigger(&self) -> String {
        self.guard(format!("{}/cmd/trigger", self.prefix))
    }

    /// `{prefix}/cmd/instances/{id}/status` — get-status (one instance).
    pub fn cmd_instance_status(&self, id: &str) -> String {
        self.guard(format!("{}/cmd/instances/{id}/status", self.prefix))
    }

    /// `{prefix}/cmd/instances/{id}/trigger` — trigger (one instance).
    pub fn cmd_instance_trigger(&self, id: &str) -> String {
        self.guard(format!("{}/cmd/instances/{id}/trigger", self.prefix))
    }

    /// `{prefix}/cmd/instances/{id}/activation` — set-activation (one instance).
    pub fn cmd_instance_activation(&self, id: &str) -> String {
        self.guard(format!("{}/cmd/instances/{id}/activation", self.prefix))
    }

    // ---- event topics (outbound; non-retained) --------------------------------------------------

    /// `{prefix}/evt/{event}` — a component-level event (e.g. `ComponentReady`).
    pub fn evt_component(&self, event: &str) -> String {
        self.guard(format!("{}/evt/{event}", self.prefix))
    }

    /// `{prefix}/evt/instances/{id}/{event}` — a per-instance event.
    pub fn evt_instance(&self, id: &str, event: &str) -> String {
        self.guard(format!("{}/evt/instances/{id}/{event}", self.prefix))
    }

    // ---- state topics (outbound; current snapshot — retain gap in `events`) ---------------------

    /// `{prefix}/state` — component current-state snapshot.
    pub fn state_component(&self) -> String {
        self.guard(format!("{}/state", self.prefix))
    }

    /// `{prefix}/state/instances/{id}` — per-instance current-state snapshot.
    pub fn state_instance(&self, id: &str) -> String {
        self.guard(format!("{}/state/instances/{id}", self.prefix))
    }
}

/// The core legacy `GetConfiguration` request topic (DESIGN §15.6), resolved for `cfg`
/// (`ggcommons/{ThingName}/config/get/FileReplicator`). Opt-in via `legacyConfigTopic`. Guarded like
/// every other topic (a pathological ThingName warns but never crashes the subscribe).
pub fn legacy_config_topic(cfg: &Config) -> String {
    let topic = template::resolve(cfg, LEGACY_CONFIG_TEMPLATE);
    guard_topic(&topic);
    topic
}

/// Warn (never panic) if `topic` breaks the AWS IoT Core budget (≤256 bytes / ≤7 slashes) or has a
/// `$`-leading level (reserved). Shared by [`Topics::guard`] and [`legacy_config_topic`] so every
/// built/subscribed topic — the `cmd/#` filter included — is checked against the same rules.
fn guard_topic(topic: &str) {
    if !within_budget(topic) {
        tracing::warn!(
            topic = %topic,
            bytes = topic.len(),
            slashes = topic.bytes().filter(|b| *b == b'/').count(),
            "UNS topic exceeds the AWS IoT Core budget (≤256 bytes / ≤7 slashes); the broker may reject it"
        );
    }
    if has_reserved_level(topic) {
        tracing::warn!(
            topic = %topic,
            "UNS topic has a level beginning with '$' (reserved by AWS IoT Core); the broker may reject it"
        );
    }
}

/// The scope of a scoped command (all instances vs one instance).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    /// The command applies to every instance.
    All,
    /// The command applies to the named instance only.
    Instance(String),
}

/// A parsed inbound control command (DESIGN §16). Produced by [`parse_cmd`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// `cmd/config` — return the effective configuration document.
    GetConfig,
    /// `cmd/status` (all) or `cmd/instances/{id}/status` (one).
    GetStatus(Scope),
    /// `cmd/trigger` (all) or `cmd/instances/{id}/trigger` (one).
    Trigger(Scope),
    /// `cmd/instances/{id}/activation` — activate/deactivate/reset the named instance.
    SetActivation(String),
}

/// Route an inbound command `topic` (received on [`Topics::cmd_filter`]) to a [`Command`].
///
/// Returns `None` for any topic that is not under `{prefix}/cmd/` or whose resource suffix is
/// unrecognized (the caller logs and sends no reply). Pure and exhaustively unit-tested.
pub fn parse_cmd(prefix: &str, topic: &str) -> Option<Command> {
    let after = topic.strip_prefix(prefix)?;
    let resource = after.strip_prefix("/cmd/")?;
    match resource {
        "config" => Some(Command::GetConfig),
        "status" => Some(Command::GetStatus(Scope::All)),
        "trigger" => Some(Command::Trigger(Scope::All)),
        other => {
            let inner = other.strip_prefix("instances/")?;
            let (id, verb) = inner.rsplit_once('/')?;
            if id.is_empty() {
                return None;
            }
            match verb {
                "status" => Some(Command::GetStatus(Scope::Instance(id.to_string()))),
                "trigger" => Some(Command::Trigger(Scope::Instance(id.to_string()))),
                "activation" => Some(Command::SetActivation(id.to_string())),
                _ => None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cfg(thing: &str) -> Config {
        Config::from_value("com.mbreissi.greengrass.FileReplicator", thing, json!({})).unwrap()
    }

    fn cfg_with_tags(thing: &str, tags: serde_json::Value) -> Config {
        Config::from_value(
            "com.mbreissi.greengrass.FileReplicator",
            thing,
            json!({ "tags": tags }),
        )
        .unwrap()
    }

    #[test]
    fn default_prefix_resolves_thing_and_keeps_literal_slug() {
        let t = Topics::from_config(&cfg("gw-01"), None, None);
        assert_eq!(t.prefix(), "gw-01/file-replicator");
    }

    #[test]
    fn default_prefix_resolves_a_tag_token() {
        // A prefix template may reference a config tag; the resolver substitutes + sanitizes it.
        let t = Topics::from_config(
            &cfg_with_tags("gw-01", json!({ "site": "plant-7" })),
            Some("{ThingName}/{site}/file-replicator"),
            None,
        );
        assert_eq!(t.prefix(), "gw-01/plant-7/file-replicator");
    }

    #[test]
    fn prefix_precedence_instance_beats_global_beats_default() {
        let c = cfg("gw-01");
        // instance override wins
        let t = Topics::from_config(&c, Some("g/{ThingName}"), Some("i/{ThingName}"));
        assert_eq!(t.prefix(), "i/gw-01");
        // global wins when no instance override
        let t = Topics::from_config(&c, Some("g/{ThingName}"), None);
        assert_eq!(t.prefix(), "g/gw-01");
        // default when neither
        let t = Topics::from_config(&c, None, None);
        assert_eq!(t.prefix(), "gw-01/file-replicator");
    }

    #[test]
    fn every_builder_produces_the_exact_topic() {
        let t = Topics::from_prefix("gw-01/file-replicator");
        assert_eq!(t.cmd_filter(), "gw-01/file-replicator/cmd/#");
        assert_eq!(t.cmd_config(), "gw-01/file-replicator/cmd/config");
        assert_eq!(t.cmd_status(), "gw-01/file-replicator/cmd/status");
        assert_eq!(t.cmd_trigger(), "gw-01/file-replicator/cmd/trigger");
        assert_eq!(
            t.cmd_instance_status("plant-1"),
            "gw-01/file-replicator/cmd/instances/plant-1/status"
        );
        assert_eq!(
            t.cmd_instance_trigger("plant-1"),
            "gw-01/file-replicator/cmd/instances/plant-1/trigger"
        );
        assert_eq!(
            t.cmd_instance_activation("plant-1"),
            "gw-01/file-replicator/cmd/instances/plant-1/activation"
        );
        assert_eq!(
            t.evt_component("ComponentReady"),
            "gw-01/file-replicator/evt/ComponentReady"
        );
        assert_eq!(
            t.evt_instance("plant-1", "ReplicationProgress"),
            "gw-01/file-replicator/evt/instances/plant-1/ReplicationProgress"
        );
        assert_eq!(t.state_component(), "gw-01/file-replicator/state");
        assert_eq!(
            t.state_instance("plant-1"),
            "gw-01/file-replicator/state/instances/plant-1"
        );
    }

    #[test]
    fn deepest_topic_is_within_budget() {
        // instance event = 5 slashes (DESIGN §15.4), the deepest resource.
        let t = Topics::from_prefix("gw-01/file-replicator");
        let topic = t.evt_instance("plant-csv-to-s3", "ReplicationProgress");
        assert_eq!(topic.bytes().filter(|b| *b == b'/').count(), 5);
        assert!(within_budget(&topic));
    }

    #[test]
    fn within_budget_boundaries() {
        // exactly 256 bytes is allowed; 257 is not.
        let ok = "a".repeat(256);
        assert_eq!(ok.len(), 256);
        assert!(within_budget(&ok));
        let too_long = "a".repeat(257);
        assert!(!within_budget(&too_long));

        // exactly 7 slashes allowed; 8 not.
        let seven = "a/a/a/a/a/a/a/a"; // 7 slashes, 8 levels
        assert_eq!(seven.bytes().filter(|b| *b == b'/').count(), 7);
        assert!(within_budget(seven));
        let eight = "a/a/a/a/a/a/a/a/a"; // 8 slashes
        assert!(!within_budget(eight));
    }

    #[test]
    fn pathological_thing_name_still_within_byte_budget_but_guard_never_panics() {
        // A 128-char ThingName (the IoT Core max) keeps the deepest topic under 256 bytes.
        let thing = "t".repeat(128);
        let t = Topics::from_config(&cfg(&thing), None, None);
        let topic = t.evt_instance(&"i".repeat(48), &"e".repeat(28));
        assert!(within_budget(&topic), "128-char thing stays within budget");

        // A ThingName beyond the IoT Core max blows the byte budget; the guard warns, never panics,
        // and still returns the (over-budget) topic.
        let huge = "t".repeat(300);
        let t2 = Topics::from_prefix(huge);
        let over = t2.state_component();
        assert!(!within_budget(&over));
        assert!(over.ends_with("/state"));
    }

    #[test]
    fn legacy_config_topic_uses_short_component_name() {
        let topic = legacy_config_topic(&cfg("gw-01"));
        assert_eq!(topic, "ggcommons/gw-01/config/get/FileReplicator");
        assert!(within_budget(&topic));
    }

    #[test]
    fn legacy_config_topic_over_budget_is_still_returned() {
        // A pathological ThingName blows the byte budget; the guard warns, never panics, still returns.
        let thing = "t".repeat(300);
        let topic = legacy_config_topic(&cfg(&thing));
        assert!(!within_budget(&topic));
        assert!(topic.starts_with("ggcommons/"));
    }

    #[test]
    fn cmd_filter_is_guarded_and_a_long_override_prefix_blows_the_budget() {
        // The control-plane SUBSCRIBE filter is where an over-long global/per-instance override prefix
        // bites hardest — an over-budget `{prefix}/cmd/#` is rejected by the broker, silently
        // disabling the whole control plane. The guard must flag it (warn) while still returning it.
        let default = Topics::from_prefix("gw-01/file-replicator");
        assert_eq!(default.cmd_filter(), "gw-01/file-replicator/cmd/#");
        assert!(within_budget(&default.cmd_filter()));

        let long = Topics::from_prefix(format!("{}/{}", "p".repeat(200), "q".repeat(60)));
        let f = long.cmd_filter();
        assert!(!within_budget(&f), "a 260-char override prefix pushes cmd/# past 256 bytes");
        assert!(f.ends_with("/cmd/#"));
    }

    #[test]
    fn over_slash_override_prefix_exceeds_the_slash_budget() {
        // A multi-segment override prefix drives the deepest event topic past the 7-slash IoT limit.
        let t = Topics::from_prefix("a/b/c/d/e/f/g/h"); // already 7 slashes in the prefix alone
        let topic = t.evt_instance("i", "E");
        assert!(topic.bytes().filter(|b| *b == b'/').count() > IOT_CORE_MAX_SLASHES);
        assert!(!within_budget(&topic), "guard warns but still returns the over-slash topic");
    }

    #[test]
    fn reserved_dollar_level_is_detected_and_topic_still_returned() {
        assert!(has_reserved_level("$aws/file-replicator/state"));
        assert!(has_reserved_level("gw-01/file-replicator/$sys/x"));
        assert!(!has_reserved_level("gw-01/file-replicator/state"));
        // A `$`-leading prefix (e.g. `-t '$x'`, which the resolver's sanitize does NOT neutralize)
        // warns but is still returned — the engine never panics on a hostile identifier.
        let t = Topics::from_prefix("$sys/file-replicator");
        let topic = t.state_component();
        assert!(topic.starts_with("$sys/"));
        assert!(has_reserved_level(&topic));
    }

    #[test]
    fn parse_cmd_routes_every_resource() {
        let p = "gw-01/file-replicator";
        assert_eq!(parse_cmd(p, "gw-01/file-replicator/cmd/config"), Some(Command::GetConfig));
        assert_eq!(
            parse_cmd(p, "gw-01/file-replicator/cmd/status"),
            Some(Command::GetStatus(Scope::All))
        );
        assert_eq!(
            parse_cmd(p, "gw-01/file-replicator/cmd/trigger"),
            Some(Command::Trigger(Scope::All))
        );
        assert_eq!(
            parse_cmd(p, "gw-01/file-replicator/cmd/instances/plant-1/status"),
            Some(Command::GetStatus(Scope::Instance("plant-1".into())))
        );
        assert_eq!(
            parse_cmd(p, "gw-01/file-replicator/cmd/instances/plant-1/trigger"),
            Some(Command::Trigger(Scope::Instance("plant-1".into())))
        );
        assert_eq!(
            parse_cmd(p, "gw-01/file-replicator/cmd/instances/plant-1/activation"),
            Some(Command::SetActivation("plant-1".into()))
        );
    }

    #[test]
    fn parse_cmd_rejects_unknown_and_foreign_topics() {
        let p = "gw-01/file-replicator";
        // unknown verb
        assert_eq!(parse_cmd(p, "gw-01/file-replicator/cmd/instances/plant-1/frobnicate"), None);
        // unknown top-level resource
        assert_eq!(parse_cmd(p, "gw-01/file-replicator/cmd/wat"), None);
        // not a cmd topic (evt)
        assert_eq!(parse_cmd(p, "gw-01/file-replicator/evt/ComponentReady"), None);
        // different prefix entirely
        assert_eq!(parse_cmd(p, "other/file-replicator/cmd/config"), None);
        // empty instance id
        assert_eq!(parse_cmd(p, "gw-01/file-replicator/cmd/instances//status"), None);
        // instances with no verb
        assert_eq!(parse_cmd(p, "gw-01/file-replicator/cmd/instances/plant-1"), None);
    }
}

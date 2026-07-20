# file-replicator (Claude Code)

EdgeCommons **sink** component (Rust), `com.mbreissi.edgecommons.FileReplicator`. The full picture —
what this component is, its config surface, and the org conventions it inherits — lives in
`AGENTS.md` and is shared with every agent tool. It is imported here in full:

@AGENTS.md

## Local-dev notes

- Local development builds against the sibling `edgecommons` checkout via the gitignored
  `.cargo/config.toml` `[patch]` override (see the org umbrella `CLAUDE.md`'s "Local-dev notes"),
  which replaces the pinned git `rev` in `Cargo.toml` outright. CI and fresh clones instead resolve
  the pinned rev via the committed `Cargo.lock` — no sibling checkout involved.

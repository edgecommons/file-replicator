# Reference — messaging interface (UNS)

All command/event/state topics use the Unified Namespace, rooted on the globally-unique ThingName and sized
for AWS IoT Core (≤256 bytes, ≤7 slashes). Full rationale + cloud-bridging in
[`DESIGN.md`](https://github.com/edgecommons/file-replicator/blob/main/DESIGN.md) §15. Implemented in P3.

```
{thing}/{component}/{class}/{resource…}      component = file-replicator   class = cmd | evt | state
```

`{thing}` and `file-replicator` are the reliable, mandatory segments; `site`/`enterprise` live in the
message envelope `tags` (not the path). Prefix is configurable via `component.global.topics.prefix`
(default `{ThingName}/file-replicator`).

## Commands (`cmd`, request/reply via `reply_to`)
| Command | Topic | Body | Reply |
|---|---|---|---|
| get-config | `{thing}/file-replicator/cmd/config` | `{}` | effective config |
| get-status | `{thing}/file-replicator/cmd/status` · `…/cmd/instances/{id}/status` | `{}` | statistics |
| trigger | `…/cmd/trigger` · `…/cmd/instances/{id}/trigger` | `{ "ignoreWindow"?: bool }` | accepted + counts |
| set-activation | `…/cmd/instances/{id}/activation` | `{ "active": bool, "persist"?: bool, "reset"?: bool }` | new state |

A legacy alias for the core `GetConfiguration` contract (`ggcommons/{thing}/config/get/{component}`) can be
enabled with `legacyConfigTopic: true` (DESIGN §15.6).

## Events (`evt`, non-retained)
`{thing}/file-replicator/evt/instances/{id}/{event}` and component-level `…/evt/{event}`. Event types:
`FileDiscovered`, `FileReady`, `ReplicationStarted`, `ReplicationProgress` (throttled), `ReplicationCompleted`,
`ReplicationFailed`, `FileArchived`, `FileDeleted`, `FileQuarantined`, `RetriesExhausted`, `ScheduleTriggered`,
`WindowOpened`, `WindowClosed`, `ScanComplete`, `Disconnected`, `Reconnected`, `InstanceActivated`,
`InstanceDeactivated`, `ComponentReady`, `PermissionDenied`.

`PermissionDenied` (`{ "path": string, "role": "ingress"|"egress"|"archive"|"failed", "error": string }`) —
a directory/target the instance depends on is unreadable/unwritable, at startup or at runtime. ALWAYS
emitted, but **deduplicated** so it is not repeated on every rescan or every file: for `ingress`/`archive`/`failed`
the dedup key (carried in `path`) is the directory, and for `egress` it is the **destination** (so a
broken-permission destination emits once, not once per file — `path` then names the destination). A
recovery re-arms the dedup, so a later re-break emits again. See **Permission handling** in
`explanation.md`. Governed by `onPermissionError` (`component.global` / per-instance).

## State (`state`, **retained**)
`{thing}/file-replicator/state/instances/{id}` and `…/state` carry the latest snapshot (retained) so a
fresh UI/cloud subscriber renders correctly on connect. An instance disabled at startup by
`onPermissionError: disableInstance` still gets a snapshot/`get-status` doc — `active: false`,
`disabled: true`, `disabledReason` — instead of appearing as an unknown id.

Envelope: the standard ggcommons `Message` (`header`/`tags`/`body`); events use `header.name =
"FileReplicatorEvent"`, `version = "1.0"`, with `body.event` discriminating. See DESIGN §16/§17 for full
payloads.

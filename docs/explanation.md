# Explanation — how file-replicator works

Deep, concept-oriented background (the *why*). The full treatment — with diagrams — lives in
[`DESIGN.md`](https://github.com/edgecommons/file-replicator/blob/main/DESIGN.md); this page distills the
concepts as the implementation lands, validated against the code.

## The instance model
An **instance** is one watched-directory specification (`component.instances[]`) — the unit of config,
isolation, statistics, activation, and control. Instances run independently, so a slow or failing
destination on one never stalls another.

## Readiness — knowing a file is done
A newly-observed file may still be mid-write. The **stability** strategy (default) waits for size+mtime to
settle; `marker`, `rename`, and `glob` suit cooperative producers. Only *ready* files enter the durable
queue.

## Discovery — how files are noticed, and the latency to expect
An instance finds files two ways, and it needs both:

1. **OS file watch** (inotify on Linux, the platform equivalent elsewhere) — the low-latency path. A change
   under the watched directory *nudges* the engine to re-scan within milliseconds. On startup the instance
   logs `OS file watch active` once the watch is established.
2. **Periodic reconciliation rescan** (`ingress.rescanSecs`, default **30 s**) — the belt-and-suspenders
   fallback. It re-scans the directory unconditionally, so a file is discovered even when no watch event
   arrives for it.

The two paths interact with readiness, and the interaction is the key to understanding latency. The OS watch
only fires on *change* events — but a file that has finished being written (or was atomically renamed in)
produces **no further events once it goes quiet**. Under the default `stability` strategy, "quiet" (`size`
and `mtime` unchanged for `quietSecs`) is precisely the *absence* of events, so the watch alone can never
observe the transition to ready. The engine closes this gap by **self-scheduling a re-observation** a short
time (~`quietSecs`) after it first sees a still-settling file, rather than waiting for the next rescan. So in
the normal case, **discovery-to-ready latency ≈ `quietSecs`** (a second or two), *independent* of
`rescanSecs`.

### Degraded mode — when the OS watch is unavailable or misses an event
The OS watch is best-effort, and correctness never depends on it. Two things can reduce discovery to the
rescan cadence:

- **The watch can't be established.** Some filesystems and mount types don't deliver watch events reliably —
  notably many **network filesystems** (NFS/SMB), some **container bind-mounts / overlay volumes**, and FUSE
  mounts. If setting up the watch fails, the instance logs a warning
  (`OS file watch … using periodic rescan only`) and continues on the rescan alone.
- **An event is dropped.** Under a burst, the OS event queue can overflow (e.g. inotify `IN_Q_OVERFLOW`); the
  instance logs `OS file watch event error` and the periodic rescan reconciles whatever the watch missed.

In degraded mode **no file is ever lost or skipped** — discovery simply falls back to the rescan, so a file
is picked up **within at most `rescanSecs`** (default 30 s) instead of ~`quietSecs`. If you run on a
filesystem where the watch may not fire and you need tighter bounds, **lower `rescanSecs`** (the rescan is a
cheap directory walk; a few seconds is fine for most spools). Conversely, on a huge, slow, or high-latency
spool you may *raise* it to reduce scan overhead, accepting a larger worst-case fallback latency. Watch the
startup log to know which mode you're in: `OS file watch active` (low-latency) vs the `… periodic rescan
only` warning (degraded).

## Durable, crash-safe movement
Work items live in an embedded **SQLite** store (WAL). Every state transition is written **before** the
side effect it authorizes (write-ahead), so a crash between "verified" and "source removed" is recovered
idempotently on restart — never re-uploading, never losing the file. Object keys are stable/deterministic
so re-delivery overwrites identically.

## Scheduling vs windows
`cron` releases ready work at each fire; a `window` (open→close cron, or open+duration) gates continuous
flow to a time span, for bandwidth conservation. Work outside the window waits; a transfer crossing a
window close either finishes or pauses/resumes next window.

## Resilience across long outages
Two mechanisms carry a transfer across a destination being unreachable for a long time:

- **Time-governed retries.** A failed attempt is retried on an exponential backoff (`retry.baseDelayMs` →
  `retry.maxDelayMs`) and the item keeps retrying until a **time budget** — `retry.giveUpAfter`, default
  **7 days** — elapses, *not* until some attempt count is reached. (`retry.maxAttempts` is an optional extra
  hard cap; by default there is none — retries are purely time-governed.) So an endpoint that is down for
  hours to a couple of days is simply retried across the outage rather than exhausted early. When the budget
  finally expires the file becomes Failed and follows `completion.onExhausted` (retain-in-place or
  quarantine).
- **Resumable, checkpointed transfers.** In-flight progress is checkpointed to the durable store, so a
  transfer interrupted by a crash, a restart, or the destination dropping mid-upload **resumes from where it
  left off** rather than re-sending the whole file — S3 persists multipart parts, and HTTP / SFTP / FTPS /
  GCS / Azure each resume through their backend's ranged-PUT / append / session / staged-block mechanism (see
  [Reference › Destinations](reference/destinations.md)).

Together these tolerate a multi-hour-to-multi-day outage without loss.

> **No destination circuit-breaker.** file-replicator has no cross-transfer circuit-breaker that trips after
> repeated failures or staggers reconnects: there are no `Disconnected`/`Reconnected` alarm events and
> `get-status` has no `link` field (see [Reference › Messaging interface](reference/messaging-interface.md)).
> Long-outage tolerance rests on the two mechanisms above — each failing transfer independently backs off and
> retries on its own schedule within the `giveUpAfter` budget, with no shared breaker gating reconnects.

## Cross-instance priority
Every instance shares two process-wide governors: the global bandwidth token bucket, and
the global concurrency cap (`component.global.limits.maxConcurrentFiles`, default 64) that bounds
in-flight *files* across ALL instances at once. Under light load neither governor matters — every
instance simply gets a slot/bytes as it asks. Under **contention** for the global concurrency cap
(more instances trying to transfer than there are free slots), each instance's `priority` (default
`100`; lower = higher priority, the same convention used elsewhere in the config) decides who is
admitted first: the queued waiter with the lowest `priority` number goes first, and waiters at the same
priority are served strictly in the order they started waiting (FIFO — a later-arriving instance at the
same priority never jumps an earlier one).

This is **admission order only** — it does not create a reservation. An instance that isn't currently
contending for a global slot (idle, or its files are all done) holds nothing back for itself: a
high-priority instance with no work in flight never starves a busy low-priority one of a slot it isn't
using. The moment the high-priority instance *does* have work and joins the queue, it jumps ahead of
whatever lower-priority instance is still waiting — but anything already admitted and running keeps
running to completion; priority is not preemptive.

Two things `priority` deliberately does **not** do:

- It doesn't affect an instance's own `limits.maxConcurrentFiles` — that per-instance cap is unrelated
  to cross-instance admission and stays exactly as configured regardless of priority.
- It doesn't weight the bandwidth token bucket. A higher-priority instance gets its transfer *started*
  sooner under concurrency contention, but once running, it competes for bytes-per-second on equal terms
  with everyone else. Weighting bandwidth by priority is explicitly out of scope for this feature — if
  you need a class of instances to also get more of the byte-rate budget, give them their own
  `limits.maxBandwidth` rather than relying on `priority`.

In practice: leave `priority` at the default for most instances, and lower it (e.g. `10`) only for the
handful whose files genuinely need to jump the queue when the global cap is saturated — a
latency-sensitive control-signal spool ahead of a bulk nightly archive feed sharing the same process, for
example.

## Permission handling
A watched directory or a destination directory can be unreadable/unwritable — a bad mount, a chmod
mistake, a container running as the wrong user. Two things are true of every such failure, no matter
when it happens or what policy is configured:

1. It is **always** logged and **always** surfaced as a `PermissionDenied` event (`{path, role, error}`,
   `role` one of `ingress`/`egress`/`archive`/`failed`) — an operator must never be silently starved of
   files with no signal why.
2. It is logged **once**, not once per reconciliation rescan (and, on the egress side, not once per
   file). A per-instance dedup log tracks each distinct failing key and only re-logs on a real state
   change: the first sighting, a later *recovery* (the key becomes accessible again — logged once, at
   `INFO`, so "it's fixed" is as visible as "it broke"), or after a long quiet interval (an hour) for a
   persistently-broken key so it never goes completely silent either. On the ingress side the key is the
   watched directory path; on the egress side it is the **destination** — so a broken-permission
   destination logs/emits once for the destination, not once for every file it can't deliver (which also
   bounds the dedup state to the fixed number of configured destinations rather than growing per file).

**Startup validation.** Before an instance starts, its ingress directory (readable), every `local`
egress directory (writable), and a configured `archiveDir`/`failedDir` (creatable-or-writable) are
probed. A violation is resolved against `onPermissionError` (component-wide default `disableInstance`,
overridable per instance — see the configuration reference):

- **`disableInstance`** (the default) — skip just that instance, the same as a malformed instance:
  **instance isolation is the whole point** — one operator's typo'd path must never take down every
  other instance's replication. A disabled instance still shows up in `get-status` (component-wide and
  scoped-by-id), with `disabled: true` and `disabledReason`, instead of silently vanishing or reading as
  "unknown instance".
- **`fatal`** — abort the whole component. An explicit opt-in for deployments that would rather fail
  loudly at startup than run in a visibly degraded state.
- **`retain`** — start the instance anyway. It will keep hitting the same error on every rescan/transfer
  attempt; the dedup-logging above is what keeps that from being a log-spam problem, and the
  `PermissionDenied` events are what keeps an operator informed without watching logs.

If **every** instance ends up disabled (or the configured set was empty to begin with), the component
still fails to start — the same "fail only if zero instances start" rule that applies to malformed
config, reachable here via an all-inaccessible instance set.

**Runtime.** A directory can also become inaccessible mid-run (unmounted, permissions changed under a
running process). On the ingress side, the discovery scan keeps walking past an unreadable subdirectory
(never fatal to the scan) and reports it through the same dedup-log + event path; a file the scan can
list but not open because of a *permission* denial is reported the same way. On the egress side, a
permission-denied delivery error is detected by how the backend **classifies** the failure (every
destination funnels its errors through the same taxonomy, so a real read-only/`chmod 000` target is
caught, not just a synthetic one), dedup-logged + surfaced as a `PermissionDenied` event once per
destination, and then follows the standard retry/quarantine decision — the permission handling here is
observability, not a separate failure-handling path. (See `src/permission.rs`.)

## The unified namespace
Commands and events ride the ggcommons UNS core: `ecv1/{device}/FileReplicator/{instance}/{cmd|evt}/…`
(minted by the library's `commands()`/`events()` facades, not a hand-rolled topic builder — see
`docs/reference/messaging-interface.md`). There is no dashboard-facing retained `state/…` snapshot: the
UNS `state` class is reserved to the library's own RUNNING/STOPPED keepalive, and a dashboard wanting an
instance's current picture calls `get-status` (a `cmd` verb) instead — which is what a late-connecting
subscriber always had to fall back to anyway.

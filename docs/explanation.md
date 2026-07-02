# Explanation — how file-replicator works

Deep, concept-oriented background (the *why*). The full treatment — with diagrams — lives in
[`DESIGN.md`](https://github.com/edgecommons/file-replicator/blob/main/DESIGN.md); this page distills the
concepts as the implementation lands, validated against the code.

## The instance model
An **instance** is one watched-directory specification (`component.instances[]`) — the unit of config,
isolation, statistics, activation, and control. Instances run independently, so a slow or failing
destination on one never stalls another. (DESIGN §3, §6.3.)

## Readiness — knowing a file is done
A newly-observed file may still be mid-write. The **stability** strategy (default) waits for size+mtime to
settle; `marker`, `rename`, and `glob` suit cooperative producers. Only *ready* files enter the durable
queue. (DESIGN §9.)

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
only` warning (degraded). (DESIGN §6.3, §9.)

## Durable, crash-safe movement
Work items live in an embedded **SQLite** store (WAL). Every state transition is written **before** the
side effect it authorizes (write-ahead), so a crash between "verified" and "source removed" is recovered
idempotently on restart — never re-uploading, never losing the file. Object keys are stable/deterministic
so re-delivery overwrites identically. (DESIGN §8.1, §13.2, §14.)

## Scheduling vs windows
`cron` releases ready work at each fire; a `window` (open→close cron, or open+duration) gates continuous
flow to a time span, for bandwidth conservation. Work outside the window waits; a transfer crossing a
window close either finishes or pauses/resumes next window. (DESIGN §12.)

## Resilience across long outages
Retries are **time-governed** (`giveUpAfter`, default 7 days) rather than attempt-capped, uploads **resume**
from persisted checkpoints, and a **disconnection circuit-breaker** avoids a reconnect thundering-herd —
so a multi-hour to ~2-day outage is tolerated without loss. (DESIGN §13.4.)

## The unified namespace
All command/event/state topics are rooted on the globally-unique ThingName:
`{thing}/file-replicator/{cmd|evt|state}/…` — RESTful, within IoT Core's 256-byte/7-slash limits, and
cloud-bridge-safe (identity is in the topic). Retained `state/…` gives dashboards a snapshot on connect.
(DESIGN §15.)

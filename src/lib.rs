//! # file-replicator — library crate
//!
//! The component's engine is exposed as a library so the thin [`main`](../main/index.html) binary and
//! the end-to-end integration tests (`tests/`) build against **one** public surface. The binary is a
//! shim: it constructs the edgecommons runtime and hands control to [`app::App`]; every unit of real
//! behavior — config parsing, the durable [`state`] store, the [`dest`] destination abstraction, the
//! per-instance [`instance::Instance`] engine (watch → durable queue → deliver → verify → complete),
//! the retry/backoff policy, integrity, rate limiting, and readiness — lives here and is unit-tested
//! inline plus driven end-to-end from `tests/p1_engine.rs`.
//!
//! Scope: P1 (local destination, immediate mode). See `DESIGN.md` for the phase plan.

pub mod admission;
pub mod app;
pub mod config;
pub mod control;
pub mod dest;
pub mod domain;
pub mod error;
pub mod events;
pub mod instance;
pub mod integrity;
pub mod metrics;
pub mod permission;
pub mod ratelimit;
pub mod readiness;
pub mod schedule;
pub mod state;

//! Library surface for `celery-bench`.
//!
//! Exposes the kombu Redis-transport protocol implementation (queue-key derivation +
//! task envelope), the bulk producer, the load worker, and the metrics/report types
//! so that `tests/` integration tests can drive real enqueue/dequeue cycles against a
//! live Redis using the exact same code the `celery-bench` binary runs — not a
//! reimplementation of it. CLI parsing and trial orchestration stay in `src/main.rs`
//! since they're specific to the binary, not something a test needs to depend on.
pub mod job;
pub mod metrics;
pub mod producer;
pub mod report;
pub mod tls;
pub mod worker;

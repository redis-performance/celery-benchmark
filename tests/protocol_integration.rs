//! Integration tests against a REAL Redis instance.
//!
//! Unit tests (in `src/*.rs`) prove the pure key-derivation/envelope logic is
//! correct in isolation, but they can't prove the wire protocol is actually right —
//! only running the real producer/worker code against a real Redis and inspecting
//! what actually landed on the wire can do that. These tests do exactly that: they
//! call the library's `producer`/`worker`/`job` modules directly (the same code the
//! `celery-bench` binary runs) against a live Redis, then assert on real `TYPE` /
//! `LLEN` / payload inspection — not just "the run completed".
//!
//! Requires a reachable Redis (matching the `redis:8.6` service the CI job runs).
//! Override the target with `CELERY_BENCH_TEST_REDIS_URL`; defaults to
//! `redis://127.0.0.1:6379/15` (db 15 — isolated from both the tool's own db-13
//! convention and the CI smoke test's db 0).
//!
//! Each test uses its own queue name prefix so the tests can run concurrently
//! (`cargo test` runs tests in a binary in parallel by default) without clobbering
//! each other's keys on the shared Redis instance.

use celery_bench::job::{self, CeleryEnvelope};
use celery_bench::metrics::Metrics;
use celery_bench::producer;
use celery_bench::worker::LoadWorker;
use redis::AsyncCommands;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

fn test_redis_url() -> String {
    std::env::var("CELERY_BENCH_TEST_REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379/15".to_string())
}

async fn connect() -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(test_redis_url()).expect("valid test Redis URL");
    client.get_multiplexed_async_connection().await.expect(
        "connect to the integration-test Redis — set CELERY_BENCH_TEST_REDIS_URL \
             or run a Redis instance on 127.0.0.1:6379 (db 15)",
    )
}

/// (a) + (b): enqueue via the real `producer::bulk_enqueue`, then verify — via raw
/// Redis introspection, not the library's own accessors — that every priority
/// combination landed on exactly the key `job::brpop_keys` says kombu would poll,
/// each key is a Redis list (kombu's `LPUSH` target type), the total item count
/// equals what was enqueued, and a sampled payload is a structurally valid
/// `CeleryEnvelope` matching the README's documented wire shape.
#[tokio::test]
async fn protocol_priority_expanded_keys_hold_correct_shape_and_counts() {
    let mut conn = connect().await;
    let queues = vec![
        "it_protocol_shape_q0".to_string(),
        "it_protocol_shape_q1".to_string(),
    ];
    producer::clear_queue(&mut conn, &queues)
        .await
        .expect("clear before test");

    let priorities: Vec<u8> = vec![0, 3, 6, 9];
    // 2 queues x 4 priorities = 8 combinations; 40 jobs = 5 full cycles, so every
    // priority-expanded key must land exactly 5 items — a precise, deterministic
    // assertion that would have caught the "half the keys silently starved"
    // round-robin correlation bug this tool's own producer used to have.
    let n_jobs: u64 = 40;
    producer::bulk_enqueue(&mut conn, &queues, &priorities, n_jobs)
        .await
        .expect("bulk_enqueue");

    let keys = job::brpop_keys(&queues);
    assert_eq!(
        keys.len(),
        8,
        "2 queues * 4 priority steps must expand to 8 keys"
    );

    let mut total: i64 = 0;
    for key in &keys {
        let kind: String = redis::cmd("TYPE")
            .arg(key)
            .query_async(&mut conn)
            .await
            .expect("TYPE query");
        assert_eq!(
            kind, "list",
            "key {key:?} must be a Redis list — kombu's Channel._put() always LPUSHes"
        );
        let len: i64 = conn.llen(key).await.expect("LLEN query");
        assert_eq!(
            len, 5,
            "round-robin over 2 queues x 4 priorities x 40 jobs must land exactly 5 \
             items per priority-expanded key, key={key:?} (if this is 0 or 10, the \
             queue/priority round-robin has regressed to the old gcd-correlation bug)"
        );
        total += len;
    }
    assert_eq!(
        total, n_jobs as i64,
        "total items across all keys must equal n_jobs"
    );

    // Non-destructively sample one item from a non-zero-priority key and verify it
    // round-trips through the exact envelope shape the README's "Protocol
    // compatibility" section documents.
    let sample_key = job::q_for_pri(&queues[0], 3);
    assert_eq!(
        sample_key,
        format!("{}{}{}", queues[0], job::PRIORITY_SEP, 3),
        "priority 3 must expand using kombu's exact control-byte separator"
    );
    let raw: Vec<u8> = conn
        .lindex(&sample_key, 0)
        .await
        .expect("LINDEX on the priority-3 key");
    let envelope: CeleryEnvelope =
        serde_json::from_slice(&raw).expect("payload must be valid CeleryEnvelope JSON");
    assert_eq!(envelope.content_encoding, "utf-8");
    assert_eq!(envelope.content_type, "application/json");
    assert_eq!(envelope.properties.body_encoding, "base64");
    assert_eq!(
        envelope.properties.priority, 3,
        "properties.priority stores the RAW requested priority, not the floored key priority"
    );
    assert_eq!(envelope.properties.delivery_info.routing_key, queues[0]);
    assert!(
        envelope.enqueued_at_ns().is_some(),
        "the benchmark-injected enqueued_at_ns latency marker must round-trip through \
         base64(json) unchanged"
    );

    producer::clear_queue(&mut conn, &queues).await.ok();
}

/// Runs the real `LoadWorker` (the exact struct `main.rs` spawns N of per trial)
/// against real enqueued jobs and asserts: every enqueued job is dequeued exactly
/// once (no loss, no duplication), zero decode errors (proves the envelope really
/// round-trips through the real BRPOP response, not just through in-process
/// serialize/deserialize), every priority-expanded key drains to empty, and a
/// latency sample is recorded per job (proves the enqueued_at_ns marker survives a
/// real network hop).
#[tokio::test]
async fn worker_dequeues_exactly_enqueued_count_and_drains_all_priority_keys() {
    let mut conn = connect().await;
    let queues = vec!["it_worker_drain_q".to_string()];
    producer::clear_queue(&mut conn, &queues)
        .await
        .expect("clear before test");

    let priorities: Vec<u8> = vec![0, 3, 6, 9];
    let n_jobs: u64 = 200;
    producer::bulk_enqueue(&mut conn, &queues, &priorities, n_jobs)
        .await
        .expect("bulk_enqueue");

    let keys = Arc::new(job::brpop_keys(&queues));
    let metrics = Arc::new(Metrics::new());
    let (done_tx, mut done_rx) = watch::channel(false);
    let done_tx = Arc::new(done_tx);
    let (latency_tx, mut latency_rx) = mpsc::unbounded_channel::<u64>();
    let shutdown = Arc::new(AtomicBool::new(false));

    let n_workers = 4;
    let client = redis::Client::open(test_redis_url()).unwrap();
    let mut handles = Vec::new();
    for _ in 0..n_workers {
        let conn = client.get_multiplexed_async_connection().await.unwrap();
        let worker = LoadWorker {
            metrics: metrics.clone(),
            latency_tx: latency_tx.clone(),
            done_tx: done_tx.clone(),
            target_jobs: n_jobs,
        };
        let keys = keys.clone();
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            worker.run(conn, keys, 1.0, shutdown).await
        }));
    }
    drop(latency_tx); // this test's own clone — workers hold theirs

    // Real network round trips (BRPOP against real Redis) — give it real wall time,
    // not an instant-fail timeout.
    let waited = tokio::time::timeout(Duration::from_secs(30), done_rx.wait_for(|v| *v)).await;
    assert!(
        waited.is_ok(),
        "worker pool did not reach target_jobs within 30s — dequeued so far: {}",
        metrics.get_completed()
    );

    shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
    for h in handles {
        let _ = tokio::time::timeout(Duration::from_secs(5), h).await;
    }

    assert_eq!(
        metrics.get_completed(),
        n_jobs,
        "every enqueued job must be dequeued exactly once — no loss, no duplication"
    );
    assert_eq!(
        metrics.get_errors(),
        0,
        "zero envelope decode errors — proves the payload round-trips through a real \
         BRPOP response byte-for-byte"
    );

    let mut latency_samples = 0u64;
    while latency_rx.try_recv().is_ok() {
        latency_samples += 1;
    }
    assert_eq!(
        latency_samples, n_jobs,
        "one latency sample per dequeued job — proves enqueued_at_ns survived the real \
         network hop and was correctly extracted on the worker side"
    );

    for key in keys.iter() {
        let len: i64 = conn.llen(key).await.expect("LLEN query");
        assert_eq!(
            len, 0,
            "key {key:?} must be fully drained after the worker pool finishes"
        );
    }

    producer::clear_queue(&mut conn, &queues).await.ok();
}

/// The BRPOP key list a worker polls must match `job::brpop_keys` (priority outer,
/// queue inner) exactly, and pushing to a key kombu would never poll (a wrong
/// separator, wrong step, or wrong ordering) must NOT be picked up by a worker
/// listening on the correct key set — verifying the worker doesn't accidentally
/// dequeue from some other key it happens to share a prefix with.
#[tokio::test]
async fn worker_only_dequeues_from_the_documented_priority_expanded_keys() {
    let mut conn = connect().await;
    let queue = "it_worker_wrong_key_q".to_string();
    let queues = vec![queue.clone()];
    producer::clear_queue(&mut conn, &queues)
        .await
        .expect("clear before test");
    // Also clear a decoy key that looks similar but isn't one kombu would ever poll.
    let decoy_key = format!("{queue}_not_a_real_priority_key");
    let _: () = conn.del(&decoy_key).await.unwrap();

    // Push one legitimate job (priority 0, bare queue key) and one job onto a decoy
    // key that a naive/buggy BRPOP key list might also happen to include.
    producer::bulk_enqueue(&mut conn, &queues, &[0], 1)
        .await
        .expect("bulk_enqueue legit job");
    let bogus_envelope = CeleryEnvelope::new(&queue, 0, 999);
    let _: () = conn
        .lpush(&decoy_key, serde_json::to_string(&bogus_envelope).unwrap())
        .await
        .unwrap();

    let keys = Arc::new(job::brpop_keys(&queues));
    assert!(
        !keys.contains(&decoy_key),
        "sanity check on the test fixture itself: decoy key must not be a real BRPOP key"
    );

    let metrics = Arc::new(Metrics::new());
    let (done_tx, mut done_rx) = watch::channel(false);
    let done_tx = Arc::new(done_tx);
    let (latency_tx, _latency_rx) = mpsc::unbounded_channel::<u64>();
    let shutdown = Arc::new(AtomicBool::new(false));

    let client = redis::Client::open(test_redis_url()).unwrap();
    let worker_conn = client.get_multiplexed_async_connection().await.unwrap();
    let worker = LoadWorker {
        metrics: metrics.clone(),
        latency_tx,
        done_tx,
        target_jobs: 1,
    };
    let handle = tokio::spawn({
        let keys = keys.clone();
        let shutdown = shutdown.clone();
        async move { worker.run(worker_conn, keys, 1.0, shutdown).await }
    });

    let waited = tokio::time::timeout(Duration::from_secs(10), done_rx.wait_for(|v| *v)).await;
    assert!(waited.is_ok(), "worker never reached its 1-job target");
    shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    assert_eq!(
        metrics.get_completed(),
        1,
        "must dequeue exactly the one legitimate job"
    );
    let decoy_len: i64 = conn.llen(&decoy_key).await.unwrap();
    assert_eq!(
        decoy_len, 1,
        "the decoy key must be left untouched — not part of the BRPOP key list"
    );

    let _: () = conn.del(&decoy_key).await.unwrap();
    producer::clear_queue(&mut conn, &queues).await.ok();
}

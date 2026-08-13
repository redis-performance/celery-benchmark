use crate::job::CeleryEnvelope;
use crate::metrics::Metrics;
use anyhow::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};

/// One simulated Celery worker process's dequeue loop.
///
/// Real Celery workers hold one broker connection per worker process and issue
/// sequential `BRPOP` calls against it (kombu/transport/redis.py
/// `MultiChannelPoller._brpop_start` / `_brpop_read`, lines 999-1035). We reproduce
/// that shape directly: each `LoadWorker::run` call owns a single dedicated
/// connection and loops BRPOP on it — concurrency across simulated workers comes
/// from running N of these loops on N separate connections (spawned in main.rs),
/// the same way N real Celery worker processes would each hold their own connection.
#[derive(Clone)]
pub struct LoadWorker {
    pub metrics: Arc<Metrics>,
    /// Sends latency_us values to the histogram collector task.
    pub latency_tx: mpsc::UnboundedSender<u64>,
    /// Signals the trial orchestrator when all jobs are done.
    pub done_tx: Arc<watch::Sender<bool>>,
    pub target_jobs: u64,
}

impl LoadWorker {
    /// Run the BRPOP dequeue loop until `shutdown` is set or `target_jobs` is reached.
    ///
    /// `keys` is the full priority-expanded BRPOP key list for however many queues this
    /// trial is consuming (see `job::brpop_keys`) — issued as a single multi-key BRPOP,
    /// exactly matching kombu's `_brpop_start` command shape:
    /// `BRPOP key1 key2 ... keyN timeout`.
    ///
    /// `brpop_timeout_secs` mirrors kombu's `Transport.brpop_timeout` (default `1`,
    /// kombu/transport/redis.py:1436) — it bounds how quickly this loop notices
    /// `shutdown` when the queue is empty, same as real kombu's polling granularity.
    pub async fn run(
        &self,
        mut conn: redis::aio::MultiplexedConnection,
        keys: Arc<Vec<String>>,
        brpop_timeout_secs: f64,
        shutdown: Arc<AtomicBool>,
    ) -> Result<()> {
        loop {
            if shutdown.load(Ordering::Relaxed) {
                return Ok(());
            }
            if self.metrics.get_completed() >= self.target_jobs {
                return Ok(());
            }

            let mut cmd = redis::cmd("BRPOP");
            for k in keys.iter() {
                cmd.arg(k.as_str());
            }
            cmd.arg(brpop_timeout_secs);

            match cmd
                .query_async::<Option<(String, Vec<u8>)>>(&mut conn)
                .await
            {
                Ok(Some((_key, payload))) => self.handle_message(&payload),
                Ok(None) => {
                    // BRPOP timed out with no data — identical to a quiet queue in
                    // production kombu; loop back around to re-check shutdown/target.
                }
                Err(e) => {
                    if e.is_connection_dropped() || e.is_io_error() {
                        // Connection died — nothing more this worker can do.
                        return Err(anyhow::anyhow!("Redis connection error in worker: {e}"));
                    }
                    // Non-fatal (e.g. transient protocol hiccup) — count as an error
                    // and keep polling rather than tearing down the whole trial.
                    self.metrics.inc_error();
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
    }

    fn handle_message(&self, payload: &[u8]) {
        match serde_json::from_slice::<CeleryEnvelope>(payload) {
            Ok(envelope) => match envelope.enqueued_at_ns() {
                Some(enqueued_at_ns) => {
                    let now_ns = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("system clock is before UNIX_EPOCH")
                        .as_nanos() as u64;
                    let latency_us = if now_ns >= enqueued_at_ns {
                        (now_ns - enqueued_at_ns) / 1_000
                    } else {
                        // Clock skew: producer clock ahead of worker clock — record
                        // 1 µs rather than let a saturating_sub-to-0 be silently
                        // discarded by the histogram's lower bound.
                        1
                    };
                    let clamped = latency_us.max(1);
                    let _ = self.latency_tx.send(clamped);
                }
                None => self.metrics.inc_error(),
            },
            Err(_) => self.metrics.inc_error(),
        }

        let done = self.metrics.inc_completed();
        if done >= self.target_jobs {
            let _ = self.done_tx.send(true);
        }
    }
}

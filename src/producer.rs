use crate::job::{brpop_keys, q_for_pri, CeleryEnvelope};
use anyhow::Result;

const BATCH_SIZE: usize = 1000;

/// Delete every priority-expanded key kombu could have written to for the given
/// queues (all 4 steps per queue — see `job::brpop_keys`). This is the default
/// pre-trial cleanup — safe to use on shared Redis since it only touches the
/// benchmark's own queue keys.
///
/// Unlike sidekiq-benchmark's `clear_queue`, there is no companion "known queues" set
/// to clean up: kombu's Redis transport has no equivalent of Sidekiq's `queues` SET
/// (that bookkeeping is Sidekiq-Web-specific, not part of the wire protocol).
pub async fn clear_queue(
    conn: &mut redis::aio::MultiplexedConnection,
    queues: &[String],
) -> Result<()> {
    let mut pipe = redis::pipe();
    for key in brpop_keys(queues) {
        pipe.cmd("DEL").arg(key).ignore();
    }
    pipe.query_async::<()>(conn).await?;
    Ok(())
}

/// Flush the entire database. Only called when --allow-flushdb is explicitly set.
pub async fn flushdb(conn: &mut redis::aio::MultiplexedConnection) -> Result<()> {
    redis::cmd("FLUSHDB").query_async::<()>(conn).await?;
    Ok(())
}

/// Bulk-enqueue `n_jobs` Celery task messages distributed round-robin across `queues`,
/// and — within each queue — round-robin across `priorities`.
///
/// Uses LPUSH, matching kombu's `Channel._put()` (kombu/transport/redis.py:1073-1086:
/// `client.lpush(key, dumps(message))`), onto the priority-derived key
/// (`job::q_for_pri`). Workers pop from the right end via BRPOP (see worker.rs),
/// matching kombu's `_brpop_start`/`_brpop_read` — same LPUSH-produce /
/// BRPOP-consume direction as real Celery/kombu, so FIFO ordering is preserved
/// exactly as in production.
pub async fn bulk_enqueue(
    conn: &mut redis::aio::MultiplexedConnection,
    queues: &[String],
    priorities: &[u8],
    n_jobs: u64,
) -> Result<()> {
    let n_queues = queues.len() as u64;
    let n_priorities = priorities.len() as u64;
    let mut idx = 0u64;
    let mut remaining = n_jobs;

    while remaining > 0 {
        let batch = remaining.min(BATCH_SIZE as u64) as usize;
        let mut pipe = redis::pipe();

        for j in 0..batch {
            let i = idx + j as u64;
            let queue = &queues[(i % n_queues) as usize];
            let priority = priorities[(i % n_priorities) as usize];
            let key = q_for_pri(queue, priority);
            let envelope = CeleryEnvelope::new(queue, priority, i);
            let payload = serde_json::to_string(&envelope)?;
            pipe.lpush(key, payload).ignore();
        }

        pipe.query_async::<()>(conn).await?;
        idx += batch as u64;
        remaining -= batch as u64;
    }

    Ok(())
}

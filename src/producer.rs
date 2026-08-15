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

/// Pick the (queue_index, priority_index) for job `i` in a round-robin distribution
/// across `n_queues` queues and `n_priorities` priorities.
///
/// Queue and priority are picked from two INDEPENDENT strides of the same counter
/// (`i % n_queues` vs `(i / n_queues) % n_priorities`), not both via `i % n`.
/// Deriving both directly from `i % n` looks like a fair round robin but isn't one
/// whenever gcd(n_queues, n_priorities) > 1: e.g. 2 queues x 4 priorities would make
/// queue_idx = i % 2 always equal priority_idx % 2, so queue 0 could only ever land
/// on priorities[0] and priorities[2] and queue 1 only on priorities[1] and
/// priorities[3] — half of the (queue, priority) combinations, and therefore half of
/// the priority-expanded Redis keys, would silently receive zero jobs while the other
/// half received double share. The div/mod split below visits every (queue,
/// priority) pair exactly once per full `n_queues * n_priorities`-job cycle,
/// regardless of their gcd.
fn round_robin_indices(i: u64, n_queues: u64, n_priorities: u64) -> (u64, u64) {
    (i % n_queues, (i / n_queues) % n_priorities)
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
            let (q_idx, p_idx) = round_robin_indices(i, n_queues, n_priorities);
            let queue = &queues[q_idx as usize];
            let priority = priorities[p_idx as usize];
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn round_robin_indices_cover_full_cartesian_product_when_gcd_gt_one() {
        // Regression test for the bug this function was extracted to fix: 2 queues x
        // 4 priorities has gcd=2, which used to make queue 0 only ever pair with 2 of
        // the 4 priorities (and queue 1 only the other 2), silently starving half of
        // the priority-expanded Redis keys.
        let (n_queues, n_priorities) = (2u64, 4u64);
        let mut seen = HashSet::new();
        for i in 0..(n_queues * n_priorities) {
            seen.insert(round_robin_indices(i, n_queues, n_priorities));
        }
        assert_eq!(
            seen.len(),
            (n_queues * n_priorities) as usize,
            "every (queue, priority) combination must be hit exactly once per full cycle"
        );
    }

    #[test]
    fn round_robin_indices_cover_full_cartesian_product_across_gcds() {
        // (n_queues, n_priorities) pairs spanning gcd=1 (coprime), gcd>1, and the
        // degenerate n=1 cases.
        for (n_queues, n_priorities) in [
            (1u64, 1u64),
            (1, 4),
            (4, 1),
            (2, 2),
            (3, 4), // gcd = 1
            (4, 4), // gcd = 4 (worst case: fully correlated under the old bug)
            (6, 9), // gcd = 3
        ] {
            let mut seen = HashSet::new();
            for i in 0..(n_queues * n_priorities) {
                seen.insert(round_robin_indices(i, n_queues, n_priorities));
            }
            assert_eq!(
                seen.len(),
                (n_queues * n_priorities) as usize,
                "gcd({n_queues},{n_priorities}) case must still cover the full cartesian product"
            );
        }
    }

    #[test]
    fn round_robin_indices_distribution_is_even_over_multiple_cycles() {
        let (n_queues, n_priorities) = (2u64, 4u64);
        let n_jobs = 40u64; // 5 full cycles of 8
        let mut counts: HashMap<(u64, u64), u64> = HashMap::new();
        for i in 0..n_jobs {
            *counts
                .entry(round_robin_indices(i, n_queues, n_priorities))
                .or_insert(0) += 1;
        }
        assert_eq!(counts.len(), 8);
        for (combo, count) in &counts {
            assert_eq!(
                *count, 5,
                "40 jobs / 8 combinations must split exactly 5 each, combo={combo:?}"
            );
        }
    }

    #[test]
    fn round_robin_indices_single_queue_or_single_priority_is_pure_round_robin() {
        // n_queues=1 degenerates to plain priority round-robin; n_priorities=1
        // degenerates to plain queue round-robin. Both must still behave sanely.
        for i in 0..8u64 {
            assert_eq!(round_robin_indices(i, 1, 4).0, 0);
            assert_eq!(round_robin_indices(i, 1, 4).1, i % 4);
            assert_eq!(round_robin_indices(i, 4, 1).1, 0);
            assert_eq!(round_robin_indices(i, 4, 1).0, i % 4);
        }
    }
}

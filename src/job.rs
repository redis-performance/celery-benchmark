use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

// ── kombu Redis-transport priority key derivation ───────────────────────────────
//
// kombu/transport/redis.py, class `Channel` (celery/kombu @ master, verified
// 2026-08-13, kombu 5.6.2):
//   - `priority_steps = PRIORITY_STEPS` where `PRIORITY_STEPS = [0, 3, 6, 9]` (line 116,
//     bound as the class default at line 680).
//   - `sep = '\x06\x16'` (line 669) — two raw control bytes (ASCII ACK + SYN), not a
//     printable string.
//   - `_q_for_pri(self, queue, pri)` (lines 1063–1067):
//       pri = self.priority(pri)          # floors `pri` to the nearest step <= pri
//       return f"{queue}{sep}{pri}" if pri else queue
//   - `priority(self, n)` (lines 1069–1071): `steps[bisect(steps, n) - 1]` — floor to
//     the largest configured step that is <= n.
//
// So each logical queue expands to exactly 4 Redis keys: the bare queue name (priority
// step 0) and `queue\x06\x163`, `queue\x06\x166`, `queue\x06\x169`. A task's priority
// (0–9, from `properties.priority`) is floored to one of these 4 steps to pick the key.
pub const PRIORITY_STEPS: [u8; 4] = [0, 3, 6, 9];
pub const PRIORITY_SEP: &str = "\x06\x16";

/// Floor an arbitrary priority (0–9) to the nearest configured step.
/// Mirrors `Channel.priority()` (kombu/transport/redis.py:1069-1071).
pub fn priority_step(n: u8) -> u8 {
    let mut result = PRIORITY_STEPS[0];
    for &step in PRIORITY_STEPS.iter() {
        if step <= n {
            result = step;
        } else {
            break;
        }
    }
    result
}

/// Derive the Redis key kombu writes to (LPUSH) / reads from (BRPOP/RPOP) for a given
/// queue + priority. Mirrors `Channel._q_for_pri()` (kombu/transport/redis.py:1063-1067).
pub fn q_for_pri(queue: &str, priority: u8) -> String {
    let pri = priority_step(priority);
    if pri != 0 {
        format!("{queue}{PRIORITY_SEP}{pri}")
    } else {
        queue.to_string()
    }
}

/// Build the exact BRPOP key list kombu issues for a set of queues, in kombu's exact
/// iteration order: `[_q_for_pri(queue, pri) for pri in priority_steps for queue in
/// queues]` — priority is the OUTER loop, queue is the INNER loop
/// (kombu/transport/redis.py `MultiChannelPoller._brpop_start`, lines 999-1013).
/// This always produces `4 * queues.len()` keys, regardless of whether traffic actually
/// uses non-zero priorities (in practice, with Celery's `default_priority = 0`
/// (kombu/transport/virtual/base.py:471), only the bare-queue-name key is ever written
/// to unless the caller sets an explicit task priority).
pub fn brpop_keys(queues: &[String]) -> Vec<String> {
    PRIORITY_STEPS
        .iter()
        .flat_map(|&pri| queues.iter().map(move |q| q_for_pri(q, pri)))
        .collect()
}

// ── Celery task message envelope (protocol v2, kombu wire format) ──────────────────
//
// Outer envelope shape: kombu/transport/virtual/base.py `Channel.prepare_message()`
// (lines 770-781) produces {body, content-encoding, content-type, headers, properties};
// `Channel.basic_publish()` / `_inplace_augment_message()` (lines 610-635) then
// base64-encodes `body` (via `Channel.body_encoding = 'base64'`, line 458) and fills in
// `properties.delivery_tag` (a UUID4, `_next_delivery_tag` line 610-611) and
// `properties.delivery_info`.
//
// Task-specific `headers` / `body` / `properties` are built by
// celery/app/amqp.py `AMQP.as_task_v2()` (lines 326-418) — this is "protocol v2", the
// default since Celery 4.0 (`task_protocol = Option(2, ...)`,
// celery/app/defaults.py:295). `body` is the 3-tuple `(args, kwargs, embed)` where
// `embed = {"callbacks": ..., "errbacks": ..., "chain": ..., "chord": ...}`
// (celery/app/amqp.py:399-406), JSON-serialized (`task_serializer` default `'json'`,
// celery/app/defaults.py:314) and THEN base64-encoded as the outer `body` string.
// The whole envelope is JSON-serialized (`from kombu.utils.json import dumps, loads`,
// kombu/transport/redis.py:87) before LPUSH.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CeleryEnvelope {
    /// base64(json.dumps([args, kwargs, embed]))
    pub body: String,
    #[serde(rename = "content-encoding")]
    pub content_encoding: String,
    #[serde(rename = "content-type")]
    pub content_type: String,
    pub headers: TaskHeaders,
    pub properties: TaskProperties,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TaskHeaders {
    pub lang: String,
    pub task: String,
    pub id: String,
    pub shadow: Option<String>,
    pub eta: Option<String>,
    pub expires: Option<String>,
    pub group: Option<String>,
    pub group_index: Option<u32>,
    pub retries: u32,
    pub timelimit: (Option<u64>, Option<u64>),
    pub root_id: String,
    pub parent_id: Option<String>,
    pub argsrepr: String,
    pub kwargsrepr: String,
    pub origin: String,
    pub ignore_result: bool,
    pub replaced_task_nesting: u32,
    pub stamped_headers: Option<Vec<String>>,
    pub stamps: Value,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TaskProperties {
    pub correlation_id: String,
    pub reply_to: String,
    pub body_encoding: String,
    pub delivery_tag: String,
    pub delivery_info: DeliveryInfo,
    /// Raw requested priority (0-9), NOT floored — the floor only affects key selection
    /// (see `q_for_pri`). Mirrors `_get_message_priority()`
    /// (kombu/transport/virtual/base.py:854-872), clamped to [min_priority=0,
    /// max_priority=9] (lines 471-472).
    pub priority: u8,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DeliveryInfo {
    pub exchange: String,
    pub routing_key: String,
}

impl CeleryEnvelope {
    /// Build a structurally realistic Celery protocol-v2 task message.
    ///
    /// `enqueued_at_ns` is injected as a task kwarg (`enqueued_at_ns`) — NOT part of
    /// the real Celery/kombu wire protocol. This mirrors sidekiq-benchmark's own
    /// `args[3]` timestamp-injection pattern (src/job.rs there): a benchmark-only
    /// payload field used purely to measure dequeue latency, chosen to sit in a
    /// position (a task kwarg) that a real task payload could plausibly carry.
    pub fn new(queue: &str, priority: u8, idx: u64) -> Self {
        let task_id = Uuid::new_v4().to_string();
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before UNIX_EPOCH")
            .as_nanos() as u64;

        let args: Value = json!([]);
        let kwargs: Value = json!({ "enqueued_at_ns": now_ns, "idx": idx });
        let embed: Value = json!({
            "callbacks": null,
            "errbacks": null,
            "chain": null,
            "chord": null,
        });
        let inner = json!([args, kwargs, embed]);
        let inner_json = serde_json::to_string(&inner).expect("inner body serializes");
        let body = BASE64.encode(inner_json.as_bytes());

        CeleryEnvelope {
            body,
            content_encoding: "utf-8".to_string(),
            content_type: "application/json".to_string(),
            headers: TaskHeaders {
                lang: "py".to_string(),
                task: "celery_bench.tasks.noop".to_string(),
                id: task_id.clone(),
                shadow: None,
                eta: None,
                expires: None,
                group: None,
                group_index: None,
                retries: 0,
                timelimit: (None, None),
                root_id: task_id.clone(),
                parent_id: None,
                argsrepr: "()".to_string(),
                kwargsrepr: format!("{{'enqueued_at_ns': {now_ns}, 'idx': {idx}}}"),
                origin: format!("gen{}@celery-bench", std::process::id()),
                ignore_result: false,
                replaced_task_nesting: 0,
                stamped_headers: None,
                stamps: json!({}),
            },
            properties: TaskProperties {
                correlation_id: task_id.clone(),
                reply_to: String::new(),
                body_encoding: "base64".to_string(),
                delivery_tag: Uuid::new_v4().to_string(),
                delivery_info: DeliveryInfo {
                    exchange: String::new(),
                    routing_key: queue.to_string(),
                },
                priority,
            },
        }
    }

    /// Extract the benchmark-injected `enqueued_at_ns` kwarg for latency measurement.
    /// Returns `None` if the envelope doesn't carry our marker (e.g. a message enqueued
    /// by something other than this tool).
    pub fn enqueued_at_ns(&self) -> Option<u64> {
        let decoded = BASE64.decode(self.body.as_bytes()).ok()?;
        let inner: Value = serde_json::from_slice(&decoded).ok()?;
        inner.as_array()?.get(1)?.get("enqueued_at_ns")?.as_u64()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn priority_step_floors_to_nearest_configured_step() {
        assert_eq!(priority_step(0), 0);
        assert_eq!(priority_step(1), 0);
        assert_eq!(priority_step(2), 0);
        assert_eq!(priority_step(3), 3);
        assert_eq!(priority_step(4), 3);
        assert_eq!(priority_step(5), 3);
        assert_eq!(priority_step(6), 6);
        assert_eq!(priority_step(8), 6);
        assert_eq!(priority_step(9), 9);
    }

    #[test]
    fn priority_step_exact_step_boundaries() {
        // One below every configured step must floor DOWN to the previous step, and
        // the step value itself must floor to itself — the two cases most likely to
        // be off-by-one in a bisect-style implementation.
        for &step in PRIORITY_STEPS.iter() {
            assert_eq!(
                priority_step(step),
                step,
                "step {step} must floor to itself"
            );
        }
        assert_eq!(priority_step(2), 0, "one below step 3 floors to step 0");
        assert_eq!(priority_step(5), 3, "one below step 6 floors to step 3");
        assert_eq!(priority_step(8), 6, "one below step 9 floors to step 6");
    }

    #[test]
    fn priority_step_beyond_cli_range_still_floors_correctly() {
        // --priorities is CLI-validated to 0-9, but `priority_step` itself is a plain
        // pub fn with no such restriction — defend it directly against out-of-range
        // callers (u8's full domain is 0-255) rather than relying on the CLI gate.
        assert_eq!(priority_step(10), 9);
        assert_eq!(priority_step(100), 9);
        assert_eq!(priority_step(u8::MAX), 9);
    }

    #[test]
    fn q_for_pri_zero_priority_returns_bare_queue_name() {
        // Celery's default_priority = 0 (kombu/transport/virtual/base.py:471) — the
        // common case in production must resolve to the bare queue name, no suffix.
        assert_eq!(q_for_pri("celery", 0), "celery");
        assert_eq!(q_for_pri("celery", 1), "celery"); // floors to step 0
        assert_eq!(q_for_pri("celery", 2), "celery");
    }

    #[test]
    fn q_for_pri_nonzero_priority_appends_control_byte_separator() {
        assert_eq!(q_for_pri("celery", 3), "celery\x06\x163");
        assert_eq!(q_for_pri("celery", 5), "celery\x06\x163"); // floors to 3
        assert_eq!(q_for_pri("celery", 6), "celery\x06\x166");
        assert_eq!(q_for_pri("celery", 9), "celery\x06\x169");
    }

    #[test]
    fn brpop_keys_single_queue_matches_kombu_priority_outer_order() {
        let queues = vec!["celery".to_string()];
        let keys = brpop_keys(&queues);
        assert_eq!(
            keys,
            vec![
                "celery",
                "celery\x06\x163",
                "celery\x06\x166",
                "celery\x06\x169"
            ]
        );
    }

    #[test]
    fn brpop_keys_multi_queue_iterates_priority_outer_queue_inner() {
        // Matches kombu's `[_q_for_pri(q, pri) for pri in priority_steps for q in queues]`
        let queues = vec!["celery_0".to_string(), "celery_1".to_string()];
        let keys = brpop_keys(&queues);
        assert_eq!(keys.len(), 8);
        assert_eq!(keys[0], "celery_0");
        assert_eq!(keys[1], "celery_1");
        assert_eq!(keys[2], "celery_0\x06\x163");
        assert_eq!(keys[3], "celery_1\x06\x163");
        assert_eq!(keys[6], "celery_0\x06\x169");
        assert_eq!(keys[7], "celery_1\x06\x169");
    }

    #[test]
    fn envelope_roundtrips_enqueued_at_ns_through_json_and_base64() {
        let env = CeleryEnvelope::new("celery", 0, 42);
        let json = serde_json::to_string(&env).unwrap();
        let back: CeleryEnvelope = serde_json::from_str(&json).unwrap();
        let ts = back.enqueued_at_ns();
        assert!(ts.is_some());
        assert!(ts.unwrap() > 0);
    }

    #[test]
    fn envelope_priority_is_stored_raw_not_floored() {
        let env = CeleryEnvelope::new("celery", 5, 0);
        // properties.priority keeps the raw requested value...
        assert_eq!(env.properties.priority, 5);
        // ...only the derived Redis key is floored to the nearest step.
        assert_eq!(
            q_for_pri("celery", env.properties.priority),
            "celery\x06\x163"
        );
    }

    #[test]
    fn envelope_body_is_valid_base64_json_three_tuple() {
        let env = CeleryEnvelope::new("celery", 0, 7);
        let decoded = BASE64.decode(env.body.as_bytes()).unwrap();
        let inner: Value = serde_json::from_slice(&decoded).unwrap();
        let arr = inner.as_array().unwrap();
        assert_eq!(arr.len(), 3); // [args, kwargs, embed]
        assert!(arr[0].as_array().unwrap().is_empty()); // args = []
        assert_eq!(arr[1]["idx"].as_u64().unwrap(), 7); // kwargs.idx
        assert!(arr[2]["callbacks"].is_null()); // embed
    }
}

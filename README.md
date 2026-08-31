# celery-benchmark

A Celery/kombu protocol load benchmark written in Rust. Measures task throughput and
full latency spectrum (p50→p99.99) against any Redis broker endpoint.

## Why Rust?

| | Python `celery worker` (prefork) | This tool |
|---|---|---|
| Concurrency model | OS processes (prefork pool) — heavy, capped by core count | tokio async tasks — scales far past core count |
| Latency recording | None built in | HDRHistogram per task (p50→p99.99) |
| Per-second time series | None | throughput + latency percentiles + errors |
| Multi-queue | Manual `-Q` config per process | `--num-queues N` (round-robin distribution) |
| Dependency | Full Celery + kombu + a broker client | Single static binary |

## Protocol compatibility

There's no official Rust client for Celery or kombu — the closest thing,
[`rusty-celery`](https://github.com/rusty-celery/rusty-celery), is an unofficial,
largely dormant community project. Instead of depending on an abstraction that may
not track kombu's actual wire behavior, this tool talks directly to Redis with the
[`redis`](https://crates.io/crates/redis) crate and reimplements kombu's Redis
transport protocol at the key/command level. Every claim below cites the exact
kombu/Celery source (verified against the `master`/`main` branches on 2026-08-13,
kombu 5.6.2).

### Priority-expanded queue keys

kombu's Redis transport doesn't map one queue to one Redis key. Each logical queue
expands to **4 keys**, one per priority step:

- `kombu/transport/redis.py`, module level: `PRIORITY_STEPS = [0, 3, 6, 9]`
- `kombu/transport/redis.py`, class `Channel`: `sep = '\x06\x16'` — two raw control
  bytes (ASCII ACK + SYN), not a printable separator
- `kombu/transport/redis.py`, `Channel._q_for_pri(queue, pri)`:
  ```python
  pri = self.priority(pri)                      # floor to nearest configured step
  return f"{queue}{sep}{pri}" if pri else queue  # step 0 = bare queue name
  ```
- `kombu/transport/redis.py`, `Channel.priority(n)`: `steps[bisect(steps, n) - 1]` —
  floors an arbitrary 0-9 priority down to the nearest step

So a queue named `celery` expands to `celery`, `celery\x06\x163`, `celery\x06\x166`,
`celery\x06\x169`. Producers `LPUSH` onto whichever of the 4 keys the task's priority
floors to (`Channel._put`, same file, lines 1073-1086: `client.lpush(key,
dumps(message))`). Consumers issue one multi-key `BRPOP` across **all 4 keys per
queue they consume from**, regardless of whether traffic ever uses non-zero
priorities (`MultiChannelPoller._brpop_start`, same file, lines 999-1013):

```python
keys = [self._q_for_pri(queue, pri) for pri in self.priority_steps
        for queue in queues] + [timeout or 0]
command_args = ['BRPOP', *keys]
```

Note the iteration order: **priority is the outer loop, queue is the inner loop.**
With `--num-queues N`, this tool's worker issues `BRPOP` across `4*N` keys in that
exact order (`src/job.rs::brpop_keys`).

Celery's own default task priority is 0 (`kombu/transport/virtual/base.py:471`,
`default_priority = 0`), so **out of the box, only the bare-queue-name key is ever
written to** — the other 3 keys sit empty and are polled for nothing. `--priorities`
(default `"0"`, matching that real default) lets you round-robin jobs across other
priority values to exercise the full 4-key spread.

### BRPOP timeout

`kombu/transport/redis.py`, class `Transport`: `brpop_timeout = 1` (seconds),
overridable via `broker_transport_options={'polling_interval': N}` — same file,
`Transport.__init__`: `if self.polling_interval is not None: self.brpop_timeout =
self.polling_interval`. `--brpop-timeout-secs` (default `1`) mirrors this.

### Companion bookkeeping — intentionally omitted (steady-state, not just recovery)

kombu's Redis transport does client-side ack emulation by default
(`Channel.ack_emulation = True`, `kombu/transport/redis.py:673`), backed by two
extra keys per channel: a `unacked` hash and an `unacked_index` sorted set
(`unacked_key = 'unacked'`, `unacked_index_key = 'unacked_index'`, same file, lines
674-675). This is **not** an error-recovery-only path — with Celery's default
`no_ack=False` consumer setting, every dequeue triggers a `ZADD unacked_index` +
`HSET unacked` pair (`class QoS`, `append()`, lines 374-389, both pipelined), and
every ack (which fires immediately after receipt unless `task_acks_late=True`)
triggers a `ZREM unacked_index` + `HDEL unacked` pair (`_remove_from_indices()`,
lines 416-419). In other words, real production Celery/kombu does roughly **5 Redis
ops per task** (`LPUSH` + `BRPOP` + `ZADD` + `HSET` + `ZREM` + `HDEL` — 6, minus the
LPUSH/BRPOP pair already counted) around the 2-op queue mechanics this tool
measures, not 2.

This tool intentionally omits `unacked`/`unacked_index` — same "queue mechanics in
isolation" philosophy as sidekiq-benchmark's own omissions (see "Intentionally
omitted" below), just called out explicitly here because, unlike Sidekiq's
omitted keys, this one sits squarely in Celery's default hot path rather than only
firing on crash recovery.

### Task message envelope (protocol v2)

The outer envelope shape comes from `kombu/transport/virtual/base.py`,
`Channel.prepare_message()` (lines 770-781) and `basic_publish()` /
`_inplace_augment_message()` (lines 610-635):

```python
{
  "body": "<base64>",             # body_encoding = 'base64' (line 458)
  "content-encoding": "utf-8",
  "content-type": "application/json",
  "headers": {...},
  "properties": {
    "correlation_id": ..., "reply_to": ..., "body_encoding": "base64",
    "delivery_tag": "<uuid4>", "delivery_info": {"exchange": ..., "routing_key": ...},
    "priority": 0,
  },
}
```

`headers` and the inner `body` tuple are built by `celery/app/amqp.py`,
`AMQP.as_task_v2()` (lines 326-418) — "protocol v2", the default since Celery 4.0
(`task_protocol = Option(2, ...)`, `celery/app/defaults.py:295`):

```python
headers = {
    'lang': 'py', 'task': name, 'id': task_id, 'shadow': shadow,
    'eta': eta, 'expires': expires, 'group': group_id, 'group_index': group_index,
    'retries': retries, 'timelimit': [time_limit, soft_time_limit],
    'root_id': root_id, 'parent_id': parent_id,
    'argsrepr': argsrepr, 'kwargsrepr': kwargsrepr, 'origin': origin,
    'ignore_result': ignore_result, 'replaced_task_nesting': replaced_task_nesting,
    'stamped_headers': stamped_headers, 'stamps': stamps,
}
body = (args, kwargs, {'callbacks': ..., 'errbacks': ..., 'chain': ..., 'chord': ...})
```

`body` is JSON-serialized (`task_serializer = 'json'` default,
`celery/app/defaults.py:314`) and then base64-encoded as the outer envelope's
`body` field. The whole envelope is JSON-serialized (`from kombu.utils.json import
dumps, loads`, `kombu/transport/redis.py:87`) before `LPUSH`. `src/job.rs`
(`CeleryEnvelope`) reproduces this exact nesting: JSON envelope → base64 `body` →
JSON 3-tuple `[args, kwargs, embed]`.

Default queue name: `celery` (`celery/app/defaults.py:283`,
`default_queue=Option('celery')`) — this tool's `--queue` default matches it exactly.

### Latency marker — not part of the real protocol

To measure dequeue latency, this tool injects `enqueued_at_ns` (nanoseconds since
epoch) as a task kwarg — a benchmark-only field, not something real Celery/kombu
puts on the wire. This mirrors sidekiq-benchmark's own `args[3]` timestamp-injection
pattern (`sidekiq-benchmark/src/job.rs`): pick a payload position a real task could
plausibly carry data in, and use it purely for benchmark instrumentation.

## Quick start

### Docker Hub

> **Memory:** the default run pre-fills 500,000 tasks against a Celery
> protocol-v2 envelope (headers + properties + base64 body — a few hundred bytes
> each; the exact size is measured and logged before each run). Use `--jobs 50000`
> for a quick local smoke test.

```bash
# Run against local Redis (default: db 13, 500k jobs, workers 10/50/100/200)
docker run --rm --network host redis/celery-benchmark

# Lighter local run
docker run --rm --network host redis/celery-benchmark \
  --workers 10,50 --jobs 50000

# Custom settings
docker run --rm --network host redis/celery-benchmark \
  --url redis://127.0.0.1:6379/0 \
  --workers 10,50,100 \
  --jobs 100000 \
  --num-queues 4

# Point at a remote Redis
docker run --rm redis/celery-benchmark \
  --url redis://myhost:6379/0 \
  --workers 50,100,200 \
  --jobs 500000 \
  --output -
```

### docker compose (Redis included)

```bash
# Start Redis + run benchmark
docker compose run --rm bench

# Use a different Redis image
REDIS_IMAGE=redis:7.4 docker compose run --rm bench

# Point at an external Redis
REDIS_URL=redis://myhost:6379/0 docker compose run --rm bench
```

### Install from GitHub Release

Pre-built static binaries for `x86_64` and `aarch64` Linux are attached to every
[release](https://github.com/redis-performance/celery-benchmark/releases), each with
a `.sha256` checksum alongside it:

```bash
# Pick one target: x86_64-unknown-linux-gnu or aarch64-unknown-linux-gnu
TARGET=x86_64-unknown-linux-gnu
VERSION=v0.1.0

curl -sLO https://github.com/redis-performance/celery-benchmark/releases/download/$VERSION/celery-bench-$TARGET.tar.gz
curl -sLO https://github.com/redis-performance/celery-benchmark/releases/download/$VERSION/celery-bench-$TARGET.tar.gz.sha256
sha256sum -c celery-bench-$TARGET.tar.gz.sha256

tar xzf celery-bench-$TARGET.tar.gz
./celery-bench-$TARGET/celery-bench --help
```

### From source

```bash
cargo build --release
./target/release/celery-bench --workers 5 --jobs 10000
```

## CLI flags

| Flag | Env | Default | Notes |
|---|---|---|---|
| `--url` | `REDIS_URL` | `redis://127.0.0.1:6379/13` | Full Redis URL |
| `--host` | — | — | Override host component of URL |
| `--port` | — | — | Override port component of URL |
| `--password` | `REDIS_PASSWORD` | — | Auth (prefer env var — CLI exposes it in `ps`) |
| `--tls` | `REDIS_TLS` | false | Enable TLS (`rediss://`) |
| `--insecure` | `REDIS_TLS_INSECURE` | false | Skip TLS certificate verification (only meaningful with `--tls` / a `rediss://` URL) — for self-signed or private-CA certs, see "TLS" below |
| `--db` | — | `13` | Database number (tool safety convention — NOT a Celery default, see below) |
| `--workers` | — | `10,50,100,200` | Comma-separated concurrency levels — one trial each; each level runs that many independent BRPOP loops (one dedicated connection each) |
| `--jobs` | — | `500000` | Total task messages per trial |
| `--warmup-jobs` | — | `0` | Warmup pass before each trial (0 = skip) |
| `--queue` | — | `celery` | Base queue name — matches Celery's real default (`task_default_queue`) |
| `--num-queues` | — | `1` | Number of queues (jobs distributed round-robin); names are `<queue>_0…<queue>_{N-1}` when N > 1 |
| `--priorities` | — | `0` | Comma-separated task priorities (0-9) to round-robin across; default matches Celery's real `default_priority` (every task lands on the bare queue key) |
| `--brpop-timeout-secs` | — | `1` | BRPOP timeout — matches kombu's `Transport.brpop_timeout` default |
| `--latency-percentiles` | — | `p50,p90,p99,p999,max` | Per-second latency series to record; supports `p50`, `p75`, `p90`, `p95`, `p99`, `p999`, `p9999`, `max`, `mean` |
| `--tag` | — | from Redis `INFO` | Label for output filename and JSON |
| `--output` | — | `celery_bench_<tag>.json` | JSON output path; `-` for stdout |
| `--timeout` | — | `300` | Per-trial timeout in seconds |
| `--quiet` | — | false | Suppress per-second progress dots |
| `--allow-flushdb` | `CELERY_BENCH_ALLOW_FLUSHDB` | false | FLUSHDB before each trial (default: DEL only the priority-expanded queue keys — safe on shared Redis) |

### TLS

`--tls` upgrades the connection URL's scheme to `rediss://`. By default this validates
the server's certificate chain and hostname against the bundled Mozilla root CAs
(`tls-rustls-webpki-roots`), same as any well-behaved TLS client.

Test/staging/ephemeral benchmark deployments frequently present a self-signed or
private-CA certificate that won't validate against those public roots. For that case,
pass `--insecure` (or set `REDIS_TLS_INSECURE=1`) alongside `--tls` to skip certificate
verification entirely:

```bash
celery-bench --tls --insecure --host my-staging-redis --port 6380
```

Under the hood this appends the `#insecure` fragment to the connection URL, which is
`redis-rs`'s own documented escape hatch (`rediss://host:port/#insecure`) — so passing
`--url rediss://host:port/0#insecure` directly works too, without `--tls`/`--insecure`.
This requires the `redis` crate's `tls-rustls-insecure` cargo feature, which this crate
enables. Without it, the fragment is **not** silently ignored — verified directly
against `redis-1.5.0`'s source (the version pinned in this crate's `Cargo.lock`):

- Parsing `#insecure` into `ConnectionAddr::TcpTls { insecure: true, .. }` is gated on
  `tls-rustls`/`tls-native-tls` (i.e. TLS support being compiled in at all), **not** on
  `tls-rustls-insecure` — `src/connection.rs:548-563`. So the fragment always parses
  successfully and always sets that flag, feature or no feature.
- What actually reads that flag, `create_rustls_config`, is gated on `tls-rustls`
  (`src/connection.rs:1166-1167`) and hard-fails the connection attempt when
  `insecure` is `true` but `tls-rustls-insecure` is off:
  `Err("Cannot create insecure client without tls-rustls-insecure feature")`
  (`src/connection.rs:1274-1279`).

So dropping `tls-rustls-insecure` doesn't make `--insecure` a no-op — it makes every
`--insecure` (or bare `#insecure`) connection attempt fail outright with that error,
turning "skip verification" into "cannot connect at all". Either way it's a trap when
wiring up `redis-rs` TLS by hand — just not a *silent* one.

**`--insecure` disables validation of the server's certificate chain and hostname.**
Only use it against trusted networks (e.g. a benchmark's own private VPC), never
across the public internet or against a server you don't control.

**Crypto provider:** `redis-rs`'s rustls integration deliberately doesn't pick a
crypto backend for you (its own `rustls` dependency is `default-features = false`) —
`rustls` 0.23 requires the *application* to install one process-wide `CryptoProvider`
before any TLS connection is attempted, or every TLS connection (`--tls`, with or
without `--insecure`) panics. This crate depends directly on `rustls` (with the
`ring` feature — unused in our own code, its only purpose is to make Cargo enable
that feature for the one resolved `rustls` package) and installs it at the top of
`main()`.

### Multi-queue mode

```bash
# Single queue
celery-bench --workers 100 --jobs 500000 --num-queues 1

# 8 queues — worker BRPOPs across 32 keys (4 priority steps × 8 queues)
celery-bench --workers 100 --jobs 500000 --num-queues 8
```

### Exercising the full priority spread

```bash
# Round-robin across all 4 priority steps kombu's BRPOP always polls
celery-bench --workers 50 --jobs 200000 --priorities 0,3,6,9
```

## Output

**Console:**
```
=== celery-bench — redis-8.0 ===
    redis://127.0.0.1:6379/13  jobs=500,000  queues=celery  priorities=[0]

  [  10 workers] ........  10,240 jobs/s  p50=480 µs  p99=2.6 ms  p99.9=6.1 ms  max=48 ms
  [  50 workers] ........  16,910 jobs/s  p50=2.3 ms  p99=9.1 ms  p99.9=13 ms   max=36 ms

--- Summary ---
+---------+--------+--------+--------+---------+---------+--------+
| Workers | jobs/s | p50    | p99    | p99.9   | max     | errors |
+---------+--------+--------+--------+---------+---------+--------+
|      10 | 10,240 | 480 µs | 2.6 ms | 6.1 ms  | 48 ms   | 0      |
|      50 | 16,910 | 2.3 ms | 9.1 ms | 13 ms   | 36 ms   | 0      |
+---------+--------+--------+--------+---------+---------+--------+
Results saved → celery_bench_redis-8.0.json
```

Progress shows `.` per second, or `[e:N]` when errors occur in that window so
nothing is silently swallowed.

**JSON** (`celery_bench_<tag>.json`) — schema-compatible with sidekiq-benchmark's
output, so results from both tools are directly comparable:

```json
{
  "tag": "redis-8.0",
  "timestamp": "2026-08-13T01:30:00Z",
  "config": {
    "url": "redis://127.0.0.1:6379/13",
    "workers": [10, 50, 100, 200],
    "jobs_per_trial": 500000,
    "queues": ["celery"],
    "warmup_jobs": 0
  },
  "results": [{
    "workers": 10,
    "total_jobs": 500000,
    "duration_s": 48.83,
    "jobs_per_sec": 10240.1,
    "timed_out": false,
    "throughput_per_sec": [10300, 10250, 10180],
    "errors_per_sec":     [0, 0, 0],
    "latency_per_sec_us": {
      "p50":  [470, 480, 475],
      "p90":  [900, 915, 890],
      "p99":  [2550, 2600, 2580],
      "p999": [6000, 6100, 6050],
      "max":  [47000, 48000, 46500]
    },
    "latency_us": {
      "p50": 480, "p75": 650, "p90": 900,
      "p95": 940, "p99": 2600, "p99_9": 6100,
      "p99_99": 13000, "max": 48000,
      "mean": 540.1, "total_count": 500000
    },
    "errors": 0
  }]
}
```

All latency values are in **microseconds**. `latency_per_sec_us` contains one value
per elapsed second of the trial, making it easy to plot latency stability over time
or spot degradation as the queue drains.

> **Note on latency:** the benchmark pre-fills the queue then starts workers.
> Latency = time a task spends in the queue until dequeued (wall-clock, same host as
> producer). Workers dequeue via **BRPOP** across all 4 priority-expanded keys per
> queue (real kombu protocol).

> **Password safety:** passwords passed via `--password` are visible in `ps aux`.
> Prefer the `REDIS_PASSWORD` environment variable. Passwords are redacted (`****`)
> in all output and JSON.

## Safety notes

### Default database: 13

The default Redis database is **13** — this is this tool's own safety convention
(matching sidekiq-benchmark's), **not** a Celery default. Real Celery/kombu defaults
to db 0 (`redis://localhost:6379/0`). Using 13 here avoids colliding with
application data and makes `--allow-flushdb` safe by default. Always confirm the
target db before running against a shared Redis.

### Shared / production Redis

Do **not** run this benchmark against a production Redis instance. The benchmark
pre-fills the queue with hundreds of thousands of task messages and (optionally)
flushes the entire database. Use a dedicated benchmark instance or an isolated
database number.

### Intentionally omitted protocol behavior

- **`unacked` / `unacked_index` ack-emulation bookkeeping** — see "Companion
  bookkeeping" above. Unlike most "intentionally omitted" housekeeping in tools like
  this, this one genuinely sits on Celery's default steady-state path (2 extra ops
  on dequeue, 2 more on ack), so treat this tool's throughput numbers as an
  **upper bound** relative to a real Celery deployment with `ack_emulation` at its
  default (`True`).
- **Task execution semantics** — no actual task body runs; this tool measures queue
  mechanics (enqueue throughput + BRPOP dequeue latency) in isolation, same
  philosophy as sidekiq-benchmark.
- **Fanout / pub-sub broadcast, result backend, retries, ETA/countdown scheduling,
  chords/chains/groups** — out of scope; the envelope's `body` embed always carries
  null `callbacks`/`errbacks`/`chain`/`chord`.

## Building

Requires Rust stable (1.75+).

```bash
cargo build --release
cargo test
cargo clippy -- -D warnings
```

## Docker image

Multi-platform image (`linux/amd64`, `linux/arm64`) published to
[`redis/celery-benchmark`](https://hub.docker.com/r/redis/celery-benchmark) on every
push to `main`. Tagged `latest` on main; semver tags (`1.0.0`, `1.0`) on `v*` git
tags.

```bash
# Pull and run
docker pull redis/celery-benchmark
docker run --rm --network host redis/celery-benchmark --workers 10 --jobs 50000

# Build locally
docker build -t celery-bench .
docker run --rm celery-bench --url redis://host:6379/0 --workers 10 --jobs 50000
```

## License

Apache-2.0

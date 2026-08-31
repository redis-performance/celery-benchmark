use anyhow::{Context, Result};
use celery_bench::job::{self, CeleryEnvelope};
use celery_bench::metrics::{LatencyStats, Metrics, TrialResult};
use celery_bench::worker::LoadWorker;
use celery_bench::{producer, report};
use clap::Parser;
use hdrhistogram::Histogram;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "celery-bench",
    version,
    about = "Celery/kombu protocol load benchmark — measures task throughput and latency against any Redis broker endpoint"
)]
struct Cli {
    /// Redis URL (takes precedence over --host/--port).
    /// Defaults to db 13 for the same reason sidekiq-benchmark does: it's an unlikely
    /// application database, so --allow-flushdb is safe by default. NOTE: this is our
    /// own tool convention, not a Celery default — real Celery/kombu defaults to db 0
    /// (redis://localhost:6379/0).
    #[arg(long, env = "REDIS_URL", default_value = "redis://127.0.0.1:6379/13")]
    url: String,

    /// Override host in the Redis URL
    #[arg(long)]
    host: Option<String>,

    /// Override port in the Redis URL
    #[arg(long)]
    port: Option<u16>,

    /// Redis password — prefer REDIS_PASSWORD env var; passing on CLI exposes it in process list
    #[arg(long, env = "REDIS_PASSWORD")]
    password: Option<String>,

    /// Enable TLS (upgrades scheme to rediss://)
    #[arg(long, env = "REDIS_TLS")]
    tls: bool,

    /// Skip TLS certificate verification (only meaningful with --tls). Use this against
    /// servers presenting a self-signed or private-CA certificate — the normal case for
    /// test/staging/ephemeral benchmark deployments. INSECURE: disables validation of the
    /// server's certificate chain and hostname, so only use this against trusted networks.
    #[arg(long, env = "REDIS_TLS_INSECURE")]
    insecure: bool,

    /// Redis database number. When omitted, the db in --url is used, falling back to 13
    /// (the Ruby sidekiqload safety default). Note db > 0 does not exist on Redis Cluster
    /// or most managed Redis, so `--db 0` is usually required against those.
    #[arg(long)]
    db: Option<u8>,

    /// Comma-separated concurrency levels — each becomes a separate trial.
    /// Each level spawns that many independent BRPOP loops, one dedicated Redis
    /// connection each — mirroring N real Celery worker processes.
    #[arg(long, default_value = "10,50,100,200", value_delimiter = ',')]
    workers: Vec<usize>,

    /// Total task messages per trial
    #[arg(long, default_value = "500000")]
    jobs: u64,

    /// Task messages for warmup run before each trial (0 = skip)
    #[arg(long, default_value = "0")]
    warmup_jobs: u64,

    /// Base Celery queue name. Defaults to "celery" — Celery's real default
    /// (`task_default_queue`, celery/app/defaults.py:283).
    #[arg(long, default_value = "celery")]
    queue: String,

    /// Number of queues to distribute jobs across (1 = single queue, matching Celery's
    /// out-of-the-box single-queue setup). Queue names are generated as
    /// <queue>_0, <queue>_1, … when > 1.
    #[arg(long, default_value = "1")]
    num_queues: usize,

    /// Comma-separated task priorities (0-9) to round-robin across within each queue.
    /// Default "0" matches Celery's real default_priority
    /// (kombu/transport/virtual/base.py:471) — every task lands on the bare
    /// (priority-0) key. Pass e.g. "0,3,6,9" to exercise all 4 priority-expanded keys
    /// kombu's BRPOP always polls (see README "Protocol compatibility").
    #[arg(long, default_value = "0", value_delimiter = ',')]
    priorities: Vec<u8>,

    /// BRPOP timeout in seconds. Default 1 matches kombu's
    /// `Transport.brpop_timeout` (kombu/transport/redis.py:1436) — the value
    /// `broker_transport_options={'polling_interval': N}` overrides in real Celery.
    #[arg(long, default_value = "1")]
    brpop_timeout_secs: f64,

    /// Per-second latency percentiles to record (comma-separated).
    /// Supported values: p50, p75, p90, p95, p99, p999, p9999, max, mean.
    #[arg(long, default_value = "p50,p90,p99,p999,max", value_delimiter = ',')]
    latency_percentiles: Vec<String>,

    /// Label for output (defaults to redis_version from INFO)
    #[arg(long)]
    tag: Option<String>,

    /// Output file path, or '-' for stdout
    #[arg(long)]
    output: Option<String>,

    /// Per-trial timeout in seconds
    #[arg(long, default_value = "300")]
    timeout: u64,

    /// Suppress per-second progress output
    #[arg(long)]
    quiet: bool,

    /// Allow FLUSHDB before each trial (clears the entire database).
    /// Default: only deletes the specific priority-expanded queue keys, which is safe
    /// on shared Redis.
    #[arg(long, env = "CELERY_BENCH_ALLOW_FLUSHDB")]
    allow_flushdb: bool,
}

// ── Redis URL helpers ─────────────────────────────────────────────────────────

/// The parsed, lowercased scheme of `url`, or `None` if it doesn't parse as a URL at
/// all. `url::Url` lowercases the scheme on parse (RFC 3986), so this is what both
/// `validate_cli`'s "does --insecure make sense" check and `build_redis_url`'s "is
/// this already rediss://" check must compare against — a raw, case-sensitive
/// `starts_with("rediss://")` on the CLI-supplied string (validate_cli's previous
/// check) would reject a perfectly legal `REDISS://host:6380/0`, which
/// `url::Url::parse` accepts and normalizes fine.
fn parsed_scheme(url: &str) -> Option<String> {
    url::Url::parse(url).ok().map(|u| u.scheme().to_string())
}

/// True when `url` will resolve to a `rediss://` (TLS) connection once `build_redis_url`
/// is done with it: either it is already `rediss://` (any casing — see `parsed_scheme`),
/// or it is `redis://` and `--tls` is set (the only upgrade `build_redis_url` performs,
/// at line ~157). Deliberately does NOT special-case any other scheme (e.g. `unix://`) —
/// `--tls` never upgrades those, so `--tls --insecure --url unix:///tmp/redis.sock` must
/// resolve to `false` here.
///
/// Both `validate_cli` (is `--insecure` meaningful?) and `build_redis_url` (should the
/// `#insecure` fragment be set?) call this instead of duplicating the scheme check, so
/// the two can never disagree about which URLs end up as TLS.
fn resolves_to_tls_scheme(cli: &Cli) -> bool {
    match parsed_scheme(&cli.url) {
        Some(scheme) => scheme == "rediss" || (cli.tls && scheme == "redis"),
        None => false,
    }
}

fn build_redis_url(cli: &Cli) -> Result<String> {
    // Computed against the ORIGINAL --url scheme, before any of the mutations below
    // (host/port/scheme overrides) — matches what `validate_cli` already checked, so
    // the two functions can't disagree about whether this URL resolves to TLS.
    let insecure_applies = cli.insecure && resolves_to_tls_scheme(cli);

    let mut u = url::Url::parse(&cli.url)
        .with_context(|| format!("invalid Redis URL: {}", redact_url_best_effort(&cli.url)))?;

    if let Some(host) = &cli.host {
        u.set_host(Some(host))
            .map_err(|_| anyhow::anyhow!("invalid --host: {host}"))?;
    }
    if let Some(port) = cli.port {
        u.set_port(Some(port)).map_err(|_| {
            anyhow::anyhow!(
                "cannot set port on URL: {}",
                redact_url_best_effort(&cli.url)
            )
        })?;
    }
    if cli.tls && u.scheme() == "redis" {
        u.set_scheme("rediss")
            .map_err(|_| anyhow::anyhow!("cannot upgrade scheme to rediss"))?;
    }
    // redis-rs recognizes the `#insecure` URL fragment (on any rediss:// URL, whether it
    // arrived via --tls or was already rediss:// in --url) to skip certificate verification —
    // this requires the `tls-rustls-insecure` cargo feature to be enabled. Without it, the
    // fragment still parses (that part isn't feature-gated) but every connection attempt then
    // hard-fails at connect time instead of skipping verification (see the Cargo.toml comment
    // next to the `redis` dependency, and README "TLS").
    if insecure_applies {
        u.set_fragment(Some("insecure"));
    }
    if let Some(password) = &cli.password {
        // url::Url::set_password percent-encodes special characters (e.g. '@', '/', ':')
        u.set_password(Some(password)).map_err(|_| {
            anyhow::anyhow!(
                "cannot set password on URL: {}",
                redact_url_best_effort(&cli.url)
            )
        })?;
    }
    // An explicit --db overrides whatever db --url carries. This used to apply only when the
    // URL had NO path -- but the default --url is redis://127.0.0.1:6379/13, which always has
    // one, so --db was silently inert. That mattered well beyond the flag: db 13 does not exist
    // on Redis Cluster or most managed Redis, and `--db 0` (the documented way out) did nothing,
    // so the tool could not connect to those endpoints at all.
    if let Some(db) = cli.db {
        u.set_path(&format!("/{db}"));
    } else if u.path().trim_matches('/').is_empty() {
        u.set_path("/13");
    }

    Ok(u.to_string())
}

/// True when `url`'s fragment is exactly `insecure` — i.e. redis-rs will actually skip
/// TLS certificate verification for it (the `rediss://host:port/#insecure` escape
/// hatch it documents, see README "TLS"). This is checked against the FINAL built URL
/// rather than `cli.insecure` because a user-supplied `--url`/`REDIS_URL` can already
/// carry that fragment on its own, disabling verification without `--insecure` ever
/// being passed — that path deserves the same warning `--insecure` gets, not silence.
/// A URL that fails to parse is treated as not-insecure (`build_redis_url` already
/// fails fast on an unparsable URL before this would ever be called with one).
///
/// Requires BOTH the `rediss://` scheme AND the `#insecure` fragment — redis-rs only
/// ever consults the fragment on a `rediss://` URL, so e.g. `redis://host:6379/0#insecure`
/// (plain, cleartext scheme) never actually disables any TLS certificate check and must
/// not print the "TLS certificate verification is DISABLED" warning, which would be
/// misleading for a connection that was never using TLS in the first place.
fn url_disables_cert_verification(url: &str) -> bool {
    url::Url::parse(url)
        .ok()
        .map(|u| u.scheme() == "rediss" && u.fragment() == Some("insecure"))
        .unwrap_or(false)
}

/// Return the URL with the password replaced by **** for logging and JSON output.
fn redact_url(raw: &str) -> String {
    match url::Url::parse(raw) {
        Ok(mut u) => {
            if u.password().is_some() {
                let _ = u.set_password(Some("****"));
            }
            u.to_string()
        }
        Err(_) => raw.to_string(),
    }
}

/// Best-effort password redaction for use in error messages built from a URL that
/// might not be well-formed enough for `url::Url::parse` to succeed (that's usually
/// exactly why we're building an error message about it). Falls back to a manual
/// scan for `scheme://user:password@` userinfo syntax so a malformed but
/// credential-bearing --url never gets echoed verbatim into stderr / anyhow's error
/// chain. Never used for anything except human-facing error text.
fn redact_url_best_effort(s: &str) -> String {
    if let Ok(u) = url::Url::parse(s) {
        return redact_url(u.as_ref());
    }
    if let Some(scheme_end) = s.find("://") {
        let after_scheme = &s[scheme_end + 3..];
        if let Some(at) = after_scheme.find('@') {
            let userinfo = &after_scheme[..at];
            if let Some(colon) = userinfo.find(':') {
                let mut redacted = String::with_capacity(s.len());
                redacted.push_str(&s[..scheme_end + 3]);
                redacted.push_str(&userinfo[..colon]);
                redacted.push_str(":****@");
                redacted.push_str(&after_scheme[at + 1..]);
                return redacted;
            }
        }
    }
    s.to_string()
}

/// Sanitize a tag string to characters safe for use in filenames.
fn sanitize_tag(tag: &str) -> String {
    let s: String = tag
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect();
    if s.is_empty() {
        "unknown".to_string()
    } else {
        s
    }
}

/// Reject output paths containing '..' to prevent path traversal.
fn validate_output_path(path: &str) -> Result<()> {
    if path == "-" {
        return Ok(());
    }
    for component in std::path::Path::new(path).components() {
        if component == std::path::Component::ParentDir {
            anyhow::bail!("--output must not contain '..' segments: {path}");
        }
    }
    Ok(())
}

// ── Per-second latency percentile specs ──────────────────────────────────────

#[derive(Clone, Debug)]
enum PercentileSpec {
    Quantile { name: String, q: f64 },
    Max,
    Mean,
}

impl PercentileSpec {
    fn name(&self) -> &str {
        match self {
            Self::Quantile { name, .. } => name,
            Self::Max => "max",
            Self::Mean => "mean",
        }
    }

    fn value(&self, hist: &Histogram<u64>) -> u64 {
        if hist.is_empty() {
            return 0;
        }
        match self {
            Self::Quantile { q, .. } => hist.value_at_quantile(*q),
            Self::Max => hist.max(),
            Self::Mean => hist.mean() as u64,
        }
    }
}

/// Parse a percentile spec string: "p50" → 0.50, "p999" → 0.999, "max", "mean".
fn parse_percentile_spec(s: &str) -> Result<PercentileSpec> {
    match s {
        "max" => Ok(PercentileSpec::Max),
        "mean" => Ok(PercentileSpec::Mean),
        s if s.starts_with('p') => {
            let digits = &s[1..];
            anyhow::ensure!(!digits.is_empty(), "invalid percentile spec: '{s}'");
            // "p100" reads naturally as "the 100th percentile" but the pXYZ scheme
            // below treats digit COUNT as decimal placement (p50 -> 0.50, p999 ->
            // 0.999), so "p100" would silently parse as 100/1000 = 0.10 (the 10th
            // percentile) instead of erroring or meaning what it looks like it means.
            // Reject it explicitly rather than let a plausible-looking typo produce a
            // silently wrong (but in-range, so otherwise unvalidated) result.
            anyhow::ensure!(
                digits != "100",
                "'{s}' is ambiguous (parses as the 10th percentile under this tool's \
                 pXYZ scheme, not the 100th) — use 'max' for the 100th percentile"
            );
            let n: u64 = digits
                .parse()
                .with_context(|| format!("invalid percentile spec: '{s}'"))?;
            let divisor = 10u64.pow(digits.len() as u32);
            let q = n as f64 / divisor as f64;
            anyhow::ensure!(q > 0.0 && q <= 1.0, "percentile out of range (0, 1]: '{s}'");
            Ok(PercentileSpec::Quantile {
                name: s.to_string(),
                q,
            })
        }
        _ => anyhow::bail!("unknown percentile spec '{s}' — use p50, p99, p999, max, mean"),
    }
}

/// Generate queue names from a base name and count.
/// With n=1 returns `["celery"]`; with n=4 returns `["celery_0".."celery_3"]`.
fn make_queue_names(base: &str, n: usize) -> Vec<String> {
    if n <= 1 {
        vec![base.to_string()]
    } else {
        (0..n).map(|i| format!("{base}_{i}")).collect()
    }
}

async fn fetch_tag(url: &str) -> String {
    let client = match redis::Client::open(url) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("warning: could not build Redis client for tag lookup: {e}");
            return "unknown".to_string();
        }
    };
    match client.get_multiplexed_async_connection().await {
        Ok(mut conn) => {
            match redis::cmd("INFO")
                .arg("server")
                .query_async::<String>(&mut conn)
                .await
            {
                Ok(info) => {
                    for line in info.lines() {
                        if let Some(v) = line.strip_prefix("redis_version:") {
                            return format!("redis-{}", v.trim());
                        }
                    }
                    "unknown".to_string()
                }
                Err(e) => {
                    eprintln!("warning: could not fetch Redis INFO for tag: {e}");
                    "unknown".to_string()
                }
            }
        }
        Err(e) => {
            eprintln!("warning: could not connect to Redis for tag lookup: {e}");
            "unknown".to_string()
        }
    }
}

// ── Trial execution ───────────────────────────────────────────────────────────

struct TrialConfig<'a> {
    url: &'a str,
    queues: &'a [String],
    jobs: u64,
    brpop_timeout_secs: f64,
    timeout_secs: u64,
    quiet: bool,
    percentile_specs: &'a [PercentileSpec],
}

/// Floor on the histogram's upper bound (60s) — preserves the tool's historical
/// resolution for typical short trials regardless of --timeout.
const MIN_HIST_MAX_US: u64 = 60_000_000;
/// Ceiling on the histogram's upper bound (24h) — bounds memory even if --timeout is
/// given an extreme value; HDRHistogram memory scales with log2(range), not range
/// itself, so this is cheap, but an unbounded value is still worth capping.
const MAX_HIST_MAX_US: u64 = 24 * 3600 * 1_000_000;

/// `max_us` should cover the largest latency a value can realistically accumulate in
/// this trial — i.e. the per-trial timeout. A job can sit in the queue for up to
/// `timeout_secs` before the trial gives up, so a histogram bounded below that would
/// silently DROP (not merely clamp) any latency past the bound: `Histogram::record`
/// returns an Err on out-of-range values and callers here ignore it (`let _ =
/// hist.record(v)`), which would understate `max`/`p99.9` and undercount
/// `total_count` without any visible warning — exactly the kind of silently
/// misleading number this tool exists to avoid producing. Bounding by the trial
/// timeout (clamped to [MIN_HIST_MAX_US, MAX_HIST_MAX_US]) makes every reachable
/// latency value recordable.
fn empty_histogram(max_us: u64) -> Histogram<u64> {
    let bound = max_us.clamp(MIN_HIST_MAX_US, MAX_HIST_MAX_US);
    // HDRHistogram requires low >= 1; values are clamped to .max(1) before recording
    Histogram::<u64>::new_with_bounds(1, bound, 3).expect("valid histogram bounds")
}

async fn run_trial(cfg: &TrialConfig<'_>, n_workers: usize) -> Result<TrialResult> {
    let metrics = Arc::new(Metrics::new());
    let (done_tx, mut done_rx) = watch::channel(false);
    let done_tx = Arc::new(done_tx);
    let (latency_tx, latency_rx) = mpsc::unbounded_channel::<u64>();
    let hist_max_us = cfg.timeout_secs.saturating_mul(1_000_000);

    // Per-second latency windows are pulled by the monitor, not pushed on a
    // separate timer: each tick the monitor sends a oneshot responder over
    // `snapshot_tx`, and the collector replies with the current window histogram
    // and resets it. Driving the reset from the monitor's clock keeps the latency
    // window aligned with the throughput/error deltas measured on that same tick,
    // so latency_per_sec[i] and throughput_per_sec[i] describe the same second.
    let (snapshot_tx, snapshot_rx) = mpsc::unbounded_channel::<oneshot::Sender<Histogram<u64>>>();

    // Histogram collector — drains the latency channel and maintains BOTH the
    // trial-long HDR histogram (returned at end) and a rolling per-second
    // window histogram (cloned out and reset whenever the monitor requests it).
    // Workers send raw latency values through this single collector task rather
    // than contending on a shared Mutex<Histogram> — see worker.rs.
    let collector = tokio::spawn(async move {
        let mut hist = empty_histogram(hist_max_us);
        let mut per_sec_hist = empty_histogram(hist_max_us);
        let mut rx = latency_rx;
        let mut snapshot_rx = snapshot_rx;
        loop {
            tokio::select! {
                maybe_us = rx.recv() => {
                    match maybe_us {
                        Some(us) => {
                            let v = us.max(1);
                            let _ = hist.record(v);
                            let _ = per_sec_hist.record(v);
                        }
                        None => break,
                    }
                }
                Some(resp) = snapshot_rx.recv() => {
                    let _ = resp.send(per_sec_hist.clone());
                    per_sec_hist.reset();
                }
            }
        }
        hist
    });

    let client = redis::Client::open(cfg.url).context("invalid Redis URL for worker pool")?;

    let w = LoadWorker {
        metrics: metrics.clone(),
        latency_tx: latency_tx.clone(), // workers hold clones; main keeps the sentinel
        done_tx: done_tx.clone(),
        target_jobs: cfg.jobs,
    };

    // Full priority-expanded BRPOP key list — same shape as kombu's real
    // `_brpop_start` (see job::brpop_keys doc comment): 4 keys per queue.
    let keys = Arc::new(job::brpop_keys(cfg.queues));
    let shutdown = Arc::new(AtomicBool::new(false));

    // Start the clock before opening connections / spawning workers, not after: on a
    // fast local Redis with a small job count, the first spawned workers can drain the
    // whole queue (and fire done_tx) WHILE later workers are still being connected —
    // starting the clock afterward could then measure a near-zero duration and produce
    // a nonsensical jobs_per_sec. Including connection setup in `duration` is the
    // conservative trade-off (it's a fixed, usually-negligible cost against real trial
    // sizes) and eliminates the race entirely.
    let start = Instant::now();
    let mut join_set: JoinSet<Result<()>> = JoinSet::new();
    for _ in 0..n_workers {
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .context("failed to open worker Redis connection")?;
        let w = w.clone();
        let keys = keys.clone();
        let shutdown = shutdown.clone();
        let brpop_timeout_secs = cfg.brpop_timeout_secs;
        join_set.spawn(async move { w.run(conn, keys, brpop_timeout_secs, shutdown).await });
    }
    // Drop the template's own latency_tx/done_tx clones now that every worker has its
    // own — otherwise `w` sits alive for the rest of this function (it's referenced
    // below only via its already-spawned clones), holding one extra latency_tx sender
    // that the collector's channel-close wait would otherwise block on forever.
    drop(w);

    // Per-second samples collected by the monitor task
    let throughput_samples: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let errors_samples: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let latency_sec_samples: Arc<Mutex<HashMap<String, Vec<u64>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let tput_for_monitor = throughput_samples.clone();
    let err_for_monitor = errors_samples.clone();
    let lat_for_monitor = latency_sec_samples.clone();
    let metrics_mon = metrics.clone();
    let specs_for_monitor = cfg.percentile_specs.to_vec();
    let quiet = cfg.quiet;

    let monitor = tokio::spawn(async move {
        let mut prev_completed = 0u64;
        let mut prev_errors = 0u64;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;

            let cur = metrics_mon.get_completed();
            let tput_delta = cur - prev_completed;
            prev_completed = cur;
            if let Ok(mut v) = tput_for_monitor.lock() {
                v.push(tput_delta);
            }

            let cur_err = metrics_mon.get_errors();
            let err_delta = cur_err - prev_errors;
            prev_errors = cur_err;
            if let Ok(mut v) = err_for_monitor.lock() {
                v.push(err_delta);
            }

            let (resp_tx, resp_rx) = oneshot::channel();
            if snapshot_tx.send(resp_tx).is_ok() {
                if let Ok(snap) = resp_rx.await {
                    if let Ok(mut map) = lat_for_monitor.lock() {
                        for spec in &specs_for_monitor {
                            map.entry(spec.name().to_string())
                                .or_default()
                                .push(spec.value(&snap));
                        }
                    }
                }
            }

            if !quiet {
                if err_delta > 0 {
                    print!("[e:{err_delta}]");
                } else {
                    print!(".");
                }
                use std::io::Write;
                let _ = std::io::stdout().flush();
            }
        }
    });

    let mut timed_out = false;

    // Wait for all jobs to complete, timeout, or every worker exiting early
    // (e.g. Redis became unreachable — no point waiting out the full timeout).
    //
    // `biased;` pins the poll order to the branches' textual order instead of tokio's
    // default pseudo-random tie-break. Without it, on the rare tick where the last
    // job's completion (done_rx flips true) and the join_set fully draining race to
    // resolve in the same poll — e.g. every in-flight BRPOP happens to return in the
    // same instant as the final one — tokio could pick the "all workers exited"
    // branch, mislabeling a fully successful trial as `timed_out` (and tripping the
    // process's overall exit(1)) purely on scheduling luck. Checking done_rx first
    // makes "target reached" always win a genuine tie, which is the only outcome an
    // operator reading the report would consider correct.
    tokio::select! {
        biased;
        _ = done_rx.wait_for(|v| *v) => {},
        _ = tokio::time::sleep(Duration::from_secs(cfg.timeout_secs)) => {
            if !cfg.quiet { eprintln!(); }
            eprintln!("  [timeout after {}s]", cfg.timeout_secs);
            timed_out = true;
        }
        _ = async {
            while let Some(res) = join_set.join_next().await {
                if let Ok(Err(e)) = res {
                    eprintln!("  [worker exited with error: {e}]");
                }
            }
        } => {
            if !cfg.quiet { eprintln!(); }
            eprintln!("  [all workers exited before target reached — check Redis connection]");
            timed_out = true;
        }
    }

    let duration = start.elapsed();
    if !cfg.quiet && !timed_out {
        println!();
    }

    monitor.abort();

    // Signal any still-running workers to stop, then give them up to 5s — bounded by
    // brpop_timeout_secs granularity, same as kombu's own poll responsiveness — to
    // notice and return. Force-abort stragglers so the latency channel (which workers
    // hold senders on) is guaranteed to close and the collector can finish.
    shutdown.store(true, Ordering::Relaxed);
    drop(latency_tx); // drop sentinel before waiting so channel can close once workers exit

    let drain = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(_res) = join_set.join_next().await {}
    })
    .await;
    if drain.is_err() {
        join_set.abort_all();
        // Drain aborted tasks so their latency_tx clones are dropped promptly.
        while join_set.join_next().await.is_some() {}
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Channel is now closed — collector drains buffered values and returns the histogram
    let hist = collector
        .await
        .unwrap_or_else(|_| empty_histogram(hist_max_us));

    let total_jobs = metrics.get_completed();
    let errors = metrics.get_errors();
    let throughput_per_sec = throughput_samples
        .lock()
        .map(|v| v.clone())
        .unwrap_or_default();
    let errors_per_sec = errors_samples.lock().map(|v| v.clone()).unwrap_or_default();
    let latency_per_sec = latency_sec_samples
        .lock()
        .map(|v| v.clone())
        .unwrap_or_default();

    let jobs_per_sec = if duration.as_secs_f64() > 0.0 {
        total_jobs as f64 / duration.as_secs_f64()
    } else {
        0.0
    };

    Ok(TrialResult {
        workers: n_workers,
        total_jobs,
        duration_s: duration.as_secs_f64(),
        jobs_per_sec,
        throughput_per_sec,
        errors_per_sec,
        latency_per_sec,
        latency: LatencyStats::from_histogram(&hist),
        errors,
        timed_out,
    })
}

// ── Entry point ───────────────────────────────────────────────────────────────

/// Sanity ceiling for --num-queues: each queue expands to 4 BRPOP keys and one entry
/// in a Vec<String> built up front. Guards against an accidental huge value (e.g. an
/// extra digit) trying to allocate an unbounded vector / building an unbounded BRPOP
/// key list rather than failing with a clear error.
const MAX_NUM_QUEUES: usize = 10_000;

/// Sanity ceiling for a single --workers value: each unit opens one dedicated Redis
/// connection (sequentially, at trial start) and spawns one tokio task. Guards
/// against an accidental huge value exhausting file descriptors / memory instead of
/// failing with a clear error.
const MAX_WORKERS_PER_TRIAL: usize = 200_000;

/// Validate CLI arguments that clap's type system can't express, failing fast with a
/// clear message instead of letting a bad value crash or silently misbehave deep
/// inside a trial (e.g. `--workers 0` would otherwise spawn zero workers and burn the
/// full --timeout before reporting anything).
fn validate_cli(cli: &Cli) -> Result<()> {
    anyhow::ensure!(cli.jobs > 0, "--jobs must be > 0");
    anyhow::ensure!(cli.num_queues > 0, "--num-queues must be > 0");
    anyhow::ensure!(
        cli.num_queues <= MAX_NUM_QUEUES,
        "--num-queues {} exceeds the sanity limit of {MAX_NUM_QUEUES} (each queue expands to \
         4 BRPOP keys)",
        cli.num_queues
    );
    anyhow::ensure!(!cli.priorities.is_empty(), "--priorities must not be empty");
    for &p in &cli.priorities {
        anyhow::ensure!(p <= 9, "--priorities values must be 0-9 (got {p}) — kombu's real max_priority is 9 (kombu/transport/virtual/base.py:473)");
    }
    anyhow::ensure!(!cli.workers.is_empty(), "--workers must not be empty");
    for &w in &cli.workers {
        anyhow::ensure!(
            w > 0,
            "--workers values must be > 0 (got 0 — a trial with zero workers can never \
             reach its job target and will burn the full --timeout before reporting a failure)"
        );
        anyhow::ensure!(
            w <= MAX_WORKERS_PER_TRIAL,
            "--workers value {w} exceeds the sanity limit of {MAX_WORKERS_PER_TRIAL} \
             (one Redis connection is opened per worker)"
        );
    }
    anyhow::ensure!(
        cli.brpop_timeout_secs.is_finite() && cli.brpop_timeout_secs > 0.0,
        "--brpop-timeout-secs must be a positive, finite number (got {}) — Redis rejects a \
         non-positive BRPOP timeout, which would make every worker error-loop until --timeout",
        cli.brpop_timeout_secs
    );
    anyhow::ensure!(cli.timeout > 0, "--timeout must be > 0");
    // Use the same TLS-resolution logic `build_redis_url` uses to decide whether the
    // `#insecure` fragment applies — checking `cli.tls` alone (regardless of the URL's
    // actual scheme) would wrongly accept e.g. `--tls --insecure --url unix:///tmp/redis.sock`,
    // since --tls never upgrades a non-redis:// scheme (see `resolves_to_tls_scheme`).
    anyhow::ensure!(
        !cli.insecure || resolves_to_tls_scheme(cli),
        "--insecure only makes sense with --tls (or a rediss:// --url) — it has nothing to skip \
         verification for otherwise"
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // See src/tls.rs doc comment: redis-rs's rustls integration installs no crypto
    // backend of its own, so rustls 0.23 panics on ANY TLS connection (not just
    // --insecure ones) unless the process installs one first.
    celery_bench::tls::install_crypto_provider();

    let cli = Cli::parse();
    validate_cli(&cli)?;

    let url = build_redis_url(&cli)?;
    let display_url = redact_url(&url);

    // Warn whenever the FINAL built URL will actually skip certificate verification —
    // keyed off the URL's own `#insecure` fragment, not `cli.insecure` — so a
    // user-supplied `--url`/`REDIS_URL` that already carries `#insecure` (per
    // redis-rs's own documented `rediss://host:port/#insecure` escape hatch, see
    // README "TLS") gets the same loud warning as passing `--insecure` explicitly,
    // instead of silently disabling verification with no indication in the output.
    if url_disables_cert_verification(&url) {
        eprintln!(
            "warning: TLS certificate verification is DISABLED for this connection \
             (insecure mode) — the server's certificate chain and hostname are not \
             validated. Only use this against trusted networks."
        );
    }

    // Warn loudly if FLUSHDB is enabled on db 0 — application data lives there by default.
    if cli.allow_flushdb {
        let db_in_url = url::Url::parse(&url)
            .ok()
            .and_then(|u| u.path().trim_matches('/').parse::<u8>().ok())
            .unwrap_or(0);
        if db_in_url == 0 {
            eprintln!(
                "warning: --allow-flushdb is set on db 0 — this will destroy ALL keys in the \
                 database. Use --db 13 (or any non-zero db) to isolate benchmark data."
            );
        }
    }

    if let Some(out) = &cli.output {
        validate_output_path(out)?;
    }

    // Parse --latency-percentiles before touching the network: a typo here (e.g.
    // "p100") should fail immediately with a clear message, not only after we've
    // already spent time connecting to Redis and fetching its tag.
    let percentile_specs: Vec<PercentileSpec> = cli
        .latency_percentiles
        .iter()
        .map(|s| parse_percentile_spec(s))
        .collect::<Result<Vec<_>>>()?;

    let tag = match &cli.tag {
        Some(t) => sanitize_tag(t),
        None => sanitize_tag(&fetch_tag(&url).await),
    };

    let output = cli
        .output
        .clone()
        .unwrap_or_else(|| format!("celery_bench_{tag}.json"));

    let queue_names = make_queue_names(&cli.queue, cli.num_queues);
    let queues_label = if queue_names.len() == 1 {
        queue_names[0].clone()
    } else {
        format!(
            "{} queues ({}…{})",
            queue_names.len(),
            queue_names[0],
            queue_names[queue_names.len() - 1]
        )
    };

    println!("\n=== celery-bench — {tag} ===");
    println!(
        "    {}  jobs={}  queues={}  priorities={:?}",
        display_url,
        report::format_n(cli.jobs),
        queues_label,
        cli.priorities,
    );
    println!();

    let client = redis::Client::open(url.as_str()).context("invalid Redis URL")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("failed to connect to Redis")?;

    let cfg = TrialConfig {
        url: &url,
        queues: &queue_names,
        jobs: cli.jobs,
        brpop_timeout_secs: cli.brpop_timeout_secs,
        timeout_secs: cli.timeout,
        quiet: cli.quiet,
        percentile_specs: &percentile_specs,
    };
    // Warmup uses the same settings but targets warmup_jobs completions
    let warmup_cfg = TrialConfig {
        jobs: cli.warmup_jobs,
        ..cfg
    };

    let workers_list = cli.workers.clone();
    let mut results: Vec<TrialResult> = Vec::new();
    let mut any_timeout = false;

    // Warn if the queue fill will likely use significant Redis memory. Measured from
    // one real serialized envelope rather than a hardcoded guess, since the Celery
    // protocol-v2 envelope (headers + properties + base64 body) is meaningfully
    // larger than a bare job payload.
    let sample_len = serde_json::to_string(&CeleryEnvelope::new(&cli.queue, 0, 0))
        .map(|s| s.len())
        .unwrap_or(300);
    let estimated_mb = cli.jobs as f64 * sample_len as f64 / (1024.0 * 1024.0);
    if estimated_mb > 100.0 {
        eprintln!(
            "warning: estimated peak Redis memory ~{:.0} MB ({} jobs × ~{} B/job)",
            estimated_mb,
            report::format_n(cli.jobs),
            sample_len
        );
    }

    for &n_workers in &workers_list {
        if cli.warmup_jobs > 0 {
            pre_trial_clear(&mut conn, &queue_names, cli.allow_flushdb).await?;
            producer::bulk_enqueue(&mut conn, &queue_names, &cli.priorities, cli.warmup_jobs)
                .await?;
            if !cli.quiet {
                print!("  [{n_workers:>4} workers] warmup … ");
            }
            run_trial(&warmup_cfg, n_workers).await?;
        }

        pre_trial_clear(&mut conn, &queue_names, cli.allow_flushdb).await?;
        producer::bulk_enqueue(&mut conn, &queue_names, &cli.priorities, cli.jobs).await?;

        if !cli.quiet {
            print!("  [{n_workers:>4} workers] ");
        }

        let result = run_trial(&cfg, n_workers).await?;

        if result.timed_out {
            any_timeout = true;
        }
        report::print_trial_line(&result);
        results.push(result);
    }

    report::print_summary(&results);

    report::write_json(
        &results,
        &tag,
        &display_url,
        &workers_list,
        cli.jobs,
        &queue_names,
        cli.warmup_jobs,
        &output,
    )?;

    if any_timeout {
        eprintln!("warning: one or more trials timed out — results are incomplete");
        std::process::exit(1);
    }

    Ok(())
}

/// Clear queues before a trial. Uses DEL by default; FLUSHDB only when explicitly allowed.
async fn pre_trial_clear(
    conn: &mut redis::aio::MultiplexedConnection,
    queues: &[String],
    allow_flushdb: bool,
) -> Result<()> {
    if allow_flushdb {
        producer::flushdb(conn).await
    } else {
        producer::clear_queue(conn, queues).await
    }
}

// ── TrialConfig Copy impl ─────────────────────────────────────────────────────

impl<'a> Copy for TrialConfig<'a> {}
impl<'a> Clone for TrialConfig<'a> {
    fn clone(&self) -> Self {
        *self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_tag_strips_unsafe_chars() {
        assert_eq!(sanitize_tag("redis-8.0"), "redis-8.0"); // dots and dashes kept
        assert_eq!(sanitize_tag("redis/8.0"), "redis-8.0"); // slash → dash
        assert_eq!(sanitize_tag("../evil"), "..-evil");
        assert_eq!(sanitize_tag("foo bar"), "foo-bar"); // space → dash
        assert_eq!(sanitize_tag(""), "unknown");
    }

    #[test]
    fn validate_output_path_rejects_traversal() {
        assert!(validate_output_path("../evil.json").is_err());
        assert!(validate_output_path("foo/../bar.json").is_err());
        assert!(validate_output_path("results/out.json").is_ok());
        assert!(validate_output_path("-").is_ok());
        assert!(validate_output_path("out.json").is_ok());
    }

    #[test]
    fn redact_url_hides_password() {
        let raw = "redis://:hunter2@127.0.0.1:6379/0";
        let redacted = redact_url(raw);
        assert!(
            !redacted.contains("hunter2"),
            "password still visible: {redacted}"
        );
        assert!(redacted.contains("****"), "no redaction marker: {redacted}");
    }

    #[test]
    fn redact_url_leaves_no_password_url_unchanged() {
        let raw = "redis://127.0.0.1:6379/0";
        assert_eq!(redact_url(raw), raw);
    }

    fn base_cli() -> Cli {
        Cli {
            url: "redis://127.0.0.1:6379/0".into(),
            host: None,
            port: None,
            password: None,
            tls: false,
            insecure: false,
            db: Some(0),
            workers: vec![10],
            jobs: 1000,
            warmup_jobs: 0,
            queue: "celery".into(),
            num_queues: 1,
            priorities: vec![0],
            brpop_timeout_secs: 1.0,
            latency_percentiles: vec![],
            tag: None,
            output: None,
            timeout: 300,
            quiet: false,
            allow_flushdb: false,
        }
    }

    #[test]
    fn build_redis_url_encodes_special_chars_in_password() {
        // Password containing '@' must be percent-encoded so the URL is parsed correctly.
        let mut cli = base_cli();
        cli.password = Some("p@ss/word".into());
        let url = build_redis_url(&cli).unwrap();
        let parsed = url::Url::parse(&url).unwrap();
        assert_eq!(parsed.host_str().unwrap(), "127.0.0.1");
        let raw_pw = parsed.password().unwrap();
        assert!(raw_pw.contains("%40"), "@ not percent-encoded: {raw_pw}");
        assert!(!url.contains(":p@ss"), "raw '@' leaked into URL: {url}");
    }

    #[test]
    fn build_redis_url_upgrades_scheme_with_tls() {
        let mut cli = base_cli();
        cli.tls = true;
        let url = build_redis_url(&cli).unwrap();
        assert!(url.starts_with("rediss://"), "expected rediss:// got {url}");
    }

    #[test]
    fn build_redis_url_appends_insecure_fragment_with_tls_and_insecure() {
        let mut cli = base_cli();
        cli.tls = true;
        cli.insecure = true;
        let url = build_redis_url(&cli).unwrap();
        assert!(url.starts_with("rediss://"), "expected rediss:// got {url}");
        assert!(
            url.ends_with("#insecure"),
            "expected #insecure fragment, got {url}"
        );
    }

    #[test]
    fn build_redis_url_omits_insecure_fragment_when_flag_not_set() {
        let mut cli = base_cli();
        cli.tls = true;
        let url = build_redis_url(&cli).unwrap();
        assert!(!url.contains("insecure"), "unexpected fragment: {url}");
    }

    // ── url_disables_cert_verification — the insecure-mode warning gate ────────
    //
    // Keyed off the FINAL URL's own `#insecure` fragment, not `cli.insecure`, so a
    // user-supplied --url/REDIS_URL that already carries `#insecure` (redis-rs's own
    // documented escape hatch) gets warned about too — not just the --insecure flag
    // path.

    #[test]
    fn url_disables_cert_verification_false_by_default() {
        // Default case: no fragment at all — must not trigger the warning.
        let cli = base_cli();
        let url = build_redis_url(&cli).unwrap();
        assert!(!url_disables_cert_verification(&url), "url={url}");
    }

    #[test]
    fn url_disables_cert_verification_true_via_insecure_flag() {
        let mut cli = base_cli();
        cli.tls = true;
        cli.insecure = true;
        let url = build_redis_url(&cli).unwrap();
        assert!(url_disables_cert_verification(&url), "url={url}");
    }

    #[test]
    fn url_disables_cert_verification_true_via_url_supplied_fragment_without_flag() {
        // The gap this fixes: --insecure was never passed, but --url itself already
        // carries #insecure. Must still warn.
        let mut cli = base_cli();
        cli.url = "rediss://127.0.0.1:6379/0#insecure".into();
        assert!(!cli.insecure, "test fixture must not set --insecure");
        let url = build_redis_url(&cli).unwrap();
        assert!(url_disables_cert_verification(&url), "url={url}");
    }

    #[test]
    fn url_disables_cert_verification_false_for_plain_tls_without_insecure() {
        let mut cli = base_cli();
        cli.tls = true;
        let url = build_redis_url(&cli).unwrap();
        assert!(!url_disables_cert_verification(&url), "url={url}");
    }

    #[test]
    fn url_disables_cert_verification_false_for_unparsable_url() {
        assert!(!url_disables_cert_verification("not a url"));
    }

    #[test]
    fn url_disables_cert_verification_false_for_plain_scheme_with_insecure_fragment() {
        // Regression guard: redis-rs only ever consults the #insecure fragment on a
        // rediss:// URL — on a plain redis:// URL the fragment is inert (the
        // connection is cleartext TCP, there is no TLS certificate to fail to
        // verify). The old check looked at the fragment alone and would have
        // wrongly printed the "TLS certificate verification is DISABLED" warning
        // for a connection that was never using TLS in the first place.
        let url = "redis://host:6379/0#insecure";
        assert!(!url_disables_cert_verification(url), "url={url}");
    }

    // ── redis-rs's own parser, not just our URL string ──────────────────────────
    //
    // Every other test in this file (build_redis_url_appends_insecure_fragment_*,
    // etc.) asserts on the STRING `build_redis_url` produces via `url::Url` — none of
    // them touch redis-rs at all. That whole suite would still pass identically with
    // `tls-rustls-insecure` removed from Cargo.toml again (the exact regression #7 /
    // this PR fixes), because the fragment string "#insecure" appears in the URL
    // either way — only what redis-rs's OWN connector does with that fragment at
    // connect time depends on the feature (see the Cargo.toml comment next to the
    // `redis` dependency, and connection.rs:1274-1279). These tests close that gap by
    // running our built URL through redis-rs's real `IntoConnectionInfo` parser
    // (the same parsing `redis::Client::open` performs) and asserting on the
    // resulting `ConnectionAddr` struct itself.
    use redis::{ConnectionAddr, IntoConnectionInfo};

    #[test]
    fn build_redis_url_insecure_parses_via_redis_rs_as_tls_insecure() {
        let mut cli = base_cli();
        cli.tls = true;
        cli.insecure = true;
        let url = build_redis_url(&cli).unwrap();
        let info = url
            .clone()
            .into_connection_info()
            .expect("redis-rs must accept our built URL");
        match info.addr().clone() {
            ConnectionAddr::TcpTls {
                host,
                port,
                insecure,
                ..
            } => {
                assert_eq!(host, "127.0.0.1");
                assert_eq!(port, 6379);
                assert!(
                    insecure,
                    "redis-rs's own parser must resolve --insecure to \
                     ConnectionAddr::TcpTls {{ insecure: true, .. }}, url={url}"
                );
            }
            other => panic!("expected ConnectionAddr::TcpTls, got {other:?} for url={url}"),
        }
        // Regression guard for the db-selection bug class this repo has been bitten by
        // before (see explicit_db_overrides_the_db_in_the_url below): the #insecure
        // fragment logic must not disturb which db redis-rs actually selects.
        assert_eq!(
            info.redis_settings().db(),
            0,
            "db must be unaffected by the #insecure fragment, url={url}"
        );
    }

    #[test]
    fn build_redis_url_tls_without_insecure_parses_as_tls_secure() {
        let mut cli = base_cli();
        cli.tls = true;
        let url = build_redis_url(&cli).unwrap();
        let info = url
            .clone()
            .into_connection_info()
            .expect("redis-rs must accept our built URL");
        match info.addr().clone() {
            ConnectionAddr::TcpTls { insecure, .. } => {
                assert!(
                    !insecure,
                    "plain --tls without --insecure must parse as insecure: false, url={url}"
                );
            }
            other => panic!("expected ConnectionAddr::TcpTls, got {other:?} for url={url}"),
        }
        assert_eq!(
            info.redis_settings().db(),
            0,
            "db must be unaffected by the --tls scheme upgrade, url={url}"
        );
    }

    #[test]
    fn build_redis_url_plain_redis_parses_as_tcp_not_tls() {
        let cli = base_cli();
        let url = build_redis_url(&cli).unwrap();
        let info = url
            .clone()
            .into_connection_info()
            .expect("redis-rs must accept our built URL");
        assert!(
            matches!(info.addr(), ConnectionAddr::Tcp(..)),
            "plain redis:// must parse as ConnectionAddr::Tcp, got {:?} for url={url}",
            info.addr()
        );
        assert_eq!(info.redis_settings().db(), 0, "url={url}");
    }

    #[test]
    fn build_redis_url_ignores_insecure_flag_without_tls_scheme() {
        // --insecure with a plain redis:// URL and no --tls has nothing to make insecure —
        // validate_cli() rejects this combination before build_redis_url() is ever reached,
        // but build_redis_url() itself must still not add the fragment to a redis:// URL.
        let mut cli = base_cli();
        cli.insecure = true;
        let url = build_redis_url(&cli).unwrap();
        assert!(url.starts_with("redis://"), "expected redis:// got {url}");
        assert!(!url.contains("insecure"), "unexpected fragment: {url}");
    }

    #[test]
    fn validate_cli_rejects_insecure_without_tls() {
        let mut cli = base_cli();
        cli.insecure = true;
        let err = validate_cli(&cli).unwrap_err().to_string();
        assert!(err.contains("--insecure"), "unexpected error: {err}");
    }

    #[test]
    fn validate_cli_accepts_insecure_with_tls() {
        let mut cli = base_cli();
        cli.tls = true;
        cli.insecure = true;
        assert!(validate_cli(&cli).is_ok());
    }

    #[test]
    fn validate_cli_accepts_insecure_with_rediss_url() {
        let mut cli = base_cli();
        cli.url = "rediss://127.0.0.1:6379/0".into();
        cli.insecure = true;
        assert!(validate_cli(&cli).is_ok());
    }

    #[test]
    fn validate_cli_accepts_insecure_with_uppercase_rediss_scheme() {
        // Regression guard: validate_cli used to compare the RAW cli.url string
        // case-sensitively against "rediss://", while build_redis_url compared the
        // already-lowercased *parsed* scheme. `url::Url` lowercases the scheme per RFC
        // 3986, so REDISS://... parses to scheme() == "rediss" and build_redis_url
        // would have accepted it — but the old validate_cli rejected it first, so the
        // two functions disagreed about the same URL.
        let mut cli = base_cli();
        cli.url = "REDISS://127.0.0.1:6379/0".into();
        cli.insecure = true;
        assert!(validate_cli(&cli).is_ok());
    }

    #[test]
    fn validate_cli_rejects_insecure_with_tls_but_unix_scheme() {
        // Regression guard: `cli.tls` being true does NOT mean the URL resolves to
        // rediss:// — build_redis_url only upgrades a `redis://` scheme, never
        // `unix://`. The old check was `!cli.insecure || cli.tls || url_is_rediss`,
        // which wrongly passed here because `cli.tls` alone short-circuited it,
        // leaving --insecure silently inert (build_redis_url never sets the
        // #insecure fragment on a unix:// URL). validate_cli must reject this.
        let mut cli = base_cli();
        cli.url = "unix:///tmp/redis.sock".into();
        cli.tls = true;
        cli.insecure = true;
        let err = validate_cli(&cli).unwrap_err().to_string();
        assert!(err.contains("--insecure"), "unexpected error: {err}");
    }

    #[test]
    fn redis_tls_insecure_env_var_false_resolves_to_false_not_present_therefore_true() {
        // Regression guard for the "env var present at all == true" bug class this org
        // has hit before with boolean flag/env pairs (see sibling repos' identical
        // fix): `--insecure`/`--tls` are plain `bool` fields with `env = "..."`, which
        // clap backs with a `true`/`false`-only value parser for the env var — NOT
        // clap's default flag behavior of "presence alone means true". Confirm the env
        // var's actual VALUE is honored, not just its presence, for both flags this
        // pattern applies to.
        //
        // Mutates process-wide env vars, so run everything needing a specific value
        // for these two vars inside this single test to avoid races with other tests
        // parsing `Cli` concurrently — no other test reads these two vars.
        for (val, expected) in [("false", false), ("true", true)] {
            std::env::set_var("REDIS_TLS_INSECURE", val);
            std::env::set_var("REDIS_TLS", val);
            let cli = Cli::try_parse_from(["celery-bench"])
                .unwrap_or_else(|e| panic!("REDIS_TLS_INSECURE/REDIS_TLS={val:?} must parse: {e}"));
            assert_eq!(
                cli.insecure, expected,
                "REDIS_TLS_INSECURE={val:?} must resolve insecure to {expected}"
            );
            assert_eq!(
                cli.tls, expected,
                "REDIS_TLS={val:?} must resolve tls to {expected}"
            );
        }
        std::env::remove_var("REDIS_TLS_INSECURE");
        std::env::remove_var("REDIS_TLS");

        // Documents the actual (safe) failure mode for a value other than the literal
        // "true"/"false" clap's bool value parser accepts for an env-backed flag: it is
        // neither silently ignored (falling back to "present therefore true") nor
        // silently coerced to false — clap hard-errors CLI parsing instead, so a typo'd
        // env value like "0"/"1"/"yes"/"no" fails loudly rather than misconfiguring TLS
        // verification silently.
        std::env::set_var("REDIS_TLS_INSECURE", "0");
        let err = Cli::try_parse_from(["celery-bench"])
            .expect_err("REDIS_TLS_INSECURE=0 is not a valid clap bool and must error, not silently resolve to true or false");
        assert!(
            err.to_string().contains("--insecure"),
            "expected the parse error to name --insecure, got: {err}"
        );
        std::env::remove_var("REDIS_TLS_INSECURE");
    }

    #[test]
    fn parsed_scheme_lowercases() {
        assert_eq!(
            parsed_scheme("REDISS://host:6380/0").as_deref(),
            Some("rediss")
        );
        assert_eq!(
            parsed_scheme("redis://host:6379/0").as_deref(),
            Some("redis")
        );
        assert_eq!(parsed_scheme("not a url"), None);
    }

    #[test]
    fn build_redis_url_host_port_override() {
        let mut cli = base_cli();
        cli.host = Some("10.0.0.1".into());
        cli.port = Some(6380);
        let url = build_redis_url(&cli).unwrap();
        let parsed = url::Url::parse(&url).unwrap();
        assert_eq!(parsed.host_str().unwrap(), "10.0.0.1");
        assert_eq!(parsed.port().unwrap(), 6380);
    }

    #[test]
    fn parse_percentile_spec_valid() {
        let cases: &[(&str, f64)] = &[
            ("p50", 0.50),
            ("p90", 0.90),
            ("p99", 0.99),
            ("p999", 0.999),
            ("p9999", 0.9999),
            ("p75", 0.75),
        ];
        for &(s, expected_q) in cases {
            match parse_percentile_spec(s).unwrap() {
                PercentileSpec::Quantile { q, name } => {
                    assert!((q - expected_q).abs() < 1e-9, "{s}: got {q}");
                    assert_eq!(name, s);
                }
                other => panic!("{s} parsed as non-quantile: {}", other.name()),
            }
        }
        assert!(matches!(
            parse_percentile_spec("max").unwrap(),
            PercentileSpec::Max
        ));
        assert!(matches!(
            parse_percentile_spec("mean").unwrap(),
            PercentileSpec::Mean
        ));
    }

    #[test]
    fn parse_percentile_spec_invalid() {
        assert!(parse_percentile_spec("p0").is_err()); // 0/10 = 0.0 out of range
        assert!(parse_percentile_spec("p").is_err());
        assert!(parse_percentile_spec("pxyz").is_err());
        assert!(parse_percentile_spec("99").is_err());
        assert!(parse_percentile_spec("").is_err());
    }

    #[test]
    fn make_queue_names_single_and_multi() {
        assert_eq!(make_queue_names("celery", 1), vec!["celery"]);
        assert_eq!(make_queue_names("q", 3), vec!["q_0", "q_1", "q_2"]);
    }

    #[test]
    fn parse_percentile_spec_rejects_ambiguous_p100() {
        // "p100" would otherwise silently parse as 100/1000 = 0.10 (the 10th
        // percentile) under the pXYZ digit-count-as-decimal-placement scheme — a
        // textbook "looks right, means something else" trap. Must error, not just
        // land in-range.
        let err = parse_percentile_spec("p100").unwrap_err();
        assert!(
            err.to_string().contains("ambiguous"),
            "expected an 'ambiguous' error, got: {err}"
        );
    }

    #[test]
    fn parse_percentile_spec_p9999_still_valid_after_p100_guard() {
        // Regression guard: the p100 special-case must not accidentally reject other
        // 3+-digit specs.
        assert!(parse_percentile_spec("p999").is_ok());
        assert!(parse_percentile_spec("p9999").is_ok());
        assert!(parse_percentile_spec("p101").is_ok()); // not "100", still parses (0.101)
    }

    // ── validate_cli ─────────────────────────────────────────────────────────

    #[test]
    fn validate_cli_accepts_defaults() {
        assert!(validate_cli(&base_cli()).is_ok());
    }

    #[test]
    fn validate_cli_rejects_zero_jobs() {
        let mut cli = base_cli();
        cli.jobs = 0;
        assert!(validate_cli(&cli).is_err());
    }

    #[test]
    fn validate_cli_rejects_zero_num_queues() {
        let mut cli = base_cli();
        cli.num_queues = 0;
        assert!(validate_cli(&cli).is_err());
    }

    #[test]
    fn validate_cli_rejects_huge_num_queues() {
        let mut cli = base_cli();
        cli.num_queues = MAX_NUM_QUEUES + 1;
        assert!(validate_cli(&cli).is_err());
        // ...but the limit itself is still accepted (boundary check).
        cli.num_queues = MAX_NUM_QUEUES;
        assert!(validate_cli(&cli).is_ok());
    }

    #[test]
    fn validate_cli_rejects_empty_workers() {
        let mut cli = base_cli();
        cli.workers = vec![];
        assert!(validate_cli(&cli).is_err());
    }

    #[test]
    fn validate_cli_rejects_zero_worker_level() {
        let mut cli = base_cli();
        cli.workers = vec![10, 0, 50];
        assert!(validate_cli(&cli).is_err());
    }

    #[test]
    fn validate_cli_rejects_huge_worker_level() {
        let mut cli = base_cli();
        cli.workers = vec![MAX_WORKERS_PER_TRIAL + 1];
        assert!(validate_cli(&cli).is_err());
        cli.workers = vec![MAX_WORKERS_PER_TRIAL];
        assert!(validate_cli(&cli).is_ok());
    }

    #[test]
    fn validate_cli_rejects_empty_priorities() {
        let mut cli = base_cli();
        cli.priorities = vec![];
        assert!(validate_cli(&cli).is_err());
    }

    #[test]
    fn validate_cli_rejects_priority_above_nine() {
        let mut cli = base_cli();
        cli.priorities = vec![0, 10];
        assert!(validate_cli(&cli).is_err());
    }

    #[test]
    fn validate_cli_accepts_duplicate_priorities() {
        // Redundant, not invalid — a user round-robining onto the same priority
        // multiple times just gets a lopsided (but well-defined) distribution.
        let mut cli = base_cli();
        cli.priorities = vec![0, 0, 0];
        assert!(validate_cli(&cli).is_ok());
    }

    #[test]
    fn validate_cli_rejects_zero_and_negative_and_nan_brpop_timeout() {
        let mut cli = base_cli();
        cli.brpop_timeout_secs = 0.0;
        assert!(validate_cli(&cli).is_err());
        cli.brpop_timeout_secs = -1.0;
        assert!(validate_cli(&cli).is_err());
        cli.brpop_timeout_secs = f64::NAN;
        assert!(validate_cli(&cli).is_err());
        cli.brpop_timeout_secs = f64::INFINITY;
        assert!(validate_cli(&cli).is_err());
    }

    #[test]
    fn validate_cli_rejects_zero_timeout() {
        let mut cli = base_cli();
        cli.timeout = 0;
        assert!(validate_cli(&cli).is_err());
    }

    // ── redact_url_best_effort ───────────────────────────────────────────────

    #[test]
    fn redact_url_best_effort_redacts_well_formed_url() {
        let raw = "redis://:hunter2@127.0.0.1:6379/0";
        let redacted = redact_url_best_effort(raw);
        assert!(!redacted.contains("hunter2"));
        assert!(redacted.contains("****"));
    }

    #[test]
    fn redact_url_best_effort_redacts_even_when_url_parse_fails() {
        // Deliberately malformed (space in host) so `url::Url::parse` errors out —
        // the manual fallback scan must still find and mask the password rather than
        // falling back to echoing the raw string.
        let raw = "redis://user:hunter2@bad host:6379/0";
        assert!(
            url::Url::parse(raw).is_err(),
            "test fixture must actually fail url::Url::parse"
        );
        let redacted = redact_url_best_effort(raw);
        assert!(
            !redacted.contains("hunter2"),
            "password leaked through fallback path: {redacted}"
        );
        assert!(redacted.contains("****"), "no redaction marker: {redacted}");
    }

    #[test]
    fn redact_url_best_effort_leaves_credential_free_malformed_url_unchanged() {
        let raw = "redis://bad host:6379/0";
        assert!(url::Url::parse(raw).is_err());
        // No userinfo to redact — falls through to returning the input as-is, which
        // is safe since there was never a secret in it.
        assert_eq!(redact_url_best_effort(raw), raw);
    }

    // ── empty_histogram bound sizing ─────────────────────────────────────────

    #[test]
    fn empty_histogram_bound_respects_floor_and_ceiling() {
        // A tiny --timeout still gets at least the historical 60s resolution.
        let mut h = empty_histogram(1);
        assert!(h.record(59_999_999).is_ok());
        // An enormous --timeout is capped, not left to balloon unbounded.
        let mut h = empty_histogram(u64::MAX);
        assert!(h.record(MAX_HIST_MAX_US - 1).is_ok());
    }

    #[test]
    fn empty_histogram_bound_covers_configured_timeout() {
        // Regression for the original bug: default --timeout is 300s but the
        // histogram used to be hardcoded to a 60s bound, silently dropping (not
        // clamping — `Histogram::record` errors and the caller ignores it) any
        // latency between 60s and the actual timeout.
        let timeout_secs = 300u64;
        let mut h = empty_histogram(timeout_secs * 1_000_000);
        assert!(
            h.record(290 * 1_000_000).is_ok(),
            "a latency just under the configured --timeout must be recordable"
        );
    }

    #[test]
    fn explicit_db_overrides_the_db_in_the_url() {
        // Regression: --db used to apply only when --url carried no path. The default --url is
        // redis://127.0.0.1:6379/13, which always has one, so --db was silently ignored -- and
        // since db 13 does not exist on Redis Cluster or most managed Redis, `--db 0` was both
        // the documented workaround and a no-op. Invisible without attaching MONITOR, hence this.
        let cli = Cli {
            url: "redis://127.0.0.1:6379/13".into(),
            host: None,
            port: None,
            db: Some(0),
            ..base_cli()
        };
        assert_eq!(build_redis_url(&cli).unwrap(), "redis://127.0.0.1:6379/0");
    }

    #[test]
    fn omitted_db_keeps_the_url_db() {
        let cli = Cli {
            url: "redis://127.0.0.1:6379/7".into(),
            host: None,
            port: None,
            db: None,
            ..base_cli()
        };
        assert_eq!(build_redis_url(&cli).unwrap(), "redis://127.0.0.1:6379/7");
    }

    #[test]
    fn omitted_db_and_pathless_url_falls_back_to_13() {
        let cli = Cli {
            url: "redis://127.0.0.1:6379".into(),
            host: None,
            port: None,
            db: None,
            ..base_cli()
        };
        assert_eq!(build_redis_url(&cli).unwrap(), "redis://127.0.0.1:6379/13");
    }
}

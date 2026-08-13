# Agent guidelines

Instructions for AI coding agents (Claude Code, Copilot, Cursor, etc.) working in this repo.

## Project overview

`celery-benchmark` is a Celery/kombu protocol load benchmark written in Rust. It measures task throughput (jobs/second) and full latency spectrum (p50 → p99.99) against any Redis broker endpoint. There is no official Rust client for Celery/kombu, so this tool talks directly to Redis via the `redis` crate and reimplements kombu's Redis transport wire protocol itself — see README.md "Protocol compatibility" for exact source citations (kombu's priority-expanded queue keys, BRPOP shape, task message envelope). The tool supports multiple concurrency levels in a single run, multi-queue and multi-priority round-robin distribution, per-second time-series output, and emits results as both a formatted console table and a JSON file (schema-compatible with the sister tool `sidekiq-benchmark`). It is published as a Docker image (`redis/celery-benchmark`) and as a single static binary.

## Local setup

Requires Rust stable (1.75+).

```bash
git clone git@github.com:redis-performance/celery-benchmark.git
cd celery-benchmark
cargo build --release
```

Verify the build:

```bash
# Requires a running Redis on 127.0.0.1:6379
./target/release/celery-bench \
  --url redis://127.0.0.1:6379/0 \
  --workers 2 \
  --jobs 500 \
  --output -
```

## Branch naming

Same as human contributors: `<type>/<short-description>` (e.g. `fix/off-by-one-in-pipeline`).

## Coding standards

- Match the style already in the file you are editing.
- Prefer clear, minimal changes over large refactors unless explicitly asked.
- Do not add comments that describe *what* the code does — only add comments when the *why* is non-obvious. The exception: `job.rs`'s protocol-derivation functions carry deliberately heavy citation comments (exact kombu/Celery file + line numbers) — preserve that density when touching them, and re-verify against current upstream source before changing any cited claim.
- Do not introduce new dependencies without checking with the maintainer.

## Running tests

Run the full suite before declaring a task complete:

```bash
cargo fmt --check
cargo clippy -- -D warnings
cargo test
```

For a full end-to-end smoke test (requires Redis on `127.0.0.1:6379`):

```bash
cargo run --release -- \
  --url redis://127.0.0.1:6379/0 \
  --workers 2 \
  --jobs 500 \
  --timeout 60 \
  --output /tmp/smoke.json \
  --quiet \
  --tag smoke
```

Always run tests before declaring a task complete.

## How to submit changes

1. Create a branch: `git checkout -b <type>/<description>`.
2. Commit with a clear message focused on *why*, not *what*.
3. Open a pull request against `main`.
4. Do **not** push directly to `main`.

## What to avoid

- Do not reformat files unrelated to your change.
- Do not remove error handling or tests.
- Do not commit secrets, credentials, or large binary files.
- Do not amend published commits.
- Do not run the benchmark against a production Redis instance — it pre-fills hundreds of thousands of task messages and can optionally flush the entire database.
- Do not restate a kombu/Celery protocol claim from memory — re-read the cited source line before changing `job.rs`'s key-derivation or envelope-construction logic. A wrong protocol claim here is worse than no claim.

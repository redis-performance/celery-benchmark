# Contributing

We treat this repo as "Open Source" within Redis: anyone who clears the bar below is welcome to contribute.

## Local setup

Requires Rust stable (1.75+) and a running Redis instance (any version 6+).

```bash
git clone git@github.com:redis-performance/celery-benchmark.git
cd celery-benchmark
cargo build --release
```

To verify the build works end-to-end, spin up Redis and run a quick smoke test:

```bash
# Start Redis (or point REDIS_URL at an existing instance)
docker run --rm -d -p 6379:6379 redis:8

# Quick smoke test — 500 jobs, 2 workers, db 0
./target/release/celery-bench \
  --url redis://127.0.0.1:6379/0 \
  --workers 2 \
  --jobs 500 \
  --output -
```

## Branch naming

```
<type>/<short-description>
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`

Example: `feat/add-pipeline-mode`

## Coding standards

- Keep changes focused; one logical change per PR.
- Follow the conventions already present in the codebase (formatting, naming, error handling).
- No dead code, no commented-out blocks.
- Any change to `src/job.rs`'s priority-key derivation or envelope construction must
  cite the exact kombu/Celery source file and line it's re-verified against — don't
  trust an existing comment's line numbers without checking current upstream.

## Submitting changes

1. Fork or create a branch from `main`.
2. Make your changes with clear, atomic commits.
3. Open a pull request against `main` with a descriptive title and summary.
4. Address review comments promptly; force-push to the same branch to update.

## Testing

All new behaviour must be covered by tests. Existing tests must pass before opening a PR. Run the full suite locally:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

`cargo test` runs both the unit tests in `src/*.rs` and the real-Redis integration
tests in `tests/protocol_integration.rs` (the latter exercise the actual
producer/worker code against a live Redis, not a reimplementation of it — see that
file's module doc). They default to `redis://127.0.0.1:6379/15`; override with
`CELERY_BENCH_TEST_REDIS_URL` if you're running Redis elsewhere. Any change to the
protocol layer (`src/job.rs`, `src/producer.rs`, `src/worker.rs`) should extend these
integration tests, not just the unit tests — unit tests can't catch a bug that only
shows up once real Redis is involved (see the round-robin queue/priority correlation
bug those tests were written to catch).

For a full end-to-end smoke test (requires a running Redis on `127.0.0.1:6379`):

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

Coverage should not decrease.

## Review process

- At least one maintainer approval is required before merge.
- CI must be green (format check, clippy, unit tests, smoke test all pass).
- Maintainers may request changes or close PRs that don't meet the bar — this is normal and not personal.

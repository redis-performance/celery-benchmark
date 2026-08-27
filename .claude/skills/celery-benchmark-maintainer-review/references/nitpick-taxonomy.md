# Cross-cutting nitpick taxonomy — celery-benchmark, real precedent only

This repo's entire recorded history, as surveyed on 2026-08-27 (repo created 2026-08-13),
is 2 merged PRs, 3 issues, and its own AGENTS.md/CONTRIBUTING.md. Every category below is
evidenced by exactly one real shipped-and-fixed bug, one real open issue, or one explicit
written rule — treat a single citation honestly as a single citation, not a repeated
pattern. There is no equivalent here to a project with hundreds of surveyed PRs; do not
imply otherwise.

1. **A CLI flag that looks wired up but never reaches the connection is this codebase's
   single sharpest real bug.** PR#4 (fixing issue #3): `--db` was accepted and parsed, but
   the "apply `--db`" branch was gated on `u.path().trim_matches('/').is_empty()`, and the
   *default* `--url` already carries `/13` — so that branch could never fire, and
   `--db 0` / `--db 5` produced identical wire behavior (`SELECT 13` either way,
   confirmed with `MONITOR`). The bug was invisible without attaching `MONITOR` — no
   error, no warning, just a silently different database. Any PR that adds or touches a
   CLI flag meant to affect an outgoing Redis command should be checked for exactly this
   shape: does the flag's effect actually reach the wire under the *default* invocation,
   not just some invocation someone happened to test? "Compiles, is accepted, doesn't
   error" is not evidence a flag works — this exact bug cleared all three.

2. **Release-artifact portability is a real, once-shipped, once-fixed problem: verifying a
   binary only on its own build host proves nothing about where it will run.** PR#2
   (fixing issue #1): the `-gnu` release artifacts required `GLIBC_2.39` (Ubuntu 24.04's
   own glibc), so they failed to even load on Ubuntu 22.04, Debian 12, or RHEL 9 — and
   failed *silently*: `--version` produced a loader error indistinguishable, to an
   automated harness, from "wrong version" or "not installed," so downstream benchmark
   suites just skipped rather than erroring, with the overall CI run staying green. Fixed
   by moving to static musl targets plus a build-time assertion that the artifact carries
   zero `GLIBC_*` symbols. If a PR touches `release.yml`, target triples, or linking
   flags, check whether it reintroduces a dynamic glibc dependency, and whether "verify
   the build" in CI still only runs on the build host itself (which by construction can't
   catch this class of bug).

3. **Protocol-fidelity comments in `src/job.rs` are load-bearing and must be re-verified
   against real upstream source, not trusted from memory.** This is written doctrine in
   both AGENTS.md ("re-verify against current upstream source before changing any cited
   claim... A wrong protocol claim here is worse than no claim") and CONTRIBUTING.md ("must
   cite the exact kombu/Celery source file and line it's re-verified against — don't trust
   an existing comment's line numbers without checking current upstream"). Not yet
   evidenced by a caught reviewer mistake, since no review has happened on this repo — but
   it is real, explicit, twice-stated written doctrine, applied nowhere else in the
   codebase's public rules as strongly. If a PR touches `job.rs`'s priority-key derivation
   or envelope construction, check that any cited upstream file/line is still accurate,
   and flag a change to that logic with no updated citation.

4. **Integration tests against real Redis exist specifically because a unit test couldn't
   have caught a real bug.** CONTRIBUTING.md states `tests/protocol_integration.rs` was
   written to catch "the round-robin queue/priority correlation bug" — cited as the reason
   these integration tests exist for the protocol layer. This survey found no PR/issue
   number attached to that specific bug (it isn't among the 2 merged PRs or 3 issues
   surveyed), so treat it as real written motivation, not a fabricated citation, but be
   honest this skill can't point to that bug's own PR the way it can for items 1 and 2.
   Any change to `src/job.rs`, `src/producer.rs`, or `src/worker.rs` should extend
   `tests/protocol_integration.rs`, per CONTRIBUTING.md's explicit instruction — a
   unit-test-only diff to these three files is worth naming as incomplete per the
   project's own written rule, not just generic "add more tests" advice.

5. **Cluster-mode Redis is a real, currently open, self-scoped gap — not a hypothetical.**
   Issue #5 (open, unresolved as of this survey): the tool uses a plain `redis::Client`
   with no `cluster`/`cluster-async` feature, so it cannot follow `MOVED` redirection, and
   the default queue-key naming spans multiple hash slots, so hash-tagging alone wouldn't
   fix it either. The maintainer's own issue already scopes a suggested shape: add the
   `cluster-async` feature, detect cluster mode via `INFO cluster` rather than requiring a
   flag, make queue naming hash-tag aware, and reject a non-zero `--db` on cluster
   endpoints with a clear error. If a PR claims to add or touches cluster support, check it
   against this real, specific, already-published shape rather than treating cluster
   support as an unscoped feature request.

6. **New dependencies require a maintainer check-in — written rule, not yet tested in the
   record.** AGENTS.md: "Do not introduce new dependencies without checking with the
   maintainer." Neither merged PR added a new crate, so this survey has no real example of
   the rule being enforced or an addition being challenged — cite it as real written
   doctrine on any PR that touches `Cargo.toml`'s dependency list, but don't claim
   precedent for how strictly it's applied in practice.

## Rust-specific engineering judgment: use it, but don't cite fake precedent for it

This is a Rust codebase, unlike redisbench-admin/memtier_benchmark's Python/C surface, so
generic Rust concerns are fair game on first-principles grounds — e.g. a `.unwrap()` or
`.expect()` on a value derived from external input (a CLI arg, a Redis reply, a parsed
URL) in a path that isn't already behind validated construction, or a new `unsafe` block.
Neither of the two real merged PRs happened to touch this territory (one was a CLI/URL
condition bug, the other a build-target/linking change), so there is no real, evidenced
celery-benchmark precedent to cite for panics-on-bad-input specifically. Reason about it
on its own merits if it comes up, and say plainly that this taxonomy doesn't have a real
citation for it yet — don't manufacture one.

## What this taxonomy is honestly thin or silent on

- **No reviewer voice or review back-and-forth exists at all** — see `voice-profiles.md`.
  Every item above is either a self-authored bugfix PR, a self-filed issue, or written
  doctrine — none is a reviewer's comment, because no reviewer comment exists anywhere in
  this repo's history yet.
- **No evidenced test-coverage enforcement mechanism** (no Codecov or equivalent tool
  referenced in CONTRIBUTING.md or the CI workflows) — "coverage should not decrease" has
  no automated number behind it in this repo, unlike redisbench-admin.
- **No precedent for a PR that was rejected, requested-changes, or went through a revision
  cycle** — both real PRs merged on the first pass, within single-digit minutes of being
  opened.
- **No precedent involving an external/first-time contributor** — all real PRs and issues
  are the maintainer's own.
- **No stray-file/dead-code reviewer catch** — CONTRIBUTING.md's "No dead code, no
  commented-out blocks" is written doctrine only; no real example of it being enforced
  found in this survey.

Given how new this repo is, re-mine this taxonomy once a second contributor's PRs and
reviews exist in the record — treat everything above as accurate for the *current* state,
not a permanent institutional voice.

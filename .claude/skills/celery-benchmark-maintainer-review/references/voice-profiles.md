# voice-profiles.md — celery-benchmark

Mined from the complete real GitHub history of `redis-performance/celery-benchmark` as
surveyed on 2026-08-27: 2 merged PRs (#2, #4), 3 issues (#1, #3, #5), **zero** PR review
entries (`gh api .../pulls/<n>/reviews` returns `[]` for both), **zero** PR issue-comments,
and **zero** issue comments, anywhere in the repo. This is a brand-new repo (created
2026-08-13, first commit within the last two weeks) with exactly one visible human across
every PR and every issue: `fcostaoliveira` (Filipe Oliveira).

## There is no reviewer voice to mine here — say so plainly

Both merged PRs were opened and merged by the same person, with no review request, no
approval body, and no comment thread at all:

- PR#2: opened 2026-08-17 22:11 UTC, merged 2026-08-17 22:15 UTC (4 minutes).
- PR#4: opened 2026-08-18 08:42 UTC, merged 2026-08-18 08:55 UTC (13 minutes).

Unlike `redisbench-admin` (which has at least one real, dialectic review — kei-nan on
PR#541) or `memtier_benchmark` (which has a deep multi-reviewer culture built up over
years), this repo's review record is currently **empty**. Do not invent a "maintainer
personality" or a distinct reviewer voice for this skill — there isn't one on record yet.
The generated review's tone should be measured and technical, grounded in what's actually
written down (AGENTS.md, CONTRIBUTING.md) and the two real shipped bugs below, not styled
as channeling a real person's real review habits the way the redisbench-admin and
memtier_benchmark skills legitimately can.

## What IS real and citable: the author's own PR-writing convention

Although no one has *reviewed* a PR here yet, the two merged PRs show a real, consistent
self-authoring pattern from fcostaoliveira, worth recognizing as the bar this codebase's
own PRs already clear — not as reviewer feedback, since no reviewer produced it:

- **Concrete repro with real command output**, not a description of the bug in the
  abstract. PR#4 attached `MONITOR` and showed `SELECT 13` issued for both `--db 0` and
  `--db 5`; PR#2 ran `objdump -T ... | grep GLIBC` and quoted `GLIBC_2.39`.
- **An explicit "why this matters" impact table** naming which real-world targets break.
  PR#4: local Redis vs. Redis Cluster vs. managed Redis. PR#2: Ubuntu 24.04 / 22.04 /
  Debian 12 / RHEL 9.
- **An explicit "why this went unnoticed" section.** PR#4: no error is raised, just a
  silently different database, invisible without attaching `MONITOR`. PR#2: the existing
  smoke-test step only runs the binary on its own build host, the one machine guaranteed
  to work, so it can't catch a portability regression.
- **Re-verification on the wire/artifact after the fix**, not just "tests pass." PR#4:
  re-ran with `MONITOR` attached for all three `--db` cases. PR#2: rebuilt for the new
  musl target and confirmed the artifact carries no `GLIBC_*` symbols.
- **New regression coverage named explicitly and tied to the exact bug.** PR#4: "Three
  regression tests cover all three paths." PR#2: a build-time assertion added specifically
  to fail the build if a `GLIBC_*` symbol regresses in.
- **Open questions flagged honestly rather than silently resolved.** PR#2's "Two notes for
  review" section admits the asset-name rename is a compatibility consideration and that
  the `musl-tools` install step might be unnecessary.

If a PR under review already does this kind of work in its own description, credit it —
the way the redisbench-admin skill credits JoanFM's design notes — as real self-review
already done, not something to "discover" and repeat. If a PR is thin on this (no repro,
no impact statement, no re-verification), that absence is worth naming, but be precise
about what it's being measured against: the codebase's own two real past PRs, not a mined
reviewer expectation, since no reviewer has ever asked for this in words here.

## Issues, not just PRs

All three real issues (#1, #3, #5) are also self-filed by fcostaoliveira, in the same
repro/impact/suggested-fix structure as the PR bodies. Issue #3 and PR #4 are the same bug
tracked start to finish; issue #1 and PR #2 likewise. Issue #5 (cluster-mode support) is
open and unresolved as of this survey — if a PR under review touches connection handling,
queue-key derivation, or the `redis::Client` construction, check it against issue #5's own
already-published suggested shape (a cluster-aware connection, hash-tag-aware queue
naming, `INFO cluster` detection, rejecting a non-zero `--db` on cluster) since that is a
real, specific, self-scoped gap, not a hypothetical concern to invent from scratch.

## What this skill is honestly thin or silent on

- **No evidenced reviewer pushback, ever** — no `COMMENTED` review, no requested changes,
  no back-and-forth on either merged PR. If a second contributor's review history
  accumulates later, re-mine this skill rather than trusting it as-is.
- **No precedent for how this repo handles an outside/first-time contributor.** All three
  issues and both PRs are the maintainer's own. CONTRIBUTING.md states the intent ("we
  treat this repo as 'Open Source' within Redis"), but there is no real example yet of an
  external PR being received, reviewed, or merged.
- **No automated coverage-percentage tool observed** (no Codecov reference in
  CONTRIBUTING.md or the CI workflows, unlike redisbench-admin) — "coverage should not
  decrease" is written doctrine with no automated number behind it here.
- **No precedent for a rejected/closed-without-merge PR, a force-pushed revision cycle, or
  an admin-merge override.** Both real merges completed within single-digit minutes with
  no visible review entry in the public API — consistent with either branch protection not
  yet requiring a review, or an admin override; the API doesn't distinguish these and this
  survey can't tell you which, so don't assert either as fact.
- **No multi-contributor voice comparison is possible** — there is exactly one human on
  record across every PR and issue.

Given all that, this skill's review persona should be: technical, grounded in the two real
bug precedents and the written AGENTS.md/CONTRIBUTING.md doctrine, and explicit about the
thin sample — never a claim to channel a "maintainer's real voice," because there isn't
one on record yet to channel.

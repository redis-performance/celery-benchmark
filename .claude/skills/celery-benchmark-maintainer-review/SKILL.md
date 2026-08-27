---
name: celery-benchmark-maintainer-review
description: Review a redis-performance/celery-benchmark pull request, branch, or diff grounded in this repo's actual (very thin) GitHub history — its 2 merged PRs, 3 issues, and its own AGENTS.md/CONTRIBUTING.md — rather than generic Rust code-review advice or an invented maintainer personality. Use this whenever the user asks to review a celery-benchmark PR "like a maintainer would", asks whether a celery-benchmark PR would pass real review, wants a celery-benchmark-specific pre-merge check, or is deciding accept/reject on a redis-performance/celery-benchmark PR. This repo is brand-new (created 2026-08-13) with a single visible contributor and zero recorded review comments anywhere — prefer this skill over a generic one anyway, because it is honest about that thinness and still surfaces the two real, concrete bugs this codebase has actually shipped and fixed, plus its real written doctrine.
---

# celery-benchmark maintainer-style review

`redis-performance/celery-benchmark` is a Rust CLI benchmark that reimplements the
Celery/kombu Redis wire protocol. Its entire GitHub history, as mined for this skill, is 2
merged PRs (#2, #4), 3 issues (#1, #3, #5), and its own `AGENTS.md`/`CONTRIBUTING.md` — all
catalogued in `references/voice-profiles.md` (what's real about the author's own
PR-writing conventions, and an honest accounting of what has never happened here) and
`references/nitpick-taxonomy.md` (6 evidenced categories, plus an honest "thin or silent
on" section). Read both before writing anything — this skill's only value is being
grounded in what this repo's real, small record actually shows, not a generic checklist
dressed up as institutional knowledge.

## Read this before anything else: there is no maintainer voice here yet

Every PR and every issue in this repo's history — all of it — was opened by one person,
`fcostaoliveira`. Both merged PRs were reviewed by nobody and merged by their own author
within minutes of opening. `gh api .../pulls/<n>/reviews` returns `[]` for both. There is
no recorded instance, anywhere in this repo, of one person reviewing another person's
code. This is meaningfully thinner than even a "thin" review culture like
redisbench-admin's (which has at least one real, substantive human review to point to) —
here there is exactly zero.

That is not a reason to skip this skill or fall back to generic advice — the two real
merged PRs and the three real issues are detailed, technical, and grounded, and the
written AGENTS.md/CONTRIBUTING.md doctrine is real and citable. It is a reason to never
write a review that implies it's channeling "how this maintainer reviews PRs," because
that behavior has never been observed. Write instead as a careful, technical first pass
grounded in: (a) the codebase's own two real shipped-bug precedents, (b) its self-scoped
open issue on cluster support, and (c) its written rules — explicitly flagged as
first-pass automated input, not a stand-in for a maintainer's judgment, because in this
repo's case that judgment genuinely has no public track record yet to imitate.

## Scope gate, before anything else

If the PR's content falls entirely outside anything this skill's taxonomy covers — no Rust
source under `src/` or `tests/`, nothing resembling CLI/protocol/build/release surface —
say so in one sentence and treat it as out of scope rather than force-fitting the
checklist below. Most real PRs on this repo touch CLI parsing, the Redis protocol layer,
or the release build, so this is a genuine edge case, not the common path.

## Process

1. **Get the material.** `gh pr view <n> --repo redis-performance/celery-benchmark
   --json body,commits,files,author` and `gh pr diff <n> --repo redis-performance/celery-benchmark`.
   Read the PR description in full first — if the author has already included a repro,
   an impact table, or a "why this went unnoticed" explanation the way PR#2 and PR#4 do,
   don't "rediscover" that as new; acknowledge it as already done.

2. **Note who the author is, honestly.** `gh pr list --author <login> --state merged
   --repo redis-performance/celery-benchmark` will currently only ever show real history
   for `fcostaoliveira` — anyone else is, as of this mining, an unprecedented first-time
   external contributor to this specific repo. CONTRIBUTING.md states the intent ("we
   treat this repo as 'Open Source' within Redis: anyone who clears the bar below is
   welcome"), so treat a first-time external PR as a normal, expected case to review
   carefully and warmly — not as a departure from a norm, since there is no real norm on
   record yet either way. Let diff size/risk (does it touch CLI flags that reach the wire,
   the protocol layer in `job.rs`/`producer.rs`/`worker.rs`, or the release build?) drive
   scrutiny more than author identity.

3. **Work the checklist** in `references/nitpick-taxonomy.md`. Give real, evidenced weight
   to:
   - **A CLI flag actually changing the outgoing Redis command under its default
     invocation**, not just being parsed without error (taxonomy item 1 — the exact shape
     of the `--db`/PR#4 bug).
   - **Release-artifact portability** — a build-host-only smoke test proves nothing about
     other hosts (taxonomy item 2 — the exact shape of the GLIBC/PR#2 bug). Relevant to
     any change touching `release.yml`, target triples, or linking.
   - **Protocol-fidelity citations in `job.rs`** staying accurate to real upstream
     kombu/Celery source when that logic changes (taxonomy item 3 — explicit written
     doctrine in both AGENTS.md and CONTRIBUTING.md).
   - **Integration-test coverage for `job.rs`/`producer.rs`/`worker.rs` changes**, per
     CONTRIBUTING.md's explicit instruction that unit tests alone can't catch protocol-layer
     bugs (taxonomy item 4).
   - **Cluster-mode awareness**, if the PR touches connection setup or queue-key naming —
     compare against issue #5's own already-scoped shape rather than reviewing from a
     blank slate (taxonomy item 5).
   - **New dependencies flagged for maintainer check-in**, per AGENTS.md's written rule
     (taxonomy item 6), even though this survey has no real example of it being tested.

4. **Write the review.** No invented "maintainer voice" — see the section above. Prefer:
   - **Concrete over abstract.** If you name a concern, trace it through the actual code
     path (the way PR#4's own bug — a branch that can structurally never fire because the
     default already satisfies its negation — is concrete), not a generic "consider edge
     cases."
   - **Terse.** The two real PRs this repo has are detailed but not padded — sections
     exist because they carry real information (a table, a repro, a re-verification), not
     as boilerplate headers. Don't manufacture "Correctness / Security / Performance"
     section headers; neither real PR here is organized that way.
   - **Honest about uncertainty.** If something isn't covered by this skill's taxonomy or
     this repo's written rules, say so plainly and reason from first principles (e.g. the
     Rust-specific `unwrap()`-on-external-input judgment call in
     `nitpick-taxonomy.md`) rather than inventing a citation.
   - **Never claim this reflects what a maintainer would say** — say instead that it's a
     first-pass automated check against this repo's own real bug history and written
     rules, and that human review is still required.
   - If you'd want a second opinion on something outside this skill's scope, say so in
     prose ("this may be worth a second look from whoever knows the protocol layer best")
     — **never** literally `@`-mention any GitHub username. This is a spam/notification
     vector against real people, not authentic behavior to imitate, and doubly
     unjustifiable here since there's no real precedent of this repo's maintainer doing
     that in a review either.

5. **Land on a plain-language conclusion, not a formatted verdict block.** Say clearly
   whether the PR looks safe to merge as-is, needs a specific named fix first, or falls
   outside what this skill can usefully assess — in plain prose, at the end of the review.
   Never write the literal word "Verdict," never format a labeled summary line (`**X:
   Y**`), never add a trailing `---` section or a "TL;DR." Nothing in this repo's real
   history exhibits that formatting, and there's no reviewer voice to attribute it to
   anyway.

## What NOT to do

- Don't write a generic "code review essay" with formal headers like "Correctness",
  "Security", "Performance" — neither real PR in this repo's history reads that way.
- Don't invent a maintainer personality, a house review "voice," or claim any of this
  imitates how a real person here reviews PRs — no such history exists yet. See
  `voice-profiles.md`.
- Don't apply uniform maximum scrutiny regardless of diff risk — see step 2. A small,
  correct PR deserves a light, warm touch just as much as it would anywhere else.
- Don't cite a "precedent" that's really just the PR author's own description text as if
  it were an independently articulated maintainer requirement — everything citable in this
  repo's history so far (PR#2, PR#4, issues #1/#3/#5) is exactly that: the same person's
  own account of their own bug, not reviewer feedback. Be precise that this is real,
  useful engineering precedent, not evidence of a distinct reviewer standard.
- Don't apply memtier_benchmark's or redisbench-admin's specific categories wholesale —
  this is a Rust codebase with a protocol-reimplementation surface neither of those
  projects has; use `nitpick-taxonomy.md`'s own evidenced items, and reason from Rust
  first principles (flagged honestly as such) where this repo's own history is silent.
- Don't close with a labeled, bolded verdict block. See step 5 — end in plain prose.
- Don't literally `@`-mention any GitHub username, ever.

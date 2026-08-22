# AGENTS.md

Guidance for AI coding agents in this repo. Human contributors: see [CONTRIBUTING.md](./CONTRIBUTING.md).

## Workflow

- Clarify the design before implementing. For anything non-trivial, agree on the approach first;
  prefer a short design note over jumping to code.
- One unit of change per commit. Never mix unrelated changes. Present the change for review
  before committing.
- Every change ships with tests. Run local CI before calling it done, and do not claim it passes
  without running it.
- Verify against the code and the tools: read before you answer, run before you assert.

Local CI:

```shell
cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-features
```

## Writing: code, comments, docs, commits

- Concise and to the point. No fluff. Explain the non-obvious; do not narrate the obvious.
- ASCII only. No em-dash and no `--`; write `-`. Do not use any non-ASCII glyph: write `->` for
  the right arrow, `<->` for the left-right arrow, `!=` for not-equal, straight quotes for curly
  ones, and the same for every other Unicode symbol. Applies everywhere, including this file.
- Comments justify *why*, not *what*. Delete any comment that restates the code.
- Do not use the word "seam"; say boundary, interface, or extension point.
- Do not use "bespoke"; say "custom".
- Use "etc.", not "...", when a list trails off.
- Describe mechanics literally, not figuratively. Do not say a value "rides along" with another,
  that a budget "stays charged", or that something "*is* the backpressure"; say what happens (a
  message takes/holds/reserves permits, which are returned on drop). Avoid "charged weight" and
  "charge it the whole budget"; write "the weight counted for a message" and "reserves the
  budget".

## Commits

- Conventional Commits (see CONTRIBUTING.md). Write the subject in the present tense, imperative
  voice: `feat: add try_send`, not `added` or `adds`.
- Keep the body minimal. The subject alone is often enough; add body lines only for the
  non-obvious *why*. Do not restate the diff or enumerate every file changed.
- Subject <= 72 characters. Wrap body lines at 72 columns and keep the body to a few lines; if it
  needs more, the change is probably too big for one commit. The `committed` hook and CI enforce
  the line length.
- Disclose AI with an `Assisted-by: Claude:claude-opus-4-8` trailer. Never `Co-Authored-By`, and
  never add a human's `Signed-off-by`.

## Tests

- Unit tests inline (`#[cfg(test)] mod tests`); public-surface tests in `tests/`.
- Put helpers *after* the tests that use them.
- Coverage must not drop. Keep line coverage at or above its current level, and never below 80%
  (aim for 90%+). CI fails the build under 80%.
- Prefer deterministic time: drive tokio's paused clock (`tokio::time::pause`) over real sleeps
  where practical. A short real sleep to observe that a send is *waiting* for room is the one
  accepted exception; keep it small.

## Terminology

- **weight**: the cost of a message in whatever unit the channel's budget uses (bytes is the
  common case), reported by the `Weigh` trait.
- **budget**: the maximum total weight of in-flight (sent, not yet dropped) messages, held as a
  tokio `Semaphore` whose permits are weight units.
- **permit**: one unit of budget. A message acquires `weight` permits to be sent; they return to
  the budget when its `Delivery` is dropped - that release is the backpressure.
- **Delivery**: the guard the receiver gets. It derefs to the message and returns its permits to
  the budget on drop.

## Code conventions

- No `unsafe`. This is a thin, safe layer over tokio's `mpsc` and `Semaphore`; keep it that way.
- Minimal dependencies: `tokio` (feature `sync`) is the only required runtime dependency; `bytes`
  is optional, behind the `weigh-bytes` feature. Do not add another without a strong, stated
  reason.
- Backpressure is the whole point. Never introduce an unbounded path; any new bound or buffer must
  be explicit and documented.
- Import a type by one path and use it consistently (a single `use std::sync::Arc`).
- Document every public item with rustdoc; keep it accurate and free of drift.

## CI workflows

- GitHub Actions live in `.github/workflows`. Write the workflow `name:`, every job name, and
  every named step in Sentence case, matching `ci.yml` (e.g. `name: CI`, `Check formatting`).
- Keep workflows minimal and scoped to one purpose; prefer the built-in `GITHUB_TOKEN` over a
  personal access token.

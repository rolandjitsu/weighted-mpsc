---
name: Bug Report
about: Report something that does not work as documented
title: "[BUG] "
labels: bug
assignees: ''

---

**Describe the bug**
A clear and concise description of what goes wrong.

**Minimal reproducible example**
The smallest `async` snippet that shows the problem:

```rust
use weighted_mpsc::channel;

// #[tokio::main] async fn main() {
let (tx, mut rx) = channel::<Vec<u8>>(16, 1024 * 1024);
// ...
// }
```

**Expected behavior**
What you expected to happen instead.

**Environment**
- weighted-mpsc version:
- tokio version and runtime flavor (current-thread / multi-thread):
- rustc version (`rustc --version`):
- OS / arch:

**Additional context**
Anything else that helps: a backtrace (`RUST_BACKTRACE=1`), whether it reproduces
under `--release`, and the `buffer` / `max_weight` / `Oversized` settings in use.

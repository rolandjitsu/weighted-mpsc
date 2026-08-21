# Security Policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately, not as a public issue. Use
GitHub's private vulnerability reporting for this repository (the "Security" tab
-> "Report a vulnerability"). Include a description, the affected version, and
reproduction steps. We aim to acknowledge within a few days and will keep you
posted on remediation.

## Scope

weighted-mpsc is a small, safe concurrency primitive: no `unsafe`, no I/O, no
network, no credentials. The security-relevant promise it makes is that the
weight bound holds. In scope, for example:

- A path where in-flight weight can exceed the configured budget (the bound does
  not hold).
- A panic, deadlock, or overflow in the weight accounting reachable from safe,
  documented use.

Documented behavior is not a vulnerability on its own: `Oversized::Clamp` letting
one over-budget message occupy the whole channel, `min_weight` of 0 allowing
zero-weight messages to be bounded only by the count buffer, and the ~4 GiB
budget cap are all by design and covered in the README and rustdoc.

## Supported versions

Pre-1.0: fixes land on the latest release published to crates.io.

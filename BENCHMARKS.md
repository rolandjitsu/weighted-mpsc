# Benchmarks

Cost of the weight accounting: `weighted-mpsc` (a bounded tokio mpsc that bounds by
message weight) against a bare, count-bounded `tokio::mpsc` with no weighing, on
the same workload.

- Workload: send + receive 10,000 messages of 1 KiB each, N producer tasks -> 1
  consumer, on a multi-thread tokio runtime.
- Tool: criterion; each cell is the median of its estimate.
- `bounded` = `weighted_mpsc::channel` (the shipped implementation).
  `baseline` = `tokio::sync::mpsc::channel` (no weighing).
- `overhead` = how much slower `bounded` is than `baseline` - i.e. what the weight
  bound costs.
- Core counts were set with `taskset` (Linux, e.g. `taskset -c 0` for one core) or
  run natively (macOS). Reproduce with `cargo bench --bench throughput`.

This is a pure send/recv microbenchmark with no real work between messages, so the
overhead percentages are a worst case. In any pipeline that does actual work or I/O
per message, the absolute cost (one extra semaphore acquire/release, tens to a few
hundred ns per message) is negligible.

## macOS (Apple silicon, 10 cores, native)

| Producers | bounded | baseline | overhead |
| --------: | ------: | -------: | -------: |
| 1         | 1.57 ms | 1.16 ms  | +35%     |
| 4         | 1.79 ms | 1.33 ms  | +34%     |
| 16        | 2.52 ms | 1.80 ms  | +41%     |
| 64        | 3.81 ms | 2.53 ms  | +50%     |

## x86-64 Linux (native, `taskset`-pinned)

| Cores | Producers | bounded | baseline | overhead |
| ----: | --------: | ------: | -------: | -------: |
| 1     | 1         | 3.06 ms | 2.19 ms  | +40%     |
| 1     | 4         | 2.69 ms | 2.16 ms  | +24%     |
| 1     | 16        | 2.64 ms | 2.18 ms  | +21%     |
| 1     | 64        | 2.72 ms | 2.25 ms  | +21%     |
| 2     | 1         | 5.25 ms | 3.96 ms  | +33%     |
| 2     | 4         | 3.30 ms | 2.32 ms  | +42%     |
| 2     | 16        | 3.75 ms | 2.97 ms  | +26%     |
| 2     | 64        | 3.49 ms | 3.02 ms  | +16%     |
| 4     | 1         | 4.99 ms | 3.74 ms  | +34%     |
| 4     | 4         | 2.97 ms | 2.36 ms  | +26%     |
| 4     | 16        | 3.49 ms | 2.94 ms  | +19%     |
| 4     | 64        | 4.12 ms | 3.57 ms  | +16%     |
| 20    | 1         | 4.90 ms | 3.81 ms  | +29%     |
| 20    | 4         | 3.23 ms | 2.42 ms  | +34%     |
| 20    | 16        | 3.93 ms | 3.03 ms  | +30%     |
| 20    | 64        | 4.56 ms | 3.85 ms  | +18%     |

## arm64 Linux (container, `taskset`-pinned)

| Cores | Producers | bounded | baseline | overhead |
| ----: | --------: | ------: | -------: | -------: |
| 1     | 1         | 1.66 ms | 1.25 ms  | +33%     |
| 1     | 4         | 1.50 ms | 1.37 ms  | +10%     |
| 1     | 16        | 1.56 ms | 1.25 ms  | +25%     |
| 1     | 64        | 1.54 ms | 1.26 ms  | +22%     |
| 2     | 1         | 2.67 ms | 2.39 ms  | +12%     |
| 2     | 4         | 2.45 ms | 2.07 ms  | +18%     |
| 2     | 16        | 2.54 ms | 2.15 ms  | +18%     |
| 2     | 64        | 2.37 ms | 2.19 ms  | +8%      |
| 4     | 1         | 2.96 ms | 2.65 ms  | +12%     |
| 4     | 4         | 2.07 ms | 1.86 ms  | +11%     |
| 4     | 16        | 2.37 ms | 1.80 ms  | +31%     |
| 4     | 64        | 2.17 ms | 1.88 ms  | +15%     |

## Takeaways

- The weight bound costs roughly +10% to +50% over a bare `tokio::mpsc` on this
  zero-work microbenchmark, most often in the +20% to +35% range. It does not blow
  up with producer count or core count.
- Absolute times are dominated by the hardware and the OS, not the channel: the
  Apple-silicon and arm-container runs are ~2x faster per operation than the x86
  box, and pinning to fewer cores is often *faster* for this latency-bound ping-pong
  (fewer cross-core wakeups).
- During development, runtime-agnostic (event-listener) and zero-dependency (Mutex)
  prototypes were benchmarked too. Across cores and architectures, none beat the
  shipped implementation at the 1-2 core operating point, so
  they were not adopted.

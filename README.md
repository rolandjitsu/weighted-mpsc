# weighted-mpsc

[![CI](https://img.shields.io/github/actions/workflow/status/rolandjitsu/weighted-mpsc/ci.yml?branch=main&style=flat-square&label=CI)](https://github.com/rolandjitsu/weighted-mpsc/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/codecov/c/github/rolandjitsu/weighted-mpsc/main?style=flat-square)](https://codecov.io/gh/rolandjitsu/weighted-mpsc)
[![crates.io](https://img.shields.io/crates/v/weighted-mpsc?style=flat-square)](https://crates.io/crates/weighted-mpsc)
[![docs.rs](https://img.shields.io/docsrs/weighted-mpsc?style=flat-square)](https://docs.rs/weighted-mpsc)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue?style=flat-square)](./LICENSE)

A bounded [tokio](https://tokio.rs) mpsc channel that bounds the queue by the
total **weight** of the messages in it, rather than by the number of messages.

Implement `Weigh` for your message type (usually its size in bytes) and give the
channel a weight budget; a send waits until the messages already in the channel
leave enough room, then goes through. Use it to cap the total size in flight - for
example the memory a producer/consumer pipeline holds - however many or few messages
that is. Weight need not be bytes; any additive measure works (rows, estimated cost,
etc.).

A `tokio::sync::Semaphore` holds the budget, one permit per weight unit: a message
takes permits equal to its weight until the `Lease` that `recv` returns is dropped
(or `Lease::into_inner` is called), which returns them and frees room. Holding the
`Lease` while you use the value keeps the bound covering it in the consumer, not
only while it sits in the queue.

## Usage

```rust
use weighted_mpsc::{channel, Weigh};

struct Frame {
    pixels: Vec<u8>,
}

// Tell the channel how to weigh a message (here: its heap bytes).
impl Weigh for Frame {
    fn weight(&self) -> usize {
        self.pixels.len()
    }
}

#[tokio::main]
async fn main() {
    // Cap the in-flight frames at 64 MiB; the count buffer (16) is a backstop.
    let (tx, mut rx) = channel::<Frame>(16, 64 * 1024 * 1024);

    tokio::spawn(async move {
        // send() waits here whenever admitting the next frame would exceed 64 MiB.
        tx.send(Frame { pixels: vec![0; 4 * 1024 * 1024] }).await.unwrap();
    });

    // `frame` derefs to the Frame; the budget it holds is returned when `frame`
    // is dropped, so hold it while the frame is genuinely in use.
    while let Some(frame) = rx.recv().await {
        assert_eq!(frame.pixels.len(), 4 * 1024 * 1024);
    }
}
```

`Weigh` is implemented for `Vec<u8>`, `String`, and `Box<[u8]>` (weight = byte
length) behind the default `weigh-std` feature. Enable the `weigh-bytes` feature
for `bytes::Bytes` and `BytesMut`, or turn off default features to drop the
built-in impls and weigh every type yourself.

### Messages larger than the whole budget

A message that weighs more than the entire budget can never wait for room, so you
pick what happens to it via `Builder`:

```rust
use weighted_mpsc::{Builder, Oversized};

let (tx, rx) = Builder::new(16, 64 * 1024 * 1024)
    .oversized(Oversized::Reject)   // Allow (default) | Reject | Drop
    .min_weight(1)                  // minimum weight counted per message
    .on_oversized(|weight, budget| eprintln!("oversized: {weight} > {budget}"))
    .build::<Vec<u8>>();
```

- `Allow` (default): send it anyway. It reserves the whole budget until it is
  received and its `Lease` dropped, so it runs on its own. The message is delivered
  whole - nothing is dropped or truncated.
- `Reject`: return `SendError::TooLarge` (does not consume the message); the budget
  is left untouched.
- `Drop`: discard it; `send` returns `Ok(())`. Use `on_oversized` to count or log.

Under `Allow`, an oversized message is fully in memory while it is in flight, so
peak memory can briefly exceed the budget by about that message's size (the budget
bounds what runs *alongside* a message, not the size of a single one). Size the
budget at or above your largest message, or use `Reject`/`Drop`, if you need the
budget to be a hard ceiling.

### Non-blocking send

`try_send` is the non-blocking alternative to `send`: it delivers the message if
there is room right now, or returns `TrySendError::Full` (does not consume the
message) instead of waiting for the budget or the count buffer. `Closed` and
`TooLarge` mean the same as for `send`.

```rust
use weighted_mpsc::{channel, TrySendError};

let (tx, _rx) = channel::<Vec<u8>>(16, 4096);
match tx.try_send(vec![0u8; 1024]) {
    Ok(()) => {}                                // delivered
    Err(TrySendError::Full(msg)) => {}          // no room now; `msg` not consumed
    Err(TrySendError::Closed(_)) => {}          // receiver dropped
    Err(TrySendError::TooLarge(_)) => {}        // over budget, policy is Reject
}
```

### Stream receiver (feature `stream`)

With the opt-in `stream` feature, `WeightedReceiver` implements
[`futures::Stream`](https://docs.rs/futures-core), yielding the same `Lease` guards
as `recv`. Drive it with any `StreamExt`:

```rust
use futures::StreamExt;

while let Some(lease) = rx.next().await {
    // `lease` derefs to the message; its budget is freed when it drops.
}
```

The feature pulls in one small, optional dependency (`futures-core`); the base crate
stays tokio-only.

## Benchmark

Sending and receiving 10,000 messages of 1 KiB each, against a raw count-bounded
`tokio::mpsc` as the baseline (criterion, on an Apple-silicon laptop):

| Channel                       |    Time | Throughput   |
| ----------------------------- | ------: | ------------ |
| `tokio::mpsc` (count-bounded) | 1.18 ms | 8.5 Melem/s  |
| `weighted-mpsc`               | 1.54 ms | 6.5 Melem/s  |

The weight accounting adds one semaphore acquire and release per message - about
36 ns each, or roughly 30% of throughput on this all-in-memory micro-benchmark.
In a pipeline whose cost is dominated by real work or I/O, that is negligible.
Reproduce with `cargo bench`; measure on your own hardware and workload before
drawing conclusions. Full results across x86-64 and arm64 at 1 to 20 cores are in
[BENCHMARKS.md](./BENCHMARKS.md).

## License

[Apache-2.0](./LICENSE).

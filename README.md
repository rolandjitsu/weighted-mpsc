# weighted-mpsc

[![CI](https://img.shields.io/github/actions/workflow/status/rolandjitsu/weighted-mpsc/ci.yml?branch=main&style=flat-square&label=CI)](https://github.com/rolandjitsu/weighted-mpsc/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/codecov/c/github/rolandjitsu/weighted-mpsc/main?style=flat-square)](https://codecov.io/gh/rolandjitsu/weighted-mpsc)
[![crates.io](https://img.shields.io/crates/v/weighted-mpsc?style=flat-square)](https://crates.io/crates/weighted-mpsc)
[![docs.rs](https://img.shields.io/docsrs/weighted-mpsc?style=flat-square)](https://docs.rs/weighted-mpsc)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue?style=flat-square)](./LICENSE)

A bounded [tokio](https://tokio.rs) mpsc channel that bounds the queue by the
total **weight** of the messages in it, rather than by the number of messages.

## What it does

You implement `Weigh` for your message type (usually returning its size in bytes)
and give the channel a weight budget. A send waits until the messages already in
the channel leave enough room for the new one, then goes through.

Reach for it when messages vary in size and you want to cap the total size in
flight - for example, to bound the memory a producer/consumer pipeline holds at
once, regardless of how many or how few messages that turns out to be. Weight does
not have to be bytes; any additive measure works (rows, estimated cost, etc.).

(This is the technique from
[this write-up](https://rolandsdev.blog/posts/glibc-to-musl-rust-memory/),
extracted into a small, reusable crate.)

## How it works

A `tokio::sync::Semaphore` holds the budget: one permit per weight unit. A message
takes permits equal to its weight while it is in the channel and while the receiver
still holds the `Lease` that `recv` returns. Dropping the `Lease` returns the
permits to the budget and frees room for more sends. Because the budget is held
until the `Lease` is dropped - not just until the message is received - the bound
covers the message while the consumer is still using it, not only while it sits in
the queue. Call `Lease::into_inner` to take the value out and free the budget
immediately if you want looser, plain-channel semantics.

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
- `Reject`: return `SendError::TooLarge` (which hands the message back); the budget
  is left untouched.
- `Drop`: discard it; `send` returns `Ok(())`. Use `on_oversized` to count or log.

Under `Allow`, an oversized message is fully in memory while it is in flight, so
peak memory can briefly exceed the budget by about that message's size (the budget
bounds what runs *alongside* a message, not the size of a single one). Size the
budget at or above your largest message, or use `Reject`/`Drop`, if you need the
budget to be a hard ceiling.

## Benchmark

Sending and receiving 10,000 messages of 1 KiB each, against a raw count-bounded
`tokio::mpsc` as the baseline (criterion, on an Apple-silicon laptop):

| Channel                       |    Time | Throughput   |
| ----------------------------- | ------: | ------------ |
| `tokio::mpsc` (count-bounded) | 1.18 ms | 8.5 Melem/s  |
| `weighted-mpsc`               | 1.54 ms | 6.5 Melem/s  |

The weight accounting adds one semaphore acquire and release per message - about
36 ns each here, or roughly 30% of throughput on this all-in-memory micro-benchmark.
In a pipeline whose cost is dominated by real work or I/O, that is negligible.
Reproduce with `cargo bench`; measure on your own hardware and workload before
drawing conclusions.

## Install

```sh
cargo add weighted-mpsc
```

## Status / scope

Early, single-maintainer software. The surface is intentionally small: a weighted
sender/receiver, the `Lease` guard, and the oversized policy. Not here yet, and
plausibly worth adding: a `try_send` and a `futures::Stream` receiver.
Contributions welcome.

## License

[Apache-2.0](./LICENSE). One dependency: `tokio`.

//! A bounded [tokio] mpsc channel that bounds the queue by the total *weight* of
//! the messages in it, rather than by the number of messages.
//!
//! Each type sent through the channel reports a weight via [`Weigh`] - usually its
//! size in bytes, but any additive measure works (rows, estimated cost, etc.). The
//! channel has a fixed weight budget; a send waits until the messages already in
//! the channel leave enough room for the new one, then goes through. This lets you
//! cap the memory (or any other weighed resource) a producer/consumer pipeline
//! holds at once, even when messages vary a lot in size.
//!
//! A [`tokio::sync::Semaphore`] tracks the budget: one permit per weight unit. A
//! message takes permits equal to its weight while it is in the channel and while
//! the receiver still holds the [`Lease`] that [`recv`] returns. The permits go
//! back to the budget when the `Lease` is dropped, which frees room for more sends.
//! Holding the `Lease` while you use the value keeps the bound accurate; dropping
//! it (or calling [`Lease::into_inner`]) frees the room immediately.
//!
//! [recv]: WeightedReceiver::recv
//!
//! ```no_run
//! use weighted_mpsc::channel;
//!
//! # async fn example() {
//! // Cap the in-flight bytes at 1 MiB; the count buffer (16) is a backstop.
//! let (tx, mut rx) = channel::<Vec<u8>>(16, 1024 * 1024);
//!
//! tx.send(vec![0u8; 256 * 1024]).await.unwrap();
//!
//! // `msg` derefs to the Vec<u8>; the budget is held until `msg` is dropped.
//! let msg = rx.recv().await.unwrap();
//! assert_eq!(msg.len(), 256 * 1024);
//! # }
//! ```
//!
//! [tokio]: https://docs.rs/tokio

use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// The weight budget is capped at `u32::MAX` (about 4 GiB) because a single
/// `acquire_many` takes a `u32`; [`Builder::build`] panics above that. Individual
/// messages may weigh more - they are handled as oversized (see [`Oversized`]).
const MAX_PERMITS: usize = u32::MAX as usize;

/// A value that can report its own weight in the unit the channel's budget uses.
///
/// Weight is almost always a byte count, but it can be any additive measure: the
/// budget is just the total weight allowed in the channel at once. Report the cost
/// that dominates the resource you want to bound (usually heap bytes); it does not
/// need to be exact.
pub trait Weigh {
    /// The weight of this value. Bytes is the common case.
    fn weight(&self) -> usize;
}

/// Convenience [`Weigh`] impls for common owned byte/text containers (weight =
/// byte length), behind the default `weigh-std` feature. Turn off default features
/// to drop them, e.g. to weigh one of these types differently via a newtype.
#[cfg(feature = "weigh-std")]
mod weigh_std {
    use super::Weigh;

    impl Weigh for Vec<u8> {
        fn weight(&self) -> usize {
            self.len()
        }
    }

    impl Weigh for String {
        fn weight(&self) -> usize {
            self.len()
        }
    }

    impl Weigh for Box<[u8]> {
        fn weight(&self) -> usize {
            self.len()
        }
    }
}

/// [`Weigh`] impls for the [`bytes`](https://docs.rs/bytes) types (weight = byte
/// length), behind the `weigh-bytes` feature.
#[cfg(feature = "weigh-bytes")]
mod weigh_bytes {
    use super::Weigh;

    impl Weigh for bytes::Bytes {
        fn weight(&self) -> usize {
            self.len()
        }
    }

    impl Weigh for bytes::BytesMut {
        fn weight(&self) -> usize {
            self.len()
        }
    }
}

/// What to do with a message that weighs more than the whole budget.
///
/// Such a message cannot fit even in an empty channel, so waiting for room would
/// wait forever. This selects the alternative. The choice is independent of
/// [`Builder::on_oversized`], which observes the event either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Oversized {
    /// Send the message anyway. It reserves the entire budget while it is in the
    /// channel, so nothing else can be in flight until it is received and its
    /// [`Lease`] dropped. The message is delivered whole: no bytes are dropped or
    /// truncated. This is the default.
    ///
    /// Note the message is fully in memory while in flight, so an oversized message
    /// makes peak memory briefly exceed the budget by about its own size. Size the
    /// budget at or above your largest message if you need it to be a hard ceiling.
    #[default]
    Allow,
    /// Do not send it: [`WeightedSender::send`] returns [`SendError::TooLarge`] and
    /// the budget is left untouched.
    Reject,
    /// Discard it: `send` returns `Ok(())` without sending. Pair it with
    /// [`Builder::on_oversized`] to count or log the discards.
    Drop,
}

/// The error returned by [`WeightedSender::send`].
pub enum SendError<T> {
    /// The receiver (and every clone of it) was dropped, so the message could not
    /// be delivered. Does not consume the message.
    Closed(T),
    /// The message weighs more than the whole budget and the channel's [`Oversized`]
    /// policy is [`Oversized::Reject`]. Does not consume the message.
    TooLarge(T),
}

impl<T> SendError<T> {
    /// Recover the message that could not be sent.
    pub fn into_inner(self) -> T {
        match self {
            SendError::Closed(v) | SendError::TooLarge(v) => v,
        }
    }
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::Closed(_) => f.write_str("SendError::Closed(..)"),
            SendError::TooLarge(_) => f.write_str("SendError::TooLarge(..)"),
        }
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::Closed(_) => f.write_str("channel closed: the receiver was dropped"),
            SendError::TooLarge(_) => f.write_str("message weighs more than the channel budget"),
        }
    }
}

impl<T> std::error::Error for SendError<T> {}

/// The error returned by [`WeightedSender::try_send`].
pub enum TrySendError<T> {
    /// There is no room right now: admitting the message would exceed the weight
    /// budget, or the count buffer is full. A later `try_send` may succeed once the
    /// receiver drains room. Does not consume the message.
    Full(T),
    /// The receiver (and every clone of it) was dropped, so the message could not
    /// be delivered. Does not consume the message.
    Closed(T),
    /// The message weighs more than the whole budget and the channel's [`Oversized`]
    /// policy is [`Oversized::Reject`]. Does not consume the message.
    TooLarge(T),
}

impl<T> TrySendError<T> {
    /// Recover the message that could not be sent.
    pub fn into_inner(self) -> T {
        match self {
            TrySendError::Full(v) | TrySendError::Closed(v) | TrySendError::TooLarge(v) => v,
        }
    }
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => f.write_str("TrySendError::Full(..)"),
            TrySendError::Closed(_) => f.write_str("TrySendError::Closed(..)"),
            TrySendError::TooLarge(_) => f.write_str("TrySendError::TooLarge(..)"),
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => {
                f.write_str("channel full: no room in the weight budget or count buffer")
            }
            TrySendError::Closed(_) => f.write_str("channel closed: the receiver was dropped"),
            TrySendError::TooLarge(_) => f.write_str("message weighs more than the channel budget"),
        }
    }
}

impl<T> std::error::Error for TrySendError<T> {}

/// Observer invoked when a message weighs more than the whole budget: called with
/// `(message_weight, max_weight)`.
type OversizedHook = Arc<dyn Fn(usize, usize) + Send + Sync>;

/// Builder for a weighted channel. Use [`channel`] for the common case.
#[derive(Clone)]
pub struct Builder {
    buffer: usize,
    max_weight: usize,
    min_weight: usize,
    oversized: Oversized,
    on_oversized: Option<OversizedHook>,
}

impl Builder {
    /// Create a builder with the two required bounds:
    ///
    /// - `buffer`: the message-count buffer of the underlying tokio channel. It is
    ///   the backstop for zero- or near-zero-weight messages, which the weight
    ///   budget alone would not bound. Must be `> 0`.
    /// - `max_weight`: the total weight allowed in the channel at once. Must be in
    ///   `1..=u32::MAX` (about 4 GiB).
    ///
    /// Defaults: [`Oversized::Allow`], `min_weight` of 1, and no observer.
    pub fn new(buffer: usize, max_weight: usize) -> Self {
        Self {
            buffer,
            max_weight,
            min_weight: 1,
            oversized: Oversized::default(),
            on_oversized: None,
        }
    }

    /// Minimum weight counted for any message. A message lighter than this still
    /// takes this much budget, so a flood of tiny messages cannot fill the channel
    /// without the budget noticing. Defaults to 1; set it to 0 to let zero-weight
    /// messages through without taking any budget (then only `buffer` bounds them).
    pub fn min_weight(mut self, min_weight: usize) -> Self {
        self.min_weight = min_weight;
        self
    }

    /// How to handle a message that weighs more than the whole budget. See
    /// [`Oversized`].
    pub fn oversized(mut self, policy: Oversized) -> Self {
        self.oversized = policy;
        self
    }

    /// Register an observer called as `(message_weight, max_weight)` whenever a
    /// message weighs more than the budget, whatever the [`Oversized`] choice.
    pub fn on_oversized<F>(mut self, hook: F) -> Self
    where
        F: Fn(usize, usize) + Send + Sync + 'static,
    {
        self.on_oversized = Some(Arc::new(hook));
        self
    }

    /// Build the sender/receiver pair.
    ///
    /// # Panics
    ///
    /// Panics if `buffer` is 0, or if `max_weight` is 0 or greater than `u32::MAX`.
    pub fn build<T>(self) -> (WeightedSender<T>, WeightedReceiver<T>) {
        assert!(self.buffer > 0, "buffer must be > 0");
        assert!(self.max_weight > 0, "max_weight must be > 0");
        // A single `acquire_many` takes a `u32`, so the budget cannot exceed that.
        // Fail loudly here rather than silently shrink what the caller asked for.
        assert!(
            self.max_weight <= MAX_PERMITS,
            "max_weight must be <= u32::MAX (about 4 GiB)"
        );

        let budget = Arc::new(Semaphore::new(self.max_weight));
        let (tx, rx) = mpsc::channel(self.buffer);

        let sender = WeightedSender {
            tx,
            budget: Arc::clone(&budget),
            max_weight: self.max_weight,
            min_weight: self.min_weight,
            oversized: self.oversized,
            on_oversized: self.on_oversized,
        };
        let receiver = WeightedReceiver { rx, budget };
        (sender, receiver)
    }
}

impl fmt::Debug for Builder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Builder")
            .field("buffer", &self.buffer)
            .field("max_weight", &self.max_weight)
            .field("min_weight", &self.min_weight)
            .field("oversized", &self.oversized)
            .finish_non_exhaustive()
    }
}

/// Convenience constructor for a weighted channel with the default policy
/// ([`Oversized::Allow`], `min_weight` 1). For anything else, use [`Builder`].
///
/// See [`Builder::new`] for the meaning of `buffer` and `max_weight`.
///
/// # Panics
///
/// Panics if `buffer` or `max_weight` is 0.
pub fn channel<T>(buffer: usize, max_weight: usize) -> (WeightedSender<T>, WeightedReceiver<T>) {
    Builder::new(buffer, max_weight).build()
}

/// The sending half of a weighted channel. Cloneable and shareable across tasks.
pub struct WeightedSender<T> {
    tx: mpsc::Sender<Lease<T>>,
    budget: Arc<Semaphore>,
    max_weight: usize,
    min_weight: usize,
    oversized: Oversized,
    on_oversized: Option<OversizedHook>,
}

// Manual: `T` need not be `Clone` for the sender to be.
impl<T> Clone for WeightedSender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            budget: Arc::clone(&self.budget),
            max_weight: self.max_weight,
            min_weight: self.min_weight,
            oversized: self.oversized,
            on_oversized: self.on_oversized.clone(),
        }
    }
}

impl<T: Weigh> WeightedSender<T> {
    /// Send a message, waiting until there is room in the budget for its weight.
    ///
    /// Waits (asynchronously) while the messages already in the channel plus this
    /// one would exceed the budget, then delivers it. A message that weighs more
    /// than the whole budget is handled per the channel's [`Oversized`] policy.
    ///
    /// # Errors
    ///
    /// - [`SendError::Closed`] if the receiver has been dropped.
    /// - [`SendError::TooLarge`] if the message weighs more than the budget and the
    ///   policy is [`Oversized::Reject`].
    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        let weight = value.weight().max(self.min_weight);

        let reserve = if weight > self.max_weight {
            if let Some(hook) = &self.on_oversized {
                hook(weight, self.max_weight);
            }
            match self.oversized {
                Oversized::Reject => return Err(SendError::TooLarge(value)),
                Oversized::Drop => return Ok(()),
                // Cannot reserve more than exists, so reserve all of it. The message
                // is still delivered whole.
                Oversized::Allow => self.max_weight,
            }
        } else {
            weight
        };

        // `reserve <= max_weight <= MAX_PERMITS` (checked in `build`), so the cast
        // is lossless.
        let permit = match Arc::clone(&self.budget)
            .acquire_many_owned(reserve as u32)
            .await
        {
            Ok(permit) => permit,
            // The budget is only ever closed by the receiver being dropped (see
            // `WeightedReceiver::drop`), which also unblocks a send waiting here.
            Err(_) => return Err(SendError::Closed(value)),
        };

        self.tx
            .send(Lease {
                value,
                weight: reserve,
                _permit: permit,
            })
            .await
            .map_err(|e| SendError::Closed(e.0.value))
    }

    /// Try to send a message without waiting.
    ///
    /// Like [`send`](Self::send), but never waits: if admitting the message would
    /// exceed the budget, or the count buffer is full, it returns
    /// [`TrySendError::Full`] right away instead of waiting for room. A message that
    /// weighs more than the whole budget is handled per the channel's [`Oversized`]
    /// policy.
    ///
    /// # Errors
    ///
    /// - [`TrySendError::Full`] if there is no room right now (weight budget or count
    ///   buffer).
    /// - [`TrySendError::Closed`] if the receiver has been dropped.
    /// - [`TrySendError::TooLarge`] if the message weighs more than the budget and
    ///   the policy is [`Oversized::Reject`].
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let weight = value.weight().max(self.min_weight);

        let reserve = if weight > self.max_weight {
            if let Some(hook) = &self.on_oversized {
                hook(weight, self.max_weight);
            }
            match self.oversized {
                Oversized::Reject => return Err(TrySendError::TooLarge(value)),
                Oversized::Drop => return Ok(()),
                Oversized::Allow => self.max_weight,
            }
        } else {
            weight
        };

        // `reserve <= max_weight <= MAX_PERMITS` (checked in `build`), so the cast
        // is lossless.
        let permit = match Arc::clone(&self.budget).try_acquire_many_owned(reserve as u32) {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => return Err(TrySendError::Full(value)),
            // The budget is only ever closed by the receiver being dropped (see
            // `WeightedReceiver::drop`).
            Err(TryAcquireError::Closed) => return Err(TrySendError::Closed(value)),
        };

        // On `Full`/`Closed` the returned `Lease` is dropped as we recover the value,
        // which returns the permit to the budget; only the value goes to the caller.
        self.tx
            .try_send(Lease {
                value,
                weight: reserve,
                _permit: permit,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(lease) => TrySendError::Full(lease.into_inner()),
                mpsc::error::TrySendError::Closed(lease) => {
                    TrySendError::Closed(lease.into_inner())
                }
            })
    }

    /// The channel's total weight budget (the value passed to [`Builder::new`]).
    pub fn max_weight(&self) -> usize {
        self.max_weight
    }

    /// Budget not currently reserved by messages in the channel, in weight units.
    pub fn available_weight(&self) -> usize {
        self.budget.available_permits()
    }
}

impl<T> fmt::Debug for WeightedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WeightedSender")
            .field("max_weight", &self.max_weight)
            .field("min_weight", &self.min_weight)
            .field("oversized", &self.oversized)
            .field("available_weight", &self.budget.available_permits())
            .finish_non_exhaustive()
    }
}

/// The receiving half of a weighted channel.
///
/// Dropping the receiver closes the channel: any producer waiting for room in
/// [`WeightedSender::send`] wakes with [`SendError::Closed`].
pub struct WeightedReceiver<T> {
    rx: mpsc::Receiver<Lease<T>>,
    budget: Arc<Semaphore>,
}

impl<T> WeightedReceiver<T> {
    /// Receive the next message, or `None` once every sender is dropped and the
    /// channel is drained.
    ///
    /// The returned [`Lease`] holds the message's budget until it is dropped, so
    /// hold it while the message is in use, or call [`Lease::into_inner`] to take
    /// the value and free the budget now.
    pub async fn recv(&mut self) -> Option<Lease<T>> {
        self.rx.recv().await
    }

    /// Close the channel without dropping the receiver: senders stop, but messages
    /// already in the channel can still be drained with [`recv`](Self::recv).
    pub fn close(&mut self) {
        self.rx.close();
        self.budget.close();
    }
}

impl<T> Drop for WeightedReceiver<T> {
    fn drop(&mut self) {
        // Wake any sender waiting on `acquire_many_owned`; without this a producer
        // waiting for room when the last receiver goes away would hang forever,
        // since dropping the receiver does not by itself return permits.
        self.budget.close();
    }
}

impl<T> fmt::Debug for WeightedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WeightedReceiver")
            .field("available_weight", &self.budget.available_permits())
            .finish_non_exhaustive()
    }
}

/// A received message together with the budget it holds.
///
/// Derefs to the message, so use it as you would the value itself. The budget the
/// message took is returned to the channel when the `Lease` is dropped - so hold it
/// while you are still using the value to keep the bound accurate, and drop it (or
/// call [`into_inner`](Self::into_inner)) to free the room for more sends.
pub struct Lease<T> {
    value: T,
    weight: usize,
    // Returned to the budget on drop; that release frees room for the next send.
    // The leading underscore documents that it is held only for its Drop effect.
    _permit: OwnedSemaphorePermit,
}

impl<T> Lease<T> {
    /// Take the message out, releasing its budget.
    pub fn into_inner(self) -> T {
        self.value
    }

    /// Weight reserved for this message (after `min_weight` and the oversized
    /// policy).
    pub fn weight(&self) -> usize {
        self.weight
    }
}

impl<T> Deref for Lease<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> DerefMut for Lease<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T: fmt::Debug> fmt::Debug for Lease<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Lease")
            .field("value", &self.value)
            .field("weight", &self.weight)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    #[derive(Debug)]
    struct Msg(Vec<u8>);

    impl Weigh for Msg {
        fn weight(&self) -> usize {
            self.0.len()
        }
    }

    fn msg(bytes: usize) -> Msg {
        Msg(vec![0u8; bytes])
    }

    #[tokio::test]
    async fn delivers_all_messages() {
        let (tx, mut rx) = channel::<Msg>(8, 1024 * 1024);
        // Produce concurrently: with a count buffer of 8 and 10 messages, a
        // produce-all-then-consume loop would block the 9th send on the buffer.
        let producer = tokio::spawn(async move {
            for _ in 0..10 {
                tx.send(msg(1000)).await.unwrap();
            }
        });

        let mut got = 0;
        while let Some(d) = rx.recv().await {
            assert_eq!(d.0.len(), 1000);
            got += 1;
        }
        producer.await.unwrap();
        assert_eq!(got, 10);
    }

    #[tokio::test]
    async fn bounds_by_weight_not_count() {
        // Budget of 1000 bytes, generous count buffer. A second 600-byte message
        // must wait even though the count buffer is nowhere near full.
        let (tx, mut rx) = channel::<Msg>(64, 1000);
        tx.send(msg(600)).await.unwrap();
        assert_eq!(tx.available_weight(), 400);

        let tx2 = tx.clone();
        let blocked = tokio::spawn(async move { tx2.send(msg(600)).await });

        // Give the spawned send a chance to run: 600 + 600 > 1000 and nothing has
        // been consumed, so it must still be waiting for room.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!blocked.is_finished(), "send should be waiting for room");

        // Free the first message; now the second fits.
        let first = rx.recv().await.unwrap();
        assert_eq!(first.weight(), 600);
        drop(first);

        timeout(Duration::from_millis(200), blocked)
            .await
            .expect("send should unblock once room frees")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn holding_lease_keeps_budget_reserved() {
        let (tx, mut rx) = channel::<Msg>(64, 1000);
        tx.send(msg(400)).await.unwrap();
        let held = rx.recv().await.unwrap();
        // The budget stays reserved while the Lease is alive, not just while the
        // message sits in the channel.
        assert_eq!(tx.available_weight(), 600);
        drop(held);
        assert_eq!(tx.available_weight(), 1000);
    }

    #[tokio::test]
    async fn into_inner_frees_budget() {
        let (tx, mut rx) = channel::<Msg>(64, 1000);
        tx.send(msg(400)).await.unwrap();
        let value = rx.recv().await.unwrap().into_inner();
        // Taking the value out drops the Lease, so the budget is freed immediately.
        assert_eq!(tx.available_weight(), 1000);
        assert_eq!(value.0.len(), 400);
    }

    #[tokio::test]
    async fn oversized_allow_delivers_whole_message() {
        let (tx, mut rx) = channel::<Msg>(64, 1000);
        // 5000 > 1000: delivered whole, but reserves the entire budget.
        tx.send(msg(5000)).await.unwrap();
        assert_eq!(tx.available_weight(), 0);
        let d = rx.recv().await.unwrap();
        assert_eq!(d.weight(), 1000);
        assert_eq!(d.0.len(), 5000, "no bytes dropped or truncated");
    }

    #[tokio::test]
    async fn oversized_reject_returns_the_message() {
        let (tx, mut rx) = Builder::new(64, 1000)
            .oversized(Oversized::Reject)
            .build::<Msg>();
        let err = tx.send(msg(5000)).await.unwrap_err();
        match err {
            SendError::TooLarge(m) => assert_eq!(m.0.len(), 5000),
            other => panic!("expected TooLarge, got {other:?}"),
        }
        assert_eq!(tx.available_weight(), 1000, "budget untouched on reject");
        // A normal message still flows.
        tx.send(msg(10)).await.unwrap();
        assert!(rx.recv().await.is_some());
    }

    #[tokio::test]
    async fn oversized_drop_silently_discards() {
        let seen = Arc::new(AtomicUsize::new(0));
        let seen2 = Arc::clone(&seen);
        let (tx, mut rx) = Builder::new(64, 1000)
            .oversized(Oversized::Drop)
            .on_oversized(move |w, max| {
                assert_eq!((w, max), (5000, 1000));
                seen2.fetch_add(1, Ordering::SeqCst);
            })
            .build::<Msg>();

        tx.send(msg(5000)).await.unwrap(); // discarded, Ok(())
        assert_eq!(seen.load(Ordering::SeqCst), 1);
        assert_eq!(tx.available_weight(), 1000);

        tx.send(msg(10)).await.unwrap();
        let d = rx.recv().await.unwrap();
        assert_eq!(d.0.len(), 10);
    }

    #[tokio::test]
    async fn min_weight_floors_cheap_messages() {
        // Zero-weight messages would otherwise never take any budget.
        let (tx, _rx) = Builder::new(64, 10).min_weight(2).build::<Msg>();
        tx.send(msg(0)).await.unwrap();
        assert_eq!(tx.available_weight(), 8);
    }

    #[test]
    #[should_panic(expected = "max_weight must be <= u32::MAX")]
    fn rejects_budget_over_u32() {
        let _ = Builder::new(8, MAX_PERMITS + 1).build::<Msg>();
    }

    #[cfg(feature = "weigh-std")]
    #[tokio::test]
    async fn weighs_std_types() {
        // Vec<u8> and String are covered by the integration test; check Box<[u8]>.
        let (tx, _rx) = channel::<Box<[u8]>>(8, 1000);
        tx.send(vec![0u8; 300].into_boxed_slice()).await.unwrap();
        assert_eq!(tx.available_weight(), 700);
    }

    #[cfg(feature = "weigh-bytes")]
    #[tokio::test]
    async fn weighs_bytes() {
        let (tx, mut rx) = channel::<bytes::Bytes>(8, 1000);
        tx.send(bytes::Bytes::from(vec![0u8; 400])).await.unwrap();
        assert_eq!(tx.available_weight(), 600);
        let d = rx.recv().await.unwrap();
        assert_eq!(d.len(), 400);
    }

    #[tokio::test]
    async fn dropping_receiver_unblocks_waiting_sender() {
        let (tx, rx) = channel::<Msg>(64, 1000);
        tx.send(msg(1000)).await.unwrap(); // budget now full

        let tx2 = tx.clone();
        let blocked = tokio::spawn(async move { tx2.send(msg(1000)).await });

        // Drop the receiver while a send is waiting for room.
        drop(rx);

        let res = timeout(Duration::from_millis(200), blocked)
            .await
            .expect("send should wake when the receiver drops")
            .unwrap();
        assert!(matches!(res, Err(SendError::Closed(_))));
    }

    #[tokio::test]
    async fn send_after_receiver_dropped_is_closed() {
        let (tx, rx) = channel::<Msg>(8, 1000);
        drop(rx);
        let err = tx.send(msg(1)).await.unwrap_err();
        assert!(matches!(err, SendError::Closed(_)));
        assert_eq!(err.into_inner().0.len(), 1);
    }

    #[tokio::test]
    async fn try_send_delivers_when_room() {
        let (tx, mut rx) = channel::<Msg>(8, 1000);
        tx.try_send(msg(400)).unwrap();
        assert_eq!(tx.available_weight(), 600);
        let d = rx.recv().await.unwrap();
        assert_eq!(d.weight(), 400);
    }

    #[tokio::test]
    async fn try_send_full_when_budget_exhausted() {
        let (tx, _rx) = channel::<Msg>(64, 1000);
        tx.try_send(msg(1000)).unwrap(); // budget now full
        let err = tx.try_send(msg(1)).unwrap_err();
        assert!(matches!(err, TrySendError::Full(_)));
        assert_eq!(err.into_inner().0.len(), 1);
        // The rejected send left the budget untouched.
        assert_eq!(tx.available_weight(), 0);
    }

    #[tokio::test]
    async fn try_send_full_when_count_buffer_full() {
        // Count buffer of 1 with a generous budget: the second message is refused by
        // the buffer, not the budget, and its permit must return to the budget.
        let (tx, _rx) = channel::<Msg>(1, 1000);
        tx.try_send(msg(10)).unwrap();
        let err = tx.try_send(msg(10)).unwrap_err();
        assert!(matches!(err, TrySendError::Full(_)));
        assert_eq!(tx.available_weight(), 990, "permit returned on buffer-full");
    }

    #[tokio::test]
    async fn try_send_closed_after_receiver_dropped() {
        let (tx, rx) = channel::<Msg>(8, 1000);
        drop(rx);
        let err = tx.try_send(msg(1)).unwrap_err();
        assert!(matches!(err, TrySendError::Closed(_)));
        assert_eq!(err.into_inner().0.len(), 1);
    }

    #[tokio::test]
    async fn try_send_oversized_reject_returns_the_message() {
        let (tx, _rx) = Builder::new(64, 1000)
            .oversized(Oversized::Reject)
            .build::<Msg>();
        let err = tx.try_send(msg(5000)).unwrap_err();
        assert!(matches!(err, TrySendError::TooLarge(_)));
        assert_eq!(tx.available_weight(), 1000, "budget untouched on reject");
    }

    #[tokio::test]
    async fn try_send_oversized_drop_silently_discards() {
        let seen = Arc::new(AtomicUsize::new(0));
        let seen2 = Arc::clone(&seen);
        let (tx, _rx) = Builder::new(64, 1000)
            .oversized(Oversized::Drop)
            .on_oversized(move |_, _| {
                seen2.fetch_add(1, Ordering::SeqCst);
            })
            .build::<Msg>();
        tx.try_send(msg(5000)).unwrap(); // discarded, Ok(())
        assert_eq!(seen.load(Ordering::SeqCst), 1);
        assert_eq!(tx.available_weight(), 1000);
    }

    #[tokio::test]
    async fn try_send_oversized_allow_reserves_whole_budget() {
        let (tx, mut rx) = channel::<Msg>(64, 1000);
        tx.try_send(msg(5000)).unwrap();
        assert_eq!(tx.available_weight(), 0);
        let d = rx.recv().await.unwrap();
        assert_eq!(d.weight(), 1000);
        assert_eq!(d.0.len(), 5000, "no bytes dropped or truncated");
    }
}

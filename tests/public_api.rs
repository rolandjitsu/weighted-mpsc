//! Public-surface tests: test the crate exactly as a downstream user would.

use std::time::Duration;

use tokio::time::timeout;
use weighted_mpsc::{Builder, Oversized, SendError, TrySendError, channel};

#[tokio::test]
async fn end_to_end_backpressure() {
    // 4 KiB budget; the count buffer is deliberately large so the *weight* is what
    // bounds the pipe.
    let (tx, mut rx) = channel::<Vec<u8>>(32, 4096);
    tx.send(vec![1u8; 3000]).await.unwrap();

    let tx2 = tx.clone();
    let blocked = tokio::spawn(async move { tx2.send(vec![2u8; 3000]).await });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!blocked.is_finished(), "second send should wait for budget");

    let first = rx.recv().await.unwrap();
    assert_eq!(first.len(), 3000); // Delivery derefs to Vec<u8>
    assert_eq!(first.weight(), 3000);
    drop(first);

    timeout(Duration::from_millis(200), blocked)
        .await
        .expect("send should unblock once budget frees")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn reject_policy_hands_the_message_back() {
    let (tx, _rx) = Builder::new(8, 100)
        .oversized(Oversized::Reject)
        .build::<String>();

    let err = tx.send("x".repeat(500)).await.unwrap_err();
    assert!(matches!(err, SendError::TooLarge(_)));
    assert_eq!(err.into_inner().len(), 500);
}

#[tokio::test]
async fn try_send_refuses_when_full_then_succeeds_after_draining() {
    let (tx, mut rx) = channel::<Vec<u8>>(32, 4096);
    tx.try_send(vec![0u8; 4096]).unwrap(); // budget now full

    let err = tx.try_send(vec![1u8; 1]).unwrap_err();
    assert!(matches!(err, TrySendError::Full(_)));
    assert_eq!(err.into_inner().len(), 1); // message not consumed

    // Drain the first message, freeing the whole budget, and try again.
    drop(rx.recv().await.unwrap());
    tx.try_send(vec![2u8; 1]).unwrap();
}

#[cfg(feature = "stream")]
#[tokio::test]
async fn stream_delivers_all_messages_end_to_end() {
    use futures::StreamExt;

    let (tx, mut rx) = channel::<Vec<u8>>(32, 4096);
    let producer = tokio::spawn(async move {
        for _ in 0..5 {
            tx.send(vec![0u8; 512]).await.unwrap();
        }
    });

    let mut got = 0;
    while let Some(lease) = rx.next().await {
        assert_eq!(lease.len(), 512); // Lease derefs to the Vec<u8>
        got += 1;
    }
    producer.await.unwrap();
    assert_eq!(
        got, 5,
        "stream yields every message, then ends when senders drop"
    );
}

#[cfg(feature = "stream")]
#[tokio::test]
async fn stream_lease_holds_budget_until_dropped() {
    use futures::StreamExt;

    let (tx, mut rx) = channel::<Vec<u8>>(32, 1000);
    tx.send(vec![0u8; 400]).await.unwrap();

    let lease = rx.next().await.unwrap();
    assert_eq!(lease.weight(), 400);
    // The budget stays reserved while the streamed Lease is alive, exactly like recv.
    assert_eq!(tx.available_weight(), 600);
    drop(lease);
    assert_eq!(tx.available_weight(), 1000);
}

//! Public-surface tests: test the crate exactly as a downstream user would.

use std::time::Duration;

use tokio::time::timeout;
use weighted_mpsc::{Builder, Oversized, SendError, channel};

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

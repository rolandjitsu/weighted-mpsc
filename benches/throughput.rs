//! Throughput of send + recv, weighted-mpsc against a raw tokio mpsc baseline, to
//! show what the weight accounting costs per message.

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;
use weighted_mpsc::channel;

const N: u64 = 10_000;
const MSG: usize = 1024;

fn send_recv(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("send_recv_1kib");
    group.throughput(Throughput::Elements(N));

    group.bench_function("weighted_mpsc", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = channel::<Vec<u8>>(1024, 64 * 1024 * 1024);
                let producer = tokio::spawn(async move {
                    for _ in 0..N {
                        tx.send(vec![0u8; MSG]).await.unwrap();
                    }
                });
                let mut got = 0usize;
                while let Some(m) = rx.recv().await {
                    black_box(m.len());
                    got += 1;
                }
                producer.await.unwrap();
                got
            })
        });
    });

    group.bench_function("tokio_mpsc_baseline", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1024);
                let producer = tokio::spawn(async move {
                    for _ in 0..N {
                        tx.send(vec![0u8; MSG]).await.unwrap();
                    }
                });
                let mut got = 0usize;
                while let Some(m) = rx.recv().await {
                    black_box(m.len());
                    got += 1;
                }
                producer.await.unwrap();
                got
            })
        });
    });

    group.finish();
}

criterion_group!(benches, send_recv);
criterion_main!(benches);

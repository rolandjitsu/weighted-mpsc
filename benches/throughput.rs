//! Throughput of send + recv: weighted-mpsc against a raw count-bounded tokio mpsc
//! baseline, across producer counts, to show what the weight accounting costs per
//! message. Pin cores with `taskset` on Linux (e.g. `taskset -c 0`) to reproduce
//! the per-core-count tables in BENCHMARKS.md.

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

const N: u64 = 10_000;
const MSG: usize = 1024;
const BUDGET: usize = 64 * 1024 * 1024;

// `$producers` senders each push N/$producers messages; one consumer drains.
macro_rules! bench_channel {
    ($group:expr, $rt:expr, $name:literal, $producers:expr, $make:expr) => {
        $group.bench_function($name, |b| {
            b.iter(|| {
                $rt.block_on(async {
                    let (tx, mut rx) = $make;
                    let per = N / $producers;
                    let mut handles = Vec::new();
                    for _ in 0..$producers {
                        let tx = tx.clone();
                        handles.push(tokio::spawn(async move {
                            for _ in 0..per {
                                tx.send(vec![0u8; MSG]).await.unwrap();
                            }
                        }));
                    }
                    drop(tx);
                    let mut got = 0u64;
                    while let Some(m) = rx.recv().await {
                        black_box(m.len());
                        got += 1;
                    }
                    for h in handles {
                        h.await.unwrap();
                    }
                    assert_eq!(got, per * $producers);
                });
            });
        });
    };
}

fn bench(c: &mut Criterion, label: &str, producers: u64) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group(label);
    group.throughput(Throughput::Elements(N));
    bench_channel!(
        group,
        rt,
        "weighted_mpsc",
        producers,
        weighted_mpsc::channel::<Vec<u8>>(1024, BUDGET)
    );
    bench_channel!(
        group,
        rt,
        "tokio_mpsc_baseline",
        producers,
        tokio::sync::mpsc::channel::<Vec<u8>>(1024)
    );
    group.finish();
}

fn all(c: &mut Criterion) {
    for &p in &[1u64, 4, 16, 64] {
        bench(c, &format!("send_recv_{p}_producers"), p);
    }
}

criterion_group!(benches, all);
criterion_main!(benches);

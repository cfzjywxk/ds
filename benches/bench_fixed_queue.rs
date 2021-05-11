use criterion::{criterion_group, criterion_main, BatchSize, Bencher, BenchmarkId, Criterion};
use crossbeam_queue::ArrayQueue;
use ds::FixedQueue;
use rand::Rng;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

const QUEUE_SIZE: usize = 1024 * 1024;

fn bench_fixed_queue_round(q: &FixedQueue<usize>, case: (usize, bool)) {
    if case.1 {
        // Do push else pop.
        let _ = q.push(case.0);
    } else {
        let _ = q.pop();
    }
}

fn bench_fixed_queue_func(b: &mut Bencher<'_>) {
    let queue = Arc::new(FixedQueue::new(QUEUE_SIZE));
    let mut rng = rand::thread_rng();
    let stop = Arc::new(AtomicBool::new(false));
    let s = stop.clone();

    let q = queue.clone();
    let h = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        while !s.load(Ordering::SeqCst) {
            let rnd_num: u32 = rng.gen();
            bench_fixed_queue_round(&q, (1, rnd_num % 2 == 0));
        }
    });

    b.iter_batched(
        || {
            let rnd_num: u32 = rng.gen();
            (1, rnd_num % 2 == 0)
        },
        |case| bench_fixed_queue_round(&queue, case),
        BatchSize::SmallInput,
    );

    stop.store(true, Ordering::SeqCst);
    h.join().unwrap();
}

fn bench_group_fixed_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("fixed_queue_push_pop");
    for i in 0..10 {
        group.bench_function(BenchmarkId::from_parameter(i), bench_fixed_queue_func);
    }
    group.finish();
}

fn bench_array_queue_round(q: &ArrayQueue<usize>, case: (usize, bool)) {
    if case.1 {
        // Do push else pop.
        let _ = q.push(case.0);
    } else {
        let _ = q.pop();
    }
}

fn bench_array_queue_func(b: &mut Bencher<'_>) {
    let queue = Arc::new(ArrayQueue::new(QUEUE_SIZE));
    let mut rng = rand::thread_rng();
    let stop = Arc::new(AtomicBool::new(false));
    let s = stop.clone();

    let q = queue.clone();
    let h = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        while !s.load(Ordering::SeqCst) {
            let rnd_num: u32 = rng.gen();
            bench_array_queue_round(&q, (1, rnd_num % 2 == 0));
        }
    });
    b.iter_batched(
        || {
            let rnd_num: u32 = rng.gen();
            (1, rnd_num % 2 == 0)
        },
        |case| bench_array_queue_round(&queue, case),
        BatchSize::SmallInput,
    );

    stop.store(true, Ordering::SeqCst);
    h.join().unwrap();
}

fn bench_group_array_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossbeam_array_queue_push_pop");
    for i in 0..10 {
        group.bench_function(BenchmarkId::from_parameter(i), bench_array_queue_func);
    }
    group.finish();
}

criterion_group!(benches, bench_group_fixed_queue, bench_group_array_queue);
criterion_main!(benches);

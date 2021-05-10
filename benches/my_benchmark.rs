use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ds::fibonacci;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("push", |b| b.iter(|| test(black_box(20))));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

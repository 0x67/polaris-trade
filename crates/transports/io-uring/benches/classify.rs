//! Completion classify benchmark: one pass over mixed recv completions
//! (single-shot data, multishot data, truncated, ENOBUFS). Linux only; empty
//! elsewhere.

#[cfg(target_os = "linux")]
mod linux {
    use std::hint::black_box;

    use criterion::{Criterion, Throughput};
    use transport_io_uring::completion::{Completion, classify};

    const SLOT_SIZE: u32 = 2048;
    // kernel CQE flag ABI: IORING_CQE_F_BUFFER, IORING_CQE_F_MORE, buffer id << 16
    const F_BUFFER: u32 = 1;
    const F_MORE: u32 = 1 << 1;

    pub fn classify_mix(c: &mut Criterion) {
        // (res, flags, multishot); data dominates, as on a busy feed
        let cqes: Vec<(i32, u32, bool)> = (0..256_u32)
            .map(|i| {
                let flags = (i << 16) | F_BUFFER;
                match i % 16 {
                    0 => (4000, flags, false),
                    1 => (-libc::ENOBUFS, 0, true),
                    n if n % 2 == 0 => (64 + n.cast_signed(), flags | F_MORE, true),
                    n => (1200 - n.cast_signed(), flags, false),
                }
            })
            .collect();
        let mut group = c.benchmark_group("io_uring_classify");
        group.throughput(Throughput::Elements(cqes.len() as u64));
        group.bench_function("mixed_256", |b| {
            b.iter(|| {
                let mut data = 0_u32;
                for &(res, flags, multishot) in &cqes {
                    if let Completion::Data { len, .. } =
                        classify(black_box(res), black_box(flags), SLOT_SIZE, multishot)
                    {
                        data = data.wrapping_add(len);
                    }
                }
                black_box(data)
            });
        });
        group.finish();
    }
}

#[cfg(target_os = "linux")]
criterion::criterion_group!(benches, linux::classify_mix);
#[cfg(target_os = "linux")]
criterion::criterion_main!(benches);

#[cfg(not(target_os = "linux"))]
fn main() {}

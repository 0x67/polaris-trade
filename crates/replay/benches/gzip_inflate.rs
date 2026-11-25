//! Gzip inflate + framing throughput for BinaryFileReplay, to A/B the flate2
//! backend the crate was built with.
//!
//!   cargo bench -p md-replay --bench gzip_inflate                         # miniz (default)
//!   cargo bench -p md-replay --no-default-features --features zlib-rs      # pure-Rust, faster
//!   cargo bench -p md-replay --no-default-features --features zlib-ng      # C, fastest
//!
//! Smoke-run (fast, small input): append `-- --test`.
//!
//! Reports MB/s over the UNCOMPRESSED byte count (the inflate work) plus ns per
//! framed message, so the backend's inflate speed is the moving part.

use std::{
    io::{Cursor, Write},
    time::Instant,
};

use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use md_pipeline::IngestSource;
use md_replay::BinaryFileReplay;

fn backend() -> &'static str {
    if cfg!(feature = "zlib-ng") {
        "zlib-ng"
    } else if cfg!(feature = "zlib-rs") {
        "zlib-rs"
    } else {
        "miniz_oxide"
    }
}

fn main() {
    let smoke = std::env::args().any(|a| a == "--test");
    let msg_count = if smoke { 1_000 } else { 3_000_000 };

    // Representative BinaryFILE: 2-byte-BE-len frames at a spread of ITCH-like
    // message sizes. Byte content is uniform so gzip ratio stays stable run to
    // run; the point is inflate + framing cost, not compressibility realism.
    let sizes = [12u16, 36, 31, 21, 20, 19, 40, 34];
    let mut raw = Vec::new();
    for i in 0..msg_count {
        let len = sizes[i % sizes.len()];
        raw.extend_from_slice(&len.to_be_bytes());
        raw.extend(std::iter::repeat_n(0xABu8, len as usize));
    }
    let uncompressed = raw.len();

    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(&raw).expect("gzip encode");
    let gz = enc.finish().expect("gzip finish");
    let gz_len = gz.len();

    let start = Instant::now();
    // Move gz into the Cursor: new requires a 'static reader (it boxes it).
    let mut src = BinaryFileReplay::new(GzDecoder::new(Cursor::new(gz)), 0, 1);
    let mut out = Vec::with_capacity(4096);
    let mut total = 0usize;
    loop {
        out.clear();
        match src.recv_batch(&mut out, 4096) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(_) => break,
        }
    }
    let elapsed = start.elapsed();

    assert_eq!(total, msg_count, "framed every message");

    if smoke {
        println!("gzip_inflate smoke ok [{}]: {total} msgs", backend());
        return;
    }

    let secs = elapsed.as_secs_f64();
    let mbps = (uncompressed as f64 / (1024.0 * 1024.0)) / secs;
    let ns_per_msg = elapsed.as_nanos() as f64 / total as f64;
    println!(
        "gzip_inflate [{}]: {total} msgs, {uncompressed} B uncompressed, {gz_len} B gz, {secs:.3}s, {mbps:.1} MB/s, {ns_per_msg:.1} ns/msg",
        backend(),
    );
}

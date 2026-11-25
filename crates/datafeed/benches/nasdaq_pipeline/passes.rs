//! The capture input and the two kinds of pass over it: decode alone, and the
//! session runner the binary's replay path calls.

use std::{
    fmt::Write as _,
    fs::File,
    io::Read,
    path::Path,
    sync::atomic::AtomicBool,
    time::{Duration, Instant},
};

use datafeed::{
    restart::RestartState,
    session::{self, SessionState, SessionStats},
    sink::NullSink,
    tap::NoopTap,
};
use md_pipeline::{
    DynAdapter, EgressSequencer, IngestError, IngestFrame, IngestSource, VenueAdapter, VenueId,
};
use md_replay::{BinaryFileReplay, SharedClock};
use proto_nasdaq::{
    adapter::{self as nasdaq, NasdaqAdapter},
    itch_v5,
};
use sha2::{Digest, Sha256};

use crate::{report::low14, visitor::DecodeVisitor};

/// Frames per pull, the batch the binary's live and replay runners use.
pub const BATCH: usize = 64;

/// Closes the wrapped source after `remaining` frames, so every pass replays
/// the same prefix of the capture.
struct Capped {
    inner: BinaryFileReplay,
    remaining: u64,
}

impl IngestSource for Capped {
    type Frame = <BinaryFileReplay as IngestSource>::Frame;

    fn recv_batch(&mut self, out: &mut Vec<Self::Frame>, max: usize) -> Result<usize, IngestError> {
        if self.remaining == 0 {
            return Err(IngestError::Closed);
        }
        let capped = usize::try_from(self.remaining)
            .unwrap_or(usize::MAX)
            .min(max);
        let n = self.inner.recv_batch(out, capped)?;
        self.remaining -= n as u64;
        Ok(n)
    }
}

impl Capped {
    // A read error ends a replay like end of file does. Without this check a
    // truncated or corrupt capture would shorten the input unnoticed.
    fn finish(self) {
        if let Some(err) = self.inner.io_error() {
            panic!("capture read failed mid-replay: {err}");
        }
    }
}

/// Open a NASDAQ BinaryFILE capture by extension: `.zst` streams through a zstd
/// decoder, anything else inflates as gzip. `ts_recv` counts up from zero in
/// steps of 1 ns.
fn open_capture(path: &Path, max_messages: u64) -> Capped {
    let inner = if path.extension().is_some_and(|ext| ext == "zst") {
        let file = File::open(path).expect("open NASDAQ_ITCH_SAMPLE");
        let decoder = zstd::Decoder::new(file).expect("start zstd decoder");
        BinaryFileReplay::new(decoder, 0, 1)
    } else {
        BinaryFileReplay::from_gz(path, 0, 1).expect("open NASDAQ_ITCH_SAMPLE as gzip")
    };
    Capped {
        inner,
        remaining: max_messages,
    }
}

/// Length check plus `parse_match`, what `NasdaqAdapter::feed` runs before it
/// normalizes. Each batch's decode loop is timed as a unit, which leaves
/// capture decompression out and spreads the clock read over [`BATCH`]
/// messages. Returns the summed decode time and the message count.
pub fn decode_only_pass(path: &Path, max_messages: u64) -> (Duration, u64) {
    let mut source = open_capture(path, max_messages);
    let mut visitor = DecodeVisitor;
    let mut decode = Duration::ZERO;
    let mut messages = 0u64;
    let mut frames = Vec::with_capacity(BATCH);

    loop {
        frames.clear();
        match source.recv_batch(&mut frames, BATCH) {
            Ok(_) => {}
            Err(IngestError::Closed) => break,
            Err(IngestError::PoolExhausted { .. }) => continue,
        }
        let started = Instant::now();
        for frame in &frames {
            let payload = frame.payload();
            if let Some(&msg_type) = payload.first()
                && itch_v5::BYTE_LEN[usize::from(msg_type)]
                    .is_some_and(|len| usize::from(len.get()) == payload.len())
            {
                itch_v5::parse_match(msg_type, payload, &mut visitor);
            }
        }
        decode += started.elapsed();
        messages += frames.len() as u64;
    }

    source.finish();
    (decode, messages)
}

/// One session over the capture: `session::run` on a boxed `dyn DynAdapter`
/// with `NullSink` and `NoopTap`. Whether the stage timers record depends on
/// the metrics gate when the call starts. Returns the session counters, the
/// wall time of the run and the low 14 bits of the boxed adapter's address.
pub fn session_pass(path: &Path, max_messages: u64) -> (SessionStats, Duration, usize) {
    let source = open_capture(path, max_messages);
    // boxed and erased like the adapter a `VenueDescriptor::make` returns
    let mut adapter: Box<dyn DynAdapter> = Box::new(NasdaqAdapter::new(nasdaq::Config {
        venue: VenueId(2),
        session_midnight_ns: 0,
    }));
    let adapter_low14 = low14(&*adapter);
    let mut sequence = EgressSequencer::new(NullSink, SharedClock::new());
    let mut state = SessionState::default();

    let started = Instant::now();
    let (stats, source) = session::run(
        source,
        &mut *adapter,
        &mut sequence,
        &mut NoopTap,
        &mut state,
        &mut RestartState::default(),
        session::RunControl {
            batch: BATCH,
            stop: &AtomicBool::new(false),
        },
    )
    .expect("NullSink and NoopTap cannot fail a session");
    let wall = started.elapsed();

    source.finish();
    assert_eq!(
        adapter.stats().decoded,
        stats.frames,
        "the adapter dropped frames, so the pass did not time the whole input"
    );
    (stats, wall, adapter_low14)
}

/// Lowercase hex sha256 of the file at `path`.
pub fn sha256_file(path: &Path) -> String {
    let mut file = File::open(path).expect("open capture for hashing");
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = file.read(&mut buf).expect("read capture for hashing");
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    // sha2 0.11 returns a `hybrid_array::Array`, which has no `LowerHex`
    hasher
        .finalize()
        .iter()
        .fold(String::new(), |mut hex, byte| {
            write!(hex, "{byte:02x}").expect("writing to a String cannot fail");
            hex
        })
}

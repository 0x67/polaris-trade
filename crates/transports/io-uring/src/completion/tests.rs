use super::{Completion, classify};

// kernel CQE flag ABI: IORING_CQE_F_BUFFER, IORING_CQE_F_MORE, IORING_CQE_BUFFER_SHIFT
const F_BUFFER: u32 = 1;
const F_MORE: u32 = 1 << 1;
const SLOT: u32 = 2048;

fn with_buffer(bid: u16) -> u32 {
    (u32::from(bid) << 16) | F_BUFFER
}

#[test]
fn classify_maps_every_completion_shape() {
    let cases = [
        (
            "single-shot data ends request",
            (100, with_buffer(7), false),
            Completion::Data {
                slot: 7,
                len: 100,
                rearm: true,
            },
        ),
        (
            "multishot data with F_MORE keeps request",
            (100, with_buffer(9) | F_MORE, true),
            Completion::Data {
                slot: 9,
                len: 100,
                rearm: false,
            },
        ),
        (
            "multishot success without F_MORE ends request",
            (64, with_buffer(3), true),
            Completion::Data {
                slot: 3,
                len: 64,
                rearm: true,
            },
        ),
        (
            "empty datagram before 6.0 still holds slot",
            (0, with_buffer(5), false),
            Completion::Data {
                slot: 5,
                len: 0,
                rearm: true,
            },
        ),
        (
            "empty datagram from 6.0 has no buffer id",
            (0, 0, false),
            Completion::Empty { rearm: true },
        ),
        (
            "empty datagram ends multishot",
            (0, 0, true),
            Completion::Empty { rearm: true },
        ),
        (
            "datagram exactly slot size is whole",
            (2048, with_buffer(1), false),
            Completion::Data {
                slot: 1,
                len: 2048,
                rearm: true,
            },
        ),
        (
            "longer than slot is truncated",
            (2049, with_buffer(2) | F_MORE, true),
            Completion::Truncated {
                slot: 2,
                rearm: false,
            },
        ),
        ("ENOBUFS", (-libc::ENOBUFS, 0, true), Completion::NoBuffers),
        (
            "other errno",
            (-libc::ECONNREFUSED, 0, false),
            Completion::Failed(libc::ECONNREFUSED),
        ),
        (
            "success without buffer id",
            (10, 0, false),
            Completion::Failed(libc::EIO),
        ),
    ];
    for (case, (res, flags, multishot), want) in cases {
        assert_eq!(classify(res, flags, SLOT, multishot), want, "{case}");
    }
}

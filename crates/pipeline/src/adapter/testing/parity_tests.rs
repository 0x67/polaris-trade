//! The fixture-parity gate against the stub adapter: a clean stub reproduces
//! the recorded output, and an altered mapping fails naming the record.

use std::path::{Path, PathBuf};

use super::{
    assert_fixture_parity,
    parity::{
        ParityError, check_fixture_parity, first_difference, parse_frames, rewrite_requested,
    },
    tests::{Stub, defect},
};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/stub-parity")
}

#[test]
fn clean_stub_matches_recorded_output() {
    assert_fixture_parity::<Stub<{ defect::CLEAN }>>((), &fixture_dir());
}

/// The stub stamps its own count instead of the wire `venue_seq`; frame 2's
/// second trade is the first record where the two differ.
#[test]
fn altered_mapping_names_the_differing_record() {
    let err = check_fixture_parity::<Stub<{ defect::OWN_VENUE_SEQ }>>((), &fixture_dir())
        .expect_err("mapping change must fail the gate");
    let ParityError::Mismatch {
        line,
        expected,
        actual,
        ..
    } = &err
    else {
        panic!("want a mismatch, got {err}");
    };
    assert_eq!(*line, 3);
    assert!(expected.starts_with("frame 2 Trade("), "{expected}");
    assert!(expected.contains("venue_seq: 2"), "{expected}");
    assert!(actual.contains("venue_seq: 3"), "{actual}");
    assert!(err.to_string().contains("POLARIS_REWRITE_EXPECTED=1"));
}

#[test]
fn counter_change_fails_at_the_stats_line() {
    let err = check_fixture_parity::<Stub<{ defect::DECODED_PER_EVENT }>>((), &fixture_dir())
        .expect_err("counter change must fail the gate");
    assert!(err.to_string().contains("stats AdapterStats"), "{err}");
}

#[test]
fn output_shorter_than_expected_reports_end_of_output() {
    let err = first_difference(Path::new("expected.txt"), "a\nb\n", "a\n")
        .expect_err("missing line must differ");
    assert!(
        err.to_string().contains("actual:   <end of output>"),
        "{err}"
    );
}

#[test]
fn malformed_frame_line_names_its_line() {
    let err = parse_frames(Path::new("frames.txt"), "# c\n1 10 1 4g\n").unwrap_err();
    assert_eq!(err.to_string(), "frames.txt line 2: payload is not hex");
}

#[test]
fn signed_hex_digit_is_not_hex() {
    let err = parse_frames(Path::new("frames.txt"), "1 10 1 +f\n").unwrap_err();
    assert_eq!(err.to_string(), "frames.txt line 1: payload is not hex");
}

#[test]
fn only_one_turns_rewrite_on() {
    use std::ffi::OsStr;
    assert!(rewrite_requested(Some(OsStr::new("1"))));
    for off in [
        None,
        Some(OsStr::new("0")),
        Some(OsStr::new("")),
        Some(OsStr::new("false")),
    ] {
        assert!(!rewrite_requested(off), "{off:?}");
    }
}

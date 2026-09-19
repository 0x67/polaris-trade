//! `GapRequestHandler`: recording, coalescing and clearing missing ranges.

use client_moldudp::{GapRequest, GapRequestHandler};

#[test]
fn single_missing_range_reported_as_one_request() {
    let mut handler = GapRequestHandler::new();
    handler.record_missing_range(10, 15);
    assert_eq!(
        handler.pending_gaps(),
        vec![GapRequest {
            start_seq: 10,
            count: 5
        }]
    );
}

#[test]
fn mark_received_at_edge_shrinks_range() {
    let mut handler = GapRequestHandler::new();
    handler.record_missing_range(10, 15); // missing 10..15
    handler.mark_received(10);
    assert_eq!(
        handler.pending_gaps(),
        vec![GapRequest {
            start_seq: 11,
            count: 4
        }]
    );
}

#[test]
fn mark_received_in_middle_splits_range() {
    let mut handler = GapRequestHandler::new();
    handler.record_missing_range(10, 15); // missing 10,11,12,13,14
    handler.mark_received(12);
    let gaps = handler.pending_gaps();
    assert_eq!(
        gaps,
        vec![
            GapRequest {
                start_seq: 10,
                count: 2
            },
            GapRequest {
                start_seq: 13,
                count: 2
            },
        ]
    );
}

#[test]
fn mark_received_clears_fully_filled_range() {
    let mut handler = GapRequestHandler::new();
    handler.record_gap(7);
    handler.mark_received(7);
    assert!(handler.pending_gaps().is_empty());
}

#[test]
fn mark_received_outside_any_range_is_a_no_op() {
    let mut handler = GapRequestHandler::new();
    handler.record_missing_range(10, 15);
    handler.mark_received(100); // unrelated seq leaves state alone
    assert_eq!(
        handler.pending_gaps(),
        vec![GapRequest {
            start_seq: 10,
            count: 5
        }]
    );
}

//! Construction guards: leg pool smaller than reorder window plus burst, or leg
//! count outside `1..=255`, fails `from_legs` instead of stalling live.

pub mod support;

use client_moldudp::{MIN_LEG_POOL_CAPACITY, MoldUdpError, MoldUdpReceiver, MoldUdpReceiverConfig};
use smallvec::{SmallVec, smallvec};
use support::MockLeg;

#[test]
fn undersized_leg_pool_fails_construction() {
    let short = u32::try_from(MIN_LEG_POOL_CAPACITY - 1).expect("fits u32");
    let legs: SmallVec<[MockLeg; 2]> =
        smallvec![support::mock_leg(), support::mock_leg_with(short)];
    match MoldUdpReceiver::from_legs(&MoldUdpReceiverConfig::default(), legs) {
        Err(MoldUdpError::PoolTooSmall {
            leg,
            capacity,
            required,
        }) => {
            assert_eq!(leg, 1);
            assert_eq!(capacity, MIN_LEG_POOL_CAPACITY - 1);
            assert_eq!(required, MIN_LEG_POOL_CAPACITY);
        }
        Err(other) => panic!("expected PoolTooSmall, got {other:?}"),
        Ok(_) => panic!("undersized pool must fail construction"),
    }
}

#[test]
fn zero_legs_fail_construction() {
    let legs: SmallVec<[MockLeg; 2]> = SmallVec::new();
    assert!(matches!(
        MoldUdpReceiver::from_legs(&MoldUdpReceiverConfig::default(), legs),
        Err(MoldUdpError::LegCount { count: 0 })
    ));
}

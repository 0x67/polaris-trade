// each integration test binary recompiles this support tree; a binary that
// uses only part of it leaves the rest unused, so allow dead_code at the
// module boundary rather than per item.
#[allow(dead_code)]
pub mod capture_tree;
#[allow(dead_code)]
pub mod digest;
#[allow(dead_code)]
pub mod itch;
#[allow(dead_code)]
pub mod record;
#[allow(dead_code)]
pub mod reference_parity;
#[allow(dead_code)]
pub mod soupbin;

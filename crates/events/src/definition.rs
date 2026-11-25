//! Instrument-definition constructor logic layered on the generated `Definition`.
//!
//! The `Definition` struct and `AssetClass` enum are generated into `crate::generated`;
//! `Definition::new` (header defaults plus the zeroed `status_flags`/`call_auction`
//! tail) and the read of a definition written before `tail` are hand-written. The
//! registry that mints ids and resolves aliases lives in md-pipeline; this is just
//! the contract record consumers receive.

use core::mem::{MaybeUninit, offset_of};

use crate::{
    flags::RecordFlags,
    generated::{AssetClass, Definition, DefinitionTail, OptionType, RecordHeader, rtype},
    price::Price,
};

impl DefinitionTail {
    /// Every term absent: null prices, zero elsewhere.
    pub const ABSENT: Self = DefinitionTail {
        tick_size: Price::NULL.raw(),
        contract_multiplier: 0,
        min_qty: 0,
        expiration_ns: 0,
        strike: Price::NULL.raw(),
        underlying_id: 0,
        base_ccy: [0; 8],
        quote_ccy: [0; 8],
        settle_ccy: [0; 8],
        price_scale: 0,
        option_type: OptionType::None,
        _pad: [0; 2],
    };
}

impl Definition {
    /// On-wire length of a definition written before `tail` existed: 104 bytes,
    /// 26 words. [`decode_prefix`](crate::decode_prefix) still reads one.
    pub const PRE_TAIL_LEN: usize = offset_of!(Definition, tail);

    /// Read a definition written before `tail`: its 104 bytes as written, `tail`
    /// set to [`DefinitionTail::ABSENT`], and `hdr.length` set to this type's 44
    /// words, so the value writes back as one whole frame. `None` unless `image`
    /// is exactly [`Self::PRE_TAIL_LEN`] bytes.
    ///
    /// Same precondition as [`decode_prefix`](crate::decode_prefix): the bytes
    /// come from our own writer, so `asset_class` holds a valid discriminant.
    pub(crate) fn decode_pre_tail(image: &[u8]) -> Option<Self> {
        if image.len() != Self::PRE_TAIL_LEN {
            return None;
        }
        let mut val = MaybeUninit::<Self>::uninit();
        let ptr = val.as_mut_ptr();
        // SAFETY: the copy initializes every byte before `tail` from a definition
        // our writer produced and the write initializes `tail`, so the whole value
        // is initialized at `assume_init`. No reference to uninit memory is made.
        let mut def = unsafe {
            core::ptr::copy_nonoverlapping(image.as_ptr(), ptr.cast::<u8>(), Self::PRE_TAIL_LEN);
            (&raw mut (*ptr).tail).write(DefinitionTail::ABSENT);
            val.assume_init()
        };
        def.hdr.length = (size_of::<Self>() / 4) as u8;
        Some(def)
    }

    /// Construct a definition record for a minted id at an effective time.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        instrument_id: u32,
        venue: u16,
        ts_event: u64,
        venue_native_id: u64,
        raw_symbol: [u8; 20],
        board: [u8; 4],
        asset_class: AssetClass,
        qty_scale: i8,
        lot_size: u32,
    ) -> Self {
        let hdr = RecordHeader {
            length: (core::mem::size_of::<Self>() / 4) as u8,
            rtype: rtype::DEFINITION,
            venue,
            instrument_id,
            ts_event,
            ts_recv: ts_event,
            ts_egress: 0,
            venue_seq: 0,
            egress_seq: 0,
            flags: RecordFlags::NONE,
            _pad: [0u8; 6],
        };
        Definition {
            hdr,
            venue_native_id,
            lot_size,
            raw_symbol,
            board,
            asset_class,
            qty_scale,
            // zeroed here; adapters set these fields directly on the returned record.
            status_flags: [0u8; 8],
            call_auction: 0,
            _pad: [0u8; 1],
            tail: DefinitionTail::ABSENT,
        }
    }
}

#[cfg(test)]
mod tests {
    use core::mem::size_of;

    use crate::{AssetClass, Definition, Record, rtype};

    // Definition is a whole number of 32-bit words (skip-tail relies on the
    // header word length) and carries the reference-band rtype.
    #[test]
    fn definition_layout() {
        assert_eq!(size_of::<Definition>() % 4, 0);
        assert_eq!(Definition::RTYPE, rtype::DEFINITION);
        assert_eq!(AssetClass::default(), AssetClass::Equity);
    }
}

//! Built-in redirect program: six eBPF instructions, loaded with no toolchain.
//!
//! `bpf_redirect_map(xskmap, ctx->rx_queue_index, XDP_PASS)`: frame of queue
//! with socket in map goes to that socket, any other frame to kernel stack
//! (lookup-miss fallback, kernel 5.3 or later). Verified to load, JIT and
//! redirect on kernel 7.0 with license below (helper is not GPL-only).

use std::{
    ffi::CStr,
    io,
    os::fd::{AsRawFd, BorrowedFd, OwnedFd},
};

use transport_core::TransportError;

use super::sys::{self, Attr, PROG_TYPE_XDP, ProgLoad};
use crate::BACKEND;

// opcodes, uapi `linux/bpf.h` and `linux/bpf_common.h`
const LDX_W_MEM: u8 = 0x61; // BPF_LDX | BPF_W | BPF_MEM
const LD_DW_IMM: u8 = 0x18; // BPF_LD | BPF_DW | BPF_IMM
const MOV64_K: u8 = 0xb7; // BPF_ALU64 | BPF_MOV | BPF_K
const CALL: u8 = 0x85; // BPF_JMP | BPF_CALL
const EXIT: u8 = 0x95; // BPF_JMP | BPF_EXIT
// `ld_imm64` source register marking immediate as map fd
const PSEUDO_MAP_FD: u8 = 1;
// offset of `rx_queue_index` in `struct xdp_md`
const RX_QUEUE_INDEX: i16 = 16;
const XDP_PASS: i32 = 2;
const FN_REDIRECT_MAP: i32 = 51;

const LICENSE: &CStr = c"Dual MIT/Apache-2.0";
const NAME: [u8; 16] = *b"polaris_xsk\0\0\0\0\0";
const INSNS: u32 = 6;
// verifier log buffer on reload after failure
const LOG_SIZE: u32 = 64 * 1024;

// `struct bpf_insn` bytes: registers share one byte, `dst` in low nibble on
// little-endian hosts, `off` and `imm` little-endian
const fn insn(code: u8, dst: u8, src: u8, off: i16, imm: i32) -> [u8; 8] {
    let off = off.to_le_bytes();
    let imm = imm.to_le_bytes();
    [
        code,
        (src << 4) | dst,
        off[0],
        off[1],
        imm[0],
        imm[1],
        imm[2],
        imm[3],
    ]
}

/// Program redirecting to XSKMAP `map_fd`.
pub(crate) const fn instructions(map_fd: i32) -> [[u8; 8]; INSNS as usize] {
    [
        insn(LDX_W_MEM, 2, 1, RX_QUEUE_INDEX, 0), // r2 = ctx->rx_queue_index
        insn(LD_DW_IMM, 1, PSEUDO_MAP_FD, 0, map_fd), // r1 = map, first half
        insn(0, 0, 0, 0, 0),                      // second half of ld_imm64
        insn(MOV64_K, 3, 0, 0, XDP_PASS),         // r3 = action on lookup miss
        insn(CALL, 0, 0, 0, FN_REDIRECT_MAP),     // r0 = bpf_redirect_map(r1, r2, r3)
        insn(EXIT, 0, 0, 0, 0),
    ]
}

/// Load program over XSKMAP `map`. Loads with no log; on failure reloads with
/// verifier log and emits it once at `error` level.
pub(crate) fn load(map: BorrowedFd<'_>) -> Result<OwnedFd, TransportError> {
    // register nibble order and immediates above hold for little-endian only
    if cfg!(target_endian = "big") {
        return Err(TransportError::Unsupported {
            backend: BACKEND,
            op: "built-in XDP program on big-endian host",
        });
    }
    let insns = instructions(map.as_raw_fd());
    let first = match prog_load(&insns, None) {
        Ok(fd) => return Ok(fd),
        Err(error) if error.raw_os_error() == Some(libc::EPERM) => {
            return Err(sys::error(ProgLoad::STAGE, error));
        }
        Err(error) => error,
    };
    let mut log = vec![0; LOG_SIZE as usize];
    prog_load(&insns, Some(&mut log)).map_err(|_| {
        let end = log.iter().position(|&b| b == 0).unwrap_or(log.len());
        let verifier_log = String::from_utf8_lossy(&log[..end]);
        tracing::error!(backend = BACKEND, %first, %verifier_log, "XDP program load failed");
        sys::error(ProgLoad::STAGE, first)
    })
}

fn prog_load(insns: &[[u8; 8]; INSNS as usize], log: Option<&mut [u8]>) -> io::Result<OwnedFd> {
    // no log: level, size and buffer all zero, as kernel requires
    let (log_level, log_size, log_buf) = match log {
        Some(log) => {
            let size = u32::try_from(log.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
            (1, size, sys::addr(log.as_mut_ptr()))
        }
        None => (0, 0, 0),
    };
    let mut attr = ProgLoad {
        prog_type: PROG_TYPE_XDP,
        insn_cnt: INSNS,
        insns: sys::addr(insns.as_ptr()),
        license: sys::addr(LICENSE.as_ptr()),
        log_level,
        log_size,
        log_buf,
        prog_name: NAME,
        ..ProgLoad::default()
    };
    // SAFETY: `insns` holds `insn_cnt` instructions, `LICENSE` NUL-terminated
    // and `log` (when set) holds `log_size` writable bytes, all live for call;
    // `PROG_LOAD` returns new program fd
    unsafe { sys::bpf_fd(&mut attr) }
}

#[cfg(test)]
mod tests {
    use super::instructions;

    #[test]
    fn instructions_match_verified_redirect_program() {
        // bytes loaded, verified and run on kernel 7.0 (aarch64 and x86_64 share encoding)
        let expected: [[u8; 8]; 6] = [
            [0x61, 0x12, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00],
            [0x18, 0x11, 0x00, 0x00, 0x2a, 0x01, 0x00, 0x00],
            [0x00; 8],
            [0xb7, 0x03, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00],
            [0x85, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00],
            [0x95, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        ];
        assert_eq!(
            instructions(0x12a),
            expected,
            "map fd 0x12a lands little-endian in imm"
        );
    }
}

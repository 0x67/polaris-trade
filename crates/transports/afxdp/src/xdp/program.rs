//! Built-in redirect program: IPv4 UDP filter in front of
//! `bpf_redirect_map`, raw eBPF loaded with no toolchain.
//!
//! IPv4 UDP frame, untagged or under one 802.1Q tag (frames
//! [`UdpDecap`](transport_core::decap::UdpDecap) accepts), goes to
//! `bpf_redirect_map(xskmap, ctx->rx_queue_index, XDP_PASS)`: socket in map
//! gets it, lookup miss sends it to kernel stack (kernel 5.3 or later). Any
//! other frame (ARP, IGMP, ICMP, TCP, IPv6, stacked tags, frame shorter than
//! Ethernet plus 20-byte IPv4 plus UDP header) returns `XDP_PASS`, so kernel
//! keeps answering ARP and IGMP queries on bound queue. Unrelated UDP on
//! queue still reaches socket. Verified to load, JIT and redirect on kernel
//! 7.0 in SKB and DRV modes with license below (helper is not GPL-only).

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
const LDX_H_MEM: u8 = 0x69; // BPF_LDX | BPF_H | BPF_MEM
const LDX_B_MEM: u8 = 0x71; // BPF_LDX | BPF_B | BPF_MEM
const LD_DW_IMM: u8 = 0x18; // BPF_LD | BPF_DW | BPF_IMM
const MOV64_K: u8 = 0xb7; // BPF_ALU64 | BPF_MOV | BPF_K
const MOV64_X: u8 = 0xbf; // BPF_ALU64 | BPF_MOV | BPF_X
const ADD64_K: u8 = 0x07; // BPF_ALU64 | BPF_ADD | BPF_K
const JA: u8 = 0x05; // BPF_JMP | BPF_JA
const JEQ_K: u8 = 0x15; // BPF_JMP | BPF_JEQ | BPF_K
const JGT_X: u8 = 0x2d; // BPF_JMP | BPF_JGT | BPF_X
const JNE_K: u8 = 0x55; // BPF_JMP | BPF_JNE | BPF_K
const CALL: u8 = 0x85; // BPF_JMP | BPF_CALL
const EXIT: u8 = 0x95; // BPF_JMP | BPF_EXIT
// `ld_imm64` source register marking immediate as map fd
const PSEUDO_MAP_FD: u8 = 1;
// `struct xdp_md` field offsets
const MD_DATA: i16 = 0;
const MD_DATA_END: i16 = 4;
const MD_RX_QUEUE_INDEX: i16 = 16;
const XDP_PASS: i32 = 2;
const FN_REDIRECT_MAP: i32 = 51;

// untagged frame offsets; 802.1Q tag shifts all by `VLAN_TAG`
const ETH_TYPE: i16 = 12;
const IP_PROTO: i16 = 14 + 9;
const VLAN_TAG: i16 = 4;
// Ethernet, 20-byte IPv4 and UDP headers: shorter frame passes
const HEADERS: i32 = 14 + 20 + 8;
// halfword load reads network-order bytes little-endian
const ETH_P_IP: i32 = 0x0800_u16.swap_bytes() as i32;
const ETH_P_8021Q: i32 = 0x8100_u16.swap_bytes() as i32;
const IPPROTO_UDP: i32 = 17;

// jump target indices in `instructions`
const UNTAGGED_PROTO: i16 = 15;
const UDP_CHECK: i16 = 16;
const PASS: i16 = 23;

const LICENSE: &CStr = c"Dual MIT/Apache-2.0";
const NAME: [u8; 16] = *b"polaris_xsk\0\0\0\0\0";
const INSNS: u32 = 25;
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

// jump offset from instruction `at` to `target`, counted from next instruction
const fn jump(at: i16, target: i16) -> i16 {
    target - at - 1
}

/// Program redirecting IPv4 UDP frames to XSKMAP `map_fd`, passing every other
/// frame to kernel stack. Each packet load follows `data + N > data_end` check
/// at constant offset, as verifier requires.
pub(crate) const fn instructions(map_fd: i32) -> [[u8; 8]; INSNS as usize] {
    [
        // 0: r2 = ctx->data, r3 = ctx->data_end
        insn(LDX_W_MEM, 2, 1, MD_DATA, 0),
        insn(LDX_W_MEM, 3, 1, MD_DATA_END, 0),
        // 2: untagged headers in frame, else pass
        insn(MOV64_X, 4, 2, 0, 0),
        insn(ADD64_K, 4, 0, 0, HEADERS),
        insn(JGT_X, 4, 3, jump(4, PASS), 0),
        // 5: IPv4 to untagged protocol, 802.1Q on to tagged checks, rest pass
        insn(LDX_H_MEM, 4, 2, ETH_TYPE, 0),
        insn(JEQ_K, 4, 0, jump(6, UNTAGGED_PROTO), ETH_P_IP),
        insn(JNE_K, 4, 0, jump(7, PASS), ETH_P_8021Q),
        // 8: tagged headers in frame and inner ethertype IPv4, else pass
        insn(MOV64_X, 4, 2, 0, 0),
        insn(ADD64_K, 4, 0, 0, HEADERS + VLAN_TAG as i32),
        insn(JGT_X, 4, 3, jump(10, PASS), 0),
        insn(LDX_H_MEM, 4, 2, ETH_TYPE + VLAN_TAG, 0),
        insn(JNE_K, 4, 0, jump(12, PASS), ETH_P_IP),
        insn(LDX_B_MEM, 4, 2, IP_PROTO + VLAN_TAG, 0),
        insn(JA, 0, 0, jump(14, UDP_CHECK), 0),
        // 15: untagged protocol byte
        insn(LDX_B_MEM, 4, 2, IP_PROTO, 0),
        // 16: UDP, else pass
        insn(JNE_K, 4, 0, jump(16, PASS), IPPROTO_UDP),
        // 17: r0 = bpf_redirect_map(map, ctx->rx_queue_index, XDP_PASS)
        insn(LDX_W_MEM, 2, 1, MD_RX_QUEUE_INDEX, 0),
        insn(LD_DW_IMM, 1, PSEUDO_MAP_FD, 0, map_fd),
        insn(0, 0, 0, 0, 0),              // second half of ld_imm64
        insn(MOV64_K, 3, 0, 0, XDP_PASS), // action on lookup miss
        insn(CALL, 0, 0, 0, FN_REDIRECT_MAP),
        insn(EXIT, 0, 0, 0, 0),
        // 23: pass
        insn(MOV64_K, 0, 0, 0, XDP_PASS),
        insn(EXIT, 0, 0, 0, 0),
    ]
}

/// Load program over XSKMAP `map`. Loads with no log; on failure reloads with
/// verifier log and emits it once at `error` level.
pub(crate) fn load(map: BorrowedFd<'_>) -> Result<OwnedFd, TransportError> {
    // register nibble order, immediates and ethertype compares hold for little-endian only
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
mod tests;

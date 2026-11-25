use super::{
    ADD64_K, CALL, EXIT, FN_REDIRECT_MAP, INSNS, JA, JEQ_K, JGT_X, JNE_K, LD_DW_IMM, LDX_B_MEM,
    LDX_H_MEM, LDX_W_MEM, MOV64_K, MOV64_X, instructions,
};

const MAP_FD: i32 = 0x12a;
const QUEUE: u32 = 3;
// disjoint address ranges tell ctx loads from packet loads
const CTX: u64 = 1 << 40;
const PKT: u64 = 1 << 32;
const XDP_PASS: u64 = 2;
const XDP_REDIRECT: u64 = 4;
const IPV4: u16 = 0x0800;
const VLAN: u16 = 0x8100;
const UDP: u8 = 17;

#[test]
fn instructions_match_verified_filter_program() {
    // bytes loaded, verified and run on kernel 7.0 (aarch64 and x86_64 share encoding)
    let expected: [[u8; 8]; INSNS as usize] = [
        [0x61, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        [0x61, 0x13, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00],
        [0xbf, 0x24, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        [0x07, 0x04, 0x00, 0x00, 0x2a, 0x00, 0x00, 0x00],
        [0x2d, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00],
        [0x69, 0x24, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00],
        [0x15, 0x04, 0x08, 0x00, 0x08, 0x00, 0x00, 0x00],
        [0x55, 0x04, 0x0f, 0x00, 0x81, 0x00, 0x00, 0x00],
        [0xbf, 0x24, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        [0x07, 0x04, 0x00, 0x00, 0x2e, 0x00, 0x00, 0x00],
        [0x2d, 0x34, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00],
        [0x69, 0x24, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00],
        [0x55, 0x04, 0x0a, 0x00, 0x08, 0x00, 0x00, 0x00],
        [0x71, 0x24, 0x1b, 0x00, 0x00, 0x00, 0x00, 0x00],
        [0x05, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00],
        [0x71, 0x24, 0x17, 0x00, 0x00, 0x00, 0x00, 0x00],
        [0x55, 0x04, 0x06, 0x00, 0x11, 0x00, 0x00, 0x00],
        [0x61, 0x12, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00],
        [0x18, 0x11, 0x00, 0x00, 0x2a, 0x01, 0x00, 0x00],
        [0x00; 8],
        [0xb7, 0x03, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00],
        [0x85, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00],
        [0x95, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        [0xb7, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00],
        [0x95, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
    ];
    assert_eq!(
        instructions(MAP_FD),
        expected,
        "program bytes differ from verified ones (map fd 0x12a little-endian in imm)"
    );
}

// `ctx` load: `data`, `data_end` and `rx_queue_index` only, word-sized
fn ctx_load(frame: &[u8], off: u64, size: usize) -> u64 {
    assert_eq!(size, 4, "ctx field {off} loaded with {size} bytes");
    match off {
        0 => PKT,
        4 => PKT + frame.len() as u64,
        16 => u64::from(QUEUE),
        _ => panic!("ctx load at {off}"),
    }
}

// little-endian packet load; panics past frame end, as verifier would reject
fn pkt_load(frame: &[u8], addr: u64, size: usize) -> u64 {
    let at = usize::try_from(addr - PKT).expect("packet offset fits usize");
    let bytes = frame
        .get(at..at + size)
        .unwrap_or_else(|| panic!("{size}-byte load at {at} past {}-byte frame", frame.len()));
    bytes.iter().rev().fold(0, |v, &b| (v << 8) | u64::from(b))
}

// runs program on `frame` for queue `QUEUE`, returns action; panics on load
// outside ctx or frame, jump outside program or unknown opcode
fn run(frame: &[u8]) -> u64 {
    let prog = instructions(MAP_FD);
    let mut r = [0_u64; 11];
    r[1] = CTX;
    let mut pc = 0;
    // forward jumps only: every step advances `pc`
    for _ in 0..INSNS {
        let [code, regs, o0, o1, i0, i1, i2, i3] = prog[pc];
        let (dst, src) = (usize::from(regs & 0xf), usize::from(regs >> 4));
        let off = i16::from_le_bytes([o0, o1]);
        let imm = i64::from(i32::from_le_bytes([i0, i1, i2, i3])).cast_unsigned();
        let target = || pc + 1 + usize::try_from(off).expect("forward jump");
        let mut next = pc + 1;
        match code {
            LDX_W_MEM | LDX_H_MEM | LDX_B_MEM => {
                let size = match code {
                    LDX_W_MEM => 4,
                    LDX_H_MEM => 2,
                    _ => 1,
                };
                let addr = r[src].wrapping_add_signed(off.into());
                r[dst] = if addr >= CTX {
                    ctx_load(frame, addr - CTX, size)
                } else {
                    pkt_load(frame, addr, size)
                };
            }
            LD_DW_IMM => {
                r[dst] = imm;
                next = pc + 2;
            }
            MOV64_K => r[dst] = imm,
            MOV64_X => r[dst] = r[src],
            ADD64_K => r[dst] = r[dst].wrapping_add(imm),
            JA => next = target(),
            JEQ_K if r[dst] == imm => next = target(),
            JNE_K if r[dst] != imm => next = target(),
            JGT_X if r[dst] > r[src] => next = target(),
            JEQ_K | JNE_K | JGT_X => {}
            CALL => {
                assert_eq!(imm, u64::from(FN_REDIRECT_MAP.cast_unsigned()), "helper");
                assert_eq!(
                    [r[1], r[2], r[3]],
                    [
                        u64::from(MAP_FD.cast_unsigned()),
                        u64::from(QUEUE),
                        XDP_PASS
                    ],
                    "bpf_redirect_map(map, rx_queue_index, XDP_PASS)"
                );
                r[0] = XDP_REDIRECT;
            }
            EXIT => return r[0],
            _ => panic!("opcode {code:#04x} at {pc}"),
        }
        pc = next;
    }
    panic!("no exit within {INSNS} steps")
}

// MACs, one TPID plus zero TCI per `tags` entry, `ethertype`, 20-byte IPv4
// header with `proto`, UDP header, 4 payload bytes
fn frame(tags: &[u16], ethertype: u16, proto: u8) -> Vec<u8> {
    let mut f = vec![0xaa; 12];
    for tpid in tags {
        f.extend(tpid.to_be_bytes());
        f.extend([0, 0]);
    }
    f.extend(ethertype.to_be_bytes());
    let mut ip = [0; 20];
    ip[0] = 0x45;
    ip[9] = proto;
    f.extend(ip);
    f.extend([0; 12]);
    f
}

#[test]
fn filter_redirects_ipv4_udp_untagged_or_under_one_tag() {
    let untagged = frame(&[], IPV4, UDP);
    let tagged = frame(&[VLAN], IPV4, UDP);
    for (case, f) in [
        ("untagged", &untagged[..]),
        ("802.1Q", &tagged[..]),
        ("untagged, headers only", &untagged[..42]),
        ("802.1Q, headers only", &tagged[..46]),
    ] {
        assert_eq!(run(f), XDP_REDIRECT, "{case} IPv4 UDP");
    }
}

#[test]
fn filter_passes_everything_else() {
    // non-IPv4 frames carry UDP at protocol offset: ethertype alone must decide
    for (case, f) in [
        ("ARP", frame(&[], 0x0806, UDP)),
        ("IPv6", frame(&[], 0x86dd, UDP)),
        ("IGMP", frame(&[], IPV4, 2)),
        ("ICMP", frame(&[], IPV4, 1)),
        ("TCP", frame(&[], IPV4, 6)),
        ("802.1Q TCP", frame(&[VLAN], IPV4, 6)),
        ("802.1Q ARP", frame(&[VLAN], 0x0806, UDP)),
        ("stacked 802.1Q", frame(&[VLAN, VLAN], IPV4, UDP)),
        ("802.1ad", frame(&[0x88a8], IPV4, UDP)),
        ("short untagged UDP", frame(&[], IPV4, UDP)[..41].to_vec()),
        ("short 802.1Q UDP", frame(&[VLAN], IPV4, UDP)[..45].to_vec()),
        ("runt", vec![0xaa; 13]),
        ("empty", Vec::new()),
    ] {
        assert_eq!(run(&f), XDP_PASS, "{case}");
    }
}

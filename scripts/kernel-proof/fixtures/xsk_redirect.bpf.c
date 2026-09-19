/*
 * Stand-in for operator's external XDP program: pinned-mode proof loads it with
 * bpftool (maps pinned by name, so `xsks_map` lands in pinmaps dir) and attaches
 * it; `transport_afxdp` then inserts its socket into `xsks_map`.
 * Build: clang -O2 -g -target bpf -I/usr/include/$(uname -m)-linux-gnu -c
 * (`-g` emits BTF, which libbpf needs for `.maps` definitions).
 */

#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>

/* key queue index, value AF_XDP socket fd; 4 bytes each, as `open_pinned` checks */
struct {
	__uint(type, BPF_MAP_TYPE_XSKMAP);
	__uint(max_entries, 64);
	__type(key, __u32);
	__type(value, __u32);
} xsks_map SEC(".maps");

/* queue with socket in map: frame to socket; otherwise kernel stack */
SEC("xdp")
int xsk_redirect(struct xdp_md *ctx)
{
	return bpf_redirect_map(&xsks_map, ctx->rx_queue_index, XDP_PASS);
}

char LICENSE[] SEC("license") = "Dual MIT/Apache-2.0";

#!/usr/bin/env bash
# Kernel-path proof: every `#[ignore]` io_uring, AF_XDP and DPDK test against live
# kernel, in privileged Docker plus one default-seccomp run.
# Usage: scripts/transports-kernel-proof.sh (needs docker; image built if missing).
# One PASS or FAIL line per case, then summary; non-zero exit on any FAIL.
# Case logs: target/linux-proof/kernel-proof/<case>.log
set -euo pipefail

IMAGE=polaris-transports-linux
LOGS=target/linux-proof/kernel-proof

# AF_XDP pair: IFACE in container netns, PEER_IFACE in NETNS (198.18.0.0/15: benchmark range)
IFACE=pxdp0
PEER_IFACE=pxdp1
NETNS=pxdp-peer
DST=198.18.0.1
PEER_ADDR=198.18.0.2
PIN_DIR=/sys/fs/bpf/polaris

FAILED=0

# host side, macOS bash 3.2 compatible

in_container() {
	local mode=$1
	shift
	# --init: Ctrl-C reaches bash (PID 1 ignores it)
	docker run --rm --init "$@" \
		-v "$ROOT":/w -w /w \
		-v polaris-cargo-registry:/usr/local/cargo/registry \
		-v polaris-cargo-git:/usr/local/cargo/git \
		-e CARGO_TARGET_DIR=/w/target/linux-proof \
		"$IMAGE" bash scripts/transports-kernel-proof.sh "$mode"
}

host() {
	ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
	docker image inspect "$IMAGE" >/dev/null 2>&1 ||
		docker build -t "$IMAGE" "$ROOT/scripts/kernel-proof"
	local status=0 passed failed
	# global: EXIT trap runs after `host` returns
	OUT=$(mktemp)
	trap 'rm -f "$OUT"' EXIT
	in_container privileged --privileged | tee "$OUT" || status=1
	# default seccomp profile and capabilities: io_uring must be refused
	in_container unprivileged | tee -a "$OUT" || status=1
	passed=$(grep -c '^PASS ' "$OUT" || true)
	failed=$(grep -c '^FAIL ' "$OUT" || true)
	echo "SUMMARY $passed passed, $failed failed"
	return "$status"
}

# container side

# case body runs in errexit subshell: failed setup or test gives FAIL, later cases still run
run_case() {
	local name=$1 log="$LOGS/${1//\//-}.log" status
	shift
	set +e
	(
		set -e
		"$@"
	) >"$log" 2>&1
	status=$?
	set -e
	if ((status == 0)); then
		echo "PASS $name: $(grep -oE '[0-9]+ tests? run: .*' "$log" | tail -n 1)"
	else
		echo "FAIL $name (exit $status, log $log)"
		tail -n 25 "$log" | sed 's/^/    /'
		FAILED=$((FAILED + 1))
	fi
}

# ignored tests only; exactly `want` must run and pass, so filter matching nothing fails
expect_tests() {
	local want=$1 out
	shift
	out=$(cargo nextest run --locked --color never --no-fail-fast --run-ignored ignored-only "$@" 2>&1) || {
		printf '%s\n' "$out"
		return 1
	}
	printf '%s\n' "$out"
	grep -qE "Summary \[.*\] $want tests? run: $want passed" <<<"$out" || {
		echo "expected $want tests run and passed"
		return 1
	}
}

io_uring_test() {
	expect_tests 1 -p transport-io-uring -E "test(=$1)"
}

io_uring_blocked() {
	# seccomp mode 2: Docker default profile active, so refusal comes from it
	grep -Eq '^Seccomp:\s+2$' /proc/self/status || {
		echo "no seccomp filter: container not unprivileged"
		return 1
	}
	expect_tests 1 -p transport-io-uring -E 'test(=bind_without_io_uring_access_is_unavailable)'
}

peer() {
	ip netns exec "$NETNS" "$@"
}

veth_down() {
	# link delete detaches any XDP program; netns delete takes peer end
	ip link del "$IFACE" 2>/dev/null || true
	ip netns del "$NETNS" 2>/dev/null || true
	rm -rf "$PIN_DIR"
	# flush lazy AF_XDP socket release so next bind on queue is not EBUSY
	{ echo 1 >/sys/module/rcutree/parameters/do_rcu_barrier; } 2>/dev/null || true
}

# fresh pair per case; leftovers from earlier case or run removed first.
# No static neighbour: peer resolves DST by ARP, which built-in program must pass to kernel
veth_up() {
	trap veth_down EXIT
	veth_down
	ip netns add "$NETNS"
	ip link add "$IFACE" type veth peer name "$PEER_IFACE" netns "$NETNS"
	# IPv6 off before link up: no RS, DAD or MLD frame reaches tight-pool case
	sysctl -qw "net.ipv6.conf.$IFACE.disable_ipv6=1"
	peer sysctl -qw "net.ipv6.conf.$PEER_IFACE.disable_ipv6=1"
	ip addr add "$DST/30" dev "$IFACE"
	ip link set "$IFACE" up
	peer ip addr add "$PEER_ADDR/30" dev "$PEER_IFACE"
	peer ip link set "$PEER_IFACE" up
}

afxdp_test() {
	veth_up
	expect_tests 1 -p transport-afxdp -E "test(=$1)"
}

# fixture stands in for operator's external program: maps pinned by name, generic attach
afxdp_pinned() {
	local obj=/tmp/xsk_redirect.bpf.o
	veth_up
	clang -O2 -g -target bpf -I"/usr/include/$(uname -m)-linux-gnu" \
		-c scripts/kernel-proof/fixtures/xsk_redirect.bpf.c -o "$obj"
	mkdir -p "$PIN_DIR"
	bpftool prog load "$obj" "$PIN_DIR/xsk_redirect" type xdp pinmaps "$PIN_DIR"
	bpftool net attach xdpgeneric pinned "$PIN_DIR/xsk_redirect" dev "$IFACE"
	# fixture redirects every frame, ARP included: peer learns receiver MAC statically
	peer ip neigh replace "$DST" lladdr "$(cat "/sys/class/net/$IFACE/address")" \
		dev "$PEER_IFACE" nud permanent
	AFXDP_PINNED_MAP=$PIN_DIR/xsks_map expect_tests 1 -p transport-afxdp \
		-E 'test(=pinned_passes_conformance_and_leaves_program_attached)'
}

privileged() {
	mkdir -p "$LOGS"
	mount -t bpf bpf /sys/fs/bpf
	export AFXDP_IFACE=$IFACE AFXDP_DST=$DST AFXDP_PEER_NETNS=$NETNS
	local path
	for path in legacy buf_ring multishot; do
		run_case "io_uring/$path" io_uring_test "${path}_path"
	done
	run_case io_uring/multicast io_uring_test multicast_join_receives_group_datagram
	run_case afxdp/builtin-skb afxdp_test builtin_skb_passes_conformance_and_detaches_on_drop
	run_case afxdp/builtin-drv afxdp_test builtin_drv_passes_conformance_and_detaches_on_drop
	run_case afxdp/pinned afxdp_pinned
	run_case afxdp/multicast afxdp_test multicast_join_listed_and_group_datagram_received
	# tests start EAL themselves (--no-huge, net_pcap vdevs) and write own pcap input
	run_case dpdk/net_pcap expect_tests 2 -p transport-dpdk --features driver-dpdk
	((FAILED == 0))
}

unprivileged() {
	mkdir -p "$LOGS"
	run_case io_uring/unprivileged io_uring_blocked
	((FAILED == 0))
}

case ${1:-} in
"") host ;;
privileged) privileged ;;
unprivileged) unprivileged ;;
*)
	echo "usage: $0" >&2
	exit 2
	;;
esac

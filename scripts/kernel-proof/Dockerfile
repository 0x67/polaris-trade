# Linux check image: clippy, tests and link checks for the Linux-only crates, plus the
# privileged kernel-path proof. Correctness host only; no performance number comes from here.
FROM rust:1.96.1-trixie

# keep in step with `components` in the repo's rust-toolchain.toml (build context cannot read it)
RUN rustup component add rustfmt clippy rust-src llvm-tools

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      libdpdk-dev libpcap-dev pkg-config iproute2 bpftool clang libbpf-dev \
 && rm -rf /var/lib/apt/lists/*

# prebuilt nextest for the image's architecture
ARG NEXTEST_VERSION=0.9.133
RUN case "$(uname -m)" in \
      x86_64) platform=linux ;; \
      aarch64) platform=linux-arm ;; \
      *) echo "unsupported arch $(uname -m)"; exit 1 ;; \
    esac \
 && curl -LsSf "https://get.nexte.st/${NEXTEST_VERSION}/${platform}" | tar zxf - -C /usr/local/cargo/bin \
 && cargo nextest --version

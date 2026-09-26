FROM rust:1.95.0-bookworm@sha256:6258907abe69656e41cd992e0b705cdcfabcbbe3db374f92ed2d47121282d4a1 AS builder

WORKDIR /app

ENV RUSTUP_AUTO_INSTALL=0

COPY rust-toolchain.toml ./

RUN test "$(rustc --version --verbose | sed -n 's/^release: //p')" = "$RUST_VERSION"

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY tests ./tests
COPY src ./src

RUN cargo build --release --locked -p o-sfu --bin o-sfu

FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171 AS runtime

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

RUN useradd --system --create-home --home-dir /srv/o-sfu --shell /usr/sbin/nologin osfu

WORKDIR /srv/o-sfu

COPY --from=builder /app/target/release/o-sfu /usr/local/bin/o-sfu

ENV HTTP_INTERFACE=0.0.0.0:8070
ENV PROXY=false
ENV RUST_LOG=info

EXPOSE 8070
EXPOSE 40000-49999/udp

USER osfu

CMD ["o-sfu"]

# syntax=docker/dockerfile:1
#
# Container image for the `live-trader` binary — the one crate that signs and
# posts REAL orders. Multi-stage: a Rust builder (needs a C toolchain for
# rustls/ring) and a minimal Debian runtime (needs ca-certificates for the
# rustls "native-roots" TLS used to reach Polymarket / Pyth / the CEX feeds).
#
# Build:  docker build -t polymarket-live-trader .
# Run:    see README notes / docker-compose.yml. The private key is read from
#         $POLY_PRIVATE_KEY at runtime and is never baked into the image.

# ---- Builder -------------------------------------------------------------
# Full (non-slim) image: buildpack-deps base ships gcc/make so ring compiles.
# Toolchain floor is dependency-driven: alloy/ruint in Cargo.lock require rustc
# >= 1.91 (edition 2024 itself only needs >= 1.85). Bump this if the lock drifts
# to deps needing a newer rustc — the failure is a clear "not supported by" list.
FROM rust:1.91-bookworm AS builder

WORKDIR /src
COPY . .

# BuildKit cache mounts keep the crates.io registry and the target/ dir warm
# across rebuilds. The binary lives inside the target/ cache mount, so it must
# be copied out to a real image path within the same RUN step. --locked builds
# exactly what Cargo.lock pins.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release -p live-trader \
    && cp target/release/live-trader /usr/local/bin/live-trader

# ---- Runtime -------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# ca-certificates: rustls native-roots reads the system trust store, so without
#   these every HTTPS/WSS connection fails with "no roots".
# tzdata: chrono::Local (period_of_day + default filename stamp) reads the zone
#   db; install it so `-e TZ=America/New_York` works. Absent TZ -> UTC.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

# Non-root runtime user with a home at /app.
RUN useradd --system --uid 10001 --create-home --home-dir /app trader

WORKDIR /app
COPY --from=builder /usr/local/bin/live-trader /usr/local/bin/live-trader

# CSVs default to ./data/trade-<stamp>.csv (created relative to WORKDIR).
RUN mkdir -p /app/data && chown -R trader:trader /app
VOLUME ["/app/data"]

USER trader

# `docker stop` defaults to SIGTERM, but the trader only listens for SIGINT
# (tokio signal::ctrl_c) to run its graceful shutdown: settle + cancel open
# orders, resolve the final window, flush the CSV. Make stop send SIGINT so
# that path runs instead of a hard SIGKILL.
STOPSIGNAL SIGINT

# Faithful to the binary: LIVE by default (posts real orders once
# POLY_PRIVATE_KEY is set). Pass --dry-run to run the full strategy with no
# wallet and no posting. Extra flags after the image name are forwarded here.
ENTRYPOINT ["/usr/local/bin/live-trader"]

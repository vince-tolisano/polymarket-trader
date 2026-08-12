# syntax=docker/dockerfile:1
#
# Container image for the `live-trader` binary — the one crate that signs and
# posts REAL orders — plus its no-wallet siblings `dry-trader` (same strategy,
# simulated fills, CSVs in dry-data/) and `feature-recorder` (posts nothing and
# takes no position: samples BOTH sides of the order book at 1s ticks into
# feature-data/, which is the only dataset that carries bid/size — see the
# compose service for why). Multi-stage: a Rust builder (needs a C toolchain
# for rustls/ring) and a minimal Debian runtime (needs ca-certificates for the
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
# be copied out to a real image path within the same RUN step.
# NOTE: --locked was dropped temporarily so cargo can update Cargo.lock for the
# SDK 0.5 -> 0.6 bump (no local cargo to regenerate the lock). Restore --locked
# once an updated Cargo.lock is committed.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p live-trader -p dry-trader -p feature-recorder \
    && cp target/release/live-trader /usr/local/bin/live-trader \
    && cp target/release/dry-trader /usr/local/bin/dry-trader \
    && cp target/release/feature-recorder /usr/local/bin/feature-recorder

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
COPY --from=builder /usr/local/bin/dry-trader /usr/local/bin/dry-trader
COPY --from=builder /usr/local/bin/feature-recorder /usr/local/bin/feature-recorder

# CSVs default to ./data/trade-<stamp>.csv for live-trader and
# ./dry-data/trade-<stamp>.csv for dry-trader (relative to WORKDIR).
# feature-recorder has no data/ subdir convention of its own — it writes
# features-<unix_ts>.csv into the CWD, so its compose service sets working_dir
# to /app/feature-data rather than passing --out (see that service's comment).
RUN mkdir -p /app/data /app/dry-data /app/feature-data && chown -R trader:trader /app
VOLUME ["/app/data", "/app/dry-data", "/app/feature-data"]

USER trader

# `docker stop` defaults to SIGTERM, but the trader only listens for SIGINT
# (tokio signal::ctrl_c) to run its graceful shutdown: settle + cancel open
# orders, resolve the final window, flush the CSV. Make stop send SIGINT so
# that path runs instead of a hard SIGKILL.
STOPSIGNAL SIGINT

# Faithful to the binary: LIVE by default (posts real orders once
# POLY_PRIVATE_KEY is set). For the no-wallet simulated run, override the
# entrypoint to /usr/local/bin/dry-trader (docker-compose.yml's dry-trader
# service does this). Extra flags after the image name are forwarded here.
ENTRYPOINT ["/usr/local/bin/live-trader"]

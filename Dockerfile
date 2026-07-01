# Container image for file-replicator, for the KUBERNETES (and HOST/Docker) platform.
# Requires the ggcommons crate resolvable (cargo git dep on the private repo, or crates.io).
#
# Multi-stage: stage 1 compiles the standalone release binary; stage 2 is a slim glibc runtime that
# carries only the binary, run as a non-root user.
#
# Build (the cargo git dep needs network + git auth to fetch the private ggcommons repo —
# pass a GITHUB_TOKEN or mount an SSH agent):
#   docker build -t <image> .
# Then push to your registry (or `kind load docker-image <image>` for a local cluster) and set
# `image:` in k8s/deployment.yaml.

# ---- stage 1: build -------------------------------------------------------------------------
FROM rust:1.85-slim AS build

ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY Cargo.toml ./
COPY Cargo.lock* ./
COPY src ./src

RUN cargo build --release --bin file-replicator

# ---- stage 2: runtime -----------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /build/target/release/file-replicator /usr/local/bin/file-replicator

# Non-root, unprivileged (matches the Deployment's runAsNonRoot). The durable state dir (/data) is a
# PVC mounted writable by this uid; see k8s/deployment.yaml.
USER 65532:65532

# No default args: with --platform auto the library auto-detects KUBERNETES (or HOST) and defaults its
# config source / transport / identity. Override via the Deployment's args: if needed.
ENTRYPOINT ["/usr/local/bin/file-replicator"]

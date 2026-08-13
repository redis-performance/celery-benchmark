FROM rust:1-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /app

# Cache dependency compilation — only re-runs when Cargo files change.
# On rust:1-alpine the host target is already musl, so --target is not needed and
# the binary lands at target/release/ on both amd64 and arm64.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo "fn main(){}" > src/main.rs \
    && cargo build --release \
    && rm -rf src

# Build the real binary (touch to bust the cached dummy timestamp)
COPY src ./src
RUN touch src/main.rs && cargo build --release

# ── Runtime image ─────────────────────────────────────────────────────────────
FROM alpine:3.21

# ca-certificates is required for TLS (rediss://) connections
RUN apk add --no-cache ca-certificates \
    && adduser -D -u 1000 bench

COPY --from=builder /app/target/release/celery-bench /usr/local/bin/celery-bench

USER bench
ENTRYPOINT ["celery-bench"]

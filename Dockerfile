# Build stage
FROM rust:1.94-slim AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p server

# Runtime stage
FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/server /usr/local/bin/server
COPY config.toml .
ENV LOCAL_CACHE_PATH=/tmp/nstp-cache
CMD ["server"]

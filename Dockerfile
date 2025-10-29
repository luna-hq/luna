FROM rust:1.90.0-bookworm AS builder
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml .
COPY rustfmt.toml .
COPY luna luna/
RUN cargo build --release

FROM debian:stable-slim
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 luna
RUN mkdir -p /data && chown luna:luna /data
COPY --from=builder /app/target/release/luna /usr/local/bin/luna
USER luna
WORKDIR /data
EXPOSE 7688
CMD ["luna", "--api-host-port", "0.0.0.0:7688"]
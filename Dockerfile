# Multi-stage build for Luna SQL server
# --- Builder Stage ---
# Use a specific stable version of Rust on Debian Bookworm
FROM rust:1.90.0-bookworm AS builder

# Install required dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy workspace files
COPY Cargo.toml .
COPY rustfmt.toml .

# Copy luna package
COPY luna luna/

# Build the application in release mode
RUN cargo build --release

# --- Runtime Stage ---
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 luna

# Create directory for database
RUN mkdir -p /data && chown luna:luna /data

# Copy binary from builder
COPY --from=builder /app/target/release/luna /usr/local/bin/luna

# Switch to non-root user
USER luna

# Set working directory
WORKDIR /data

# Expose default port
EXPOSE 7688

# Default command - can be overridden
CMD ["luna", "--api-host-port", "0.0.0.0:7688"]
# End of Dockerfile
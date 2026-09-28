# syntax=docker/dockerfile:1

# ---- build ----
FROM rust:1-bookworm AS build
WORKDIR /src
# Cache dependencies separately from the sources.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && echo '' > src/lib.rs \
    && cargo build --release --locked && rm -rf src
COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release --locked --bin mango
# An empty data directory owned by the runtime user, so fresh volumes are writable.
RUN mkdir /data-template

# ---- runtime ----
# distroless: glibc + CA certs, no shell, runs as an unprivileged user.
FROM gcr.io/distroless/cc-debian12:nonroot
LABEL org.opencontainers.image.title="mango" \
      org.opencontainers.image.description="🥭 A ripe, MongoDB-compatible document database with built-in Raft replication"
COPY --from=build /src/target/release/mango /usr/local/bin/mango
COPY --from=build --chown=65532:65532 /data-template /data
ENV MANGO_DATA_DIR=/data \
    MANGO_BIND=0.0.0.0:27017
VOLUME ["/data"]
# 27017: MongoDB wire protocol (clients). 7017: Raft (peers).
EXPOSE 27017 7017
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/mango"]

FROM rust:1.93 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 iptables && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/asteroidb /usr/local/bin/
ENTRYPOINT ["asteroidb"]

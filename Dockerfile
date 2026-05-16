FROM rust:1.89-slim-bookworm AS build

WORKDIR /src

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential ca-certificates pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY . .
RUN cargo build --release -p msgbus-server --bin msgbusd

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home-dir /var/lib/msgbus --create-home --shell /usr/sbin/nologin msgbus

COPY --from=build /src/target/release/msgbusd /usr/local/bin/msgbusd

USER msgbus
WORKDIR /var/lib/msgbus
VOLUME ["/var/lib/msgbus"]
EXPOSE 50051

ENTRYPOINT ["/usr/local/bin/msgbusd"]
CMD ["--listen", "0.0.0.0:50051", "--data", "/var/lib/msgbus/msgbus.redb", "--storage", "redb"]

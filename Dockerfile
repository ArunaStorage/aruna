# Build Stage
FROM rust:slim-buster AS builder
WORKDIR /build
RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y libpq-dev libssl-dev protobuf-compiler pkg-config
COPY . .
RUN cargo build --release

FROM rust:slim-buster
WORKDIR /run
RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y libpq-dev libssl-dev pkg-config ca-certificates
COPY --from=builder /build/target/release/aruna_server .
COPY config.toml .
CMD [ "/run/aruna_server" ]
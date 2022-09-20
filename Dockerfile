# Build Stage
FROM rust:slim-buster AS builder
WORKDIR /build
RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y libpq-dev libssl-dev protobuf-compiler
COPY . .
RUN cargo build --release

FROM alpine

RUN apk update
RUN apk upgrade
RUN apk add ca-certificates openssl
COPY --from=builder /build/target/release/aruna_server .
COPY .env .
CMD [ "./aruna_server" ]
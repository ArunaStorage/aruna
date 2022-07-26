# Build Stage
FROM rust:1.62 AS builder
WORKDIR /usr/src/
RUN apt-get -y update
RUN apt-get -y install llvm cmake gcc

COPY . .
RUN cargo build --release


FROM debian:stable-slim

RUN apt-get -y update
RUN apt-get -y install ca-certificates
COPY --from=builder /usr/src/target/release/aos_data_proxy .
CMD [ "./aos_data_proxy" ]
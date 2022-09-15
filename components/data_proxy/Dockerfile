# Build Stage
FROM rust:1-alpine AS builder
WORKDIR /usr/src/
RUN apk update
RUN apk upgrade
RUN apk add llvm cmake gcc ca-certificates libc-dev pkgconfig openssl-dev protoc

COPY . .
RUN cargo build --release

FROM alpine

RUN apk update
RUN apk upgrade
RUN apk add ca-certificates openssl
COPY --from=builder /usr/src/target/release/aos_data_proxy .
COPY .env .
CMD [ "./aos_data_proxy" ]
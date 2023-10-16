# Build Stage
FROM rust:1-alpine AS builder
WORKDIR /build
RUN apk update
RUN apk upgrade
ENV RUSTFLAGS="-C target-feature=-crt-static"
RUN apk add llvm cmake gcc ca-certificates libc-dev pkgconfig openssl-dev protoc protobuf-dev libpq-dev musl-dev git
COPY . .
RUN cargo build --release

FROM rust:1-alpine
WORKDIR /run
RUN apk update
RUN apk upgrade
RUN apk add llvm cmake gcc ca-certificates libc-dev pkgconfig openssl-dev protoc protobuf-dev libpq-dev musl-dev git
COPY --from=builder /build/target/release/aruna_server .
CMD [ "/run/aruna_server" ]

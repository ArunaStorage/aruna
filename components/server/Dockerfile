# Build Stage
FROM rust:1-alpine3.20 AS builder
WORKDIR /build
RUN apk update
RUN apk upgrade
ENV RUSTFLAGS="-C target-feature=-crt-static"
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN apk add llvm cmake gcc ca-certificates libc-dev pkgconfig openssl-dev protoc protobuf-dev protobuf-dev libpq-dev musl-dev git
COPY . .
RUN cargo build --release -p aruna_server

FROM alpine:3.20
WORKDIR /run
RUN apk update
RUN apk upgrade
RUN apk add llvm cmake gcc ca-certificates libc-dev pkgconfig openssl-dev protoc protobuf-dev libpq-dev musl-dev git
COPY --from=builder /build/target/release/aruna_server .
COPY --from=builder /build/components/server/.env .
COPY --from=builder /build/components/server/src/database/schema.sql .
CMD [ "/run/aruna_server" ]


# Build Stage
FROM rust:1-alpine AS builder
WORKDIR /build
RUN apk update
RUN apk upgrade
ENV RUSTFLAGS="-C target-feature=-crt-static"
RUN apk add llvm cmake gcc ca-certificates libc-dev pkgconfig openssl-dev protoc protobuf-dev protobuf-dev libpq-dev musl-dev git
COPY . .
RUN cargo build --release

FROM alpine:3.18
WORKDIR /run
RUN apk update
RUN apk upgrade
RUN apk add ca-certificates libpq-dev musl-dev
COPY --from=builder /build/target/release/aos_data_proxy .
COPY --from=builder /build/.env .
COPY --from=builder /build/src/database/schema.sql .
CMD [ "/run/aos_data_proxy" ]
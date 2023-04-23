# Build Stage
FROM harbor.computational.bio.uni-giessen.de/docker_hub_cache/library/rust:1 AS builder
WORKDIR /usr/src/
RUN apt-get -y update && apt-get -y upgrade
RUN apt-get -y install llvm cmake gcc ca-certificates libssl-dev libsodium-dev
ENV PB_REL="https://github.com/protocolbuffers/protobuf/releases"
RUN curl -LO $PB_REL/download/v3.15.8/protoc-3.15.8-linux-x86_64.zip
RUN unzip protoc-3.15.8-linux-x86_64.zip -d $HOME/.local
RUN export PATH="$PATH:$HOME/.local/bin"
COPY . .
RUN cargo build --release

FROM harbor.computational.bio.uni-giessen.de/docker_hub_cache/library/ubuntu

RUN apt-get -y update && apt-get -y upgrade
RUN apt-get install ca-certificates openssl
COPY --from=builder /usr/src/target/release/aos_data_proxy .
COPY .env .
CMD [ "./aos_data_proxy" ]
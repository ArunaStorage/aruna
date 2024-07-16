[![Rust](https://img.shields.io/badge/built_with-Rust-dca282.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/License-Apache_2.0-brightgreen.svg)](https://github.com/ArunaStorage/ArunaServer/blob/main/LICENSE)
![CI](https://github.com/ArunaStorage/ArunaServer/actions/workflows/test.yml/badge.svg)
[![Codecov](https://codecov.io/github/ArunaStorage/ArunaServer/coverage.svg?branch=main)](https://codecov.io/gh/ArunaStorage/ArunaServer)
[![Dependency status](https://deps.rs/repo/github/ArunaStorage/ArunaServer/status.svg)](https://deps.rs/repo/github/ArunaStorage/ArunaServer)
___

<p align="center">
    <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./img/aruna_white_font.png">
    <img alt="Aruna logo" src="./img/aruna_dark_font.png" width="70%">
    </picture>
</p>

<br>

# Aruna Data Orchestration Engine

**Aruna** is a geo-redundant data orchestration engine that manages scientific data and a rich set of associated metadata according to [FAIR](https://www.go-fair.org/fair-principles/) principles.

It supports multiple data storage backends (e.g. S3, File ...) via [data proxies](https://github.com/ArunaStorage/aruna/tree/main/components/data_proxy) that expose an S3-compatible interface.
The main [server](https://github.com/ArunaStorage/aruna/tree/main/components/server) handles metadata, user and resource hierarchies while the data proxies handle the data itself.
Data proxies can communicate with each other in a peer-to-peer-like network and share data.

This repository is split into two components, the server and the data proxy.

## Features

- [FAIR](https://www.go-fair.org/fair-principles/), geo-redundant, data storage for multiple scientific domains
- Decentralized data storage system
- Data proxy specific authorization rules to restrict access on the data side
- Data proxy ingestion that can integrate existing data collections
- Organization of your data objects into projects, collections and datasets
- Flexible, file format and data structure independent metadata annotation via labels and dedicated metadata files (e.g. [schema.org](https://schema.org/))
- Notification streams for all actions performed
- Compatible with multiple (existing) data storage architectures (S3, File, ...)
- S3-compatible API for pre-authenticated upload and download URLs
- REST-API and dedicated client libraries for Python, Rust, Go and Java
- Hook system to integrate  external workflows for data validation and transformation
- Dedicated rule system to handle custom server-side authorization

## Getting started

### How to run a local test instance

Start the needed containers
```bash
docker compose up
```
Run the server
```bash
cd components/server
cargo run --release
```

Run the proxy in a new instance
```bash
cd components/data_proxy
cargo run --release
```

Alternatively you can build docker images for server and data proxy with
```bash
docker build -t server -f ./components/server/Dockerfile .
docker build -t data_proxy -f ./components/data_proxy/Dockerfile .
```



### Interacting with aruna

Test tokens can be found in `components/server/tests/common/keycloak/fake-tokens.md`.
A detailed user guide is found in the [Documentation](https://arunastorage.github.io/documentation/latest/).

**TLDR:**
- **REST**: see our [Swagger Documentation](https://api.dev.aruna-storage.org/swagger-ui/)
- **Python**: [PyPI](https://pypi.org/project/Aruna-Python-API/) 
- **Rust**: [Crates.io](https://crates.io/crates/aruna-rust-api)
- **Go**: [Go package](https://github.com/ArunaStorage/go-api/releases/)


## License

The API is licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option. Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion for Aruna by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions. 
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

# Aruna Object Storage - Server


**Aruna Object Storage** is a geo-redundant data lake storage system that manages scientific data and a rich set of associated metadata according to [FAIR](https://www.go-fair.org/fair-principles/) principles.

It supports multiple data storage backends (e.g. S3, File ...) via a [DataProxy](https://github.com/ArunaStorage/DataProxy) that exposes a S3-compatible interface.

This is the main repo for the server application of the Aruna Object Storage (AOS).

## Features

- [FAIR](https://www.go-fair.org/fair-principles/), geo-redundant, data storage for multiple scientific domains
- Organize your data objects in projects, collections and (optional) object_groups
- Flexible, file format and data structure independent metadata annotations via labels and dedicated metadata files (e.g [schema.org](https://schema.org/))
- Notification streams for all performed actions
- Compatible with multiple (existing) data storage architectures (S3, File, ...)
- S3-compatible API for pre-authenticated up- and download URLs
- REST-API and dedicated client libraries for Python, Rust, Go and Java
- (planned) integrated scheduling of external workflows for data validation and transformation

## Getting started

A detailed user guide is found in the [Documentation](https://arunastorage.github.io/Documentation/).

**TLDR:**
- **REST**: see our [Swagger Documentation](https://api.aruna.nfdi-dev.gi.denbi.de/swagger-ui/)
- **Python**: [PyPI](https://pypi.org/project/Aruna-Python-API/) 
- **Rust**: [Crates.io](https://crates.io/crates/aruna-rust-api)
- **Go**: [Go package](https://github.com/ArunaStorage/go-api/releases/)

## License

The API is licensed under the [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) license. See the [License](LICENSE.md) file for more information
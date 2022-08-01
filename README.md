[![Codecov](https://codecov.io/github/ArunaStorage/ArunaServer/coverage.svg?branch=main)](https://codecov.io/gh/ArunaStorage/ArunaServer)
[![Dependency status](https://deps.rs/repo/github/ArunaStorage/ArunaServer/status.svg)](https://deps.rs/repo/github/ArunaStorage/ArunaServer)

# Aruna Object Storage Server

Aruna Object Storage is data lake application with API that manages scientific data objects according to [FAIR](https://www.go-fair.org/fair-principles/) principles. It interacts with multiple data storage backends (e.g. S3, File ...) via the [DataProxy](https://github.com/ArunaStorage/DataProxy) application and stores a rich and queryable set of metadata about stored objects in a postgres compatible database (e.g. CockroachDB). Aruna is conceptualy geo-redundant with multiple dataset and database locations. Users can choose where and how to store their data.

This is the main repo for the server application of the Aruna Object Storage (AOS).

## Features

- ObjectStorage (S3) and file storage compatible
- Pre-authenticated up- and download URLs for your data
- Organize your data in Collections and objectgroups
- Immutability of objects with strict versioning
- Increased findability through a rich set of metadata and optional labels
- Possibility to include further metadata description as files for each object

## License

The API is licensed under the [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) license. See the [License](LICENSE.md) file for more information
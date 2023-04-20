[![Rust](https://img.shields.io/badge/built_with-Rust-dca282.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/License-Apache_2.0-brightgreen.svg)](https://github.com/ArunaStorage/ArunaServer/blob/main/LICENSE-APACHE)
[![Dependency status](https://deps.rs/repo/github/ArunaStorage/DataProxy/status.svg)](https://deps.rs/repo/github/ArunaStorage/DataProxy)

---

# DataProxy

DataProxy is a subcomponent of Aruna Object Storage that provides a partially compatible S3 API for data storage with advanced features like encryption at REST, deduplication, and storage according to FAIR principles.
Features

- Partial S3 API compatibility: DataProxy implements a subset of the S3 API, making it easy to integrate with existing S3 clients and libraries.
- Encryption at rest: all data stored in DataProxy is encrypted at rest, ensuring the confidentiality and integrity of your data.
- Deduplication: DataProxy uses advanced deduplication algorithms to ensure that identical data is stored only once, reducing storage costs and improving performance.
- FAIR principles: DataProxy adheres to the FAIR principles of data management, ensuring that your data is findable, accessible, interoperable and reusable.

## Getting started

To use DataProxy in your application, you will need to:

- Install and configure Aruna Object Storage on your server or cloud platform.
- Configure your S3 client or library to use the DataProxy endpoint.
- Create buckets and upload your data to the DataProxy.

For detailed instructions on how to get started, please see the [documentation](https://arunastorage.github.io/Documentation/v1.0.x/get_started/basic_usage/00_index/).

## Support

If you need help with DataProxy, you can reach out to our support team at support@aruna-storage.org.

## Conclusion

DataProxy is a secure, scalable and partially S3 compatible storage component for Aruna Object Storage, offering encryption at REST, deduplication and adherence to the FAIR principles of data management. With partial S3 API compatibility, DataProxy makes it easy to store and manage your data in the cloud.
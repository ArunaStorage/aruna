# DataProxy

## Introduction

The DataProxy service is designed to handle data transfers AOS.
It starts two server: One server implements the internal proxy api from the official AOS gRPC API. The other server handles the transfer of the actual data. All data access is handled through presigned URLs.

Currently only single uploads and downloads are supported.

## Configuration

The configuration is provided via environment variables.

| Parameter                     | Environment variable  | default        |
| ----------------------------- | --------------------- | -------------- |
| Internal proxy socket address | -                     | 0.0.0.0        |
| Internal proxy socket port    | -                     | 8080           |
| Data proxy socket address     | -                     | 0.0.0.0        |
| Data proxy socker port        | -                     | 8081           |
| S3 access key                 | AWS_ACCESS_KEY_ID     | \*             |
| S3 secret key                 | AWS_SECRET_ACCESS_KEY | \*             |
| HMAC signage key              | HMAC_SIGN_KEY         | \*             |
| Hostname for the data proxy   | PROXY_DATA_HOST       | localhost:8081 |
| S3 endpoint                   | S3_ENDPOINT_HOST      | localhost:9000 |

Required environment variables are marked with \*.

use s3s::host::DomainError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProxyError {
    #[error("S3s wrong domain error: {0}")]
    DomainError(#[from] DomainError),
}
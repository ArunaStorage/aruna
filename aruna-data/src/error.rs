use s3s::host::DomainError;
use thiserror::Error;

#[macro_export]
macro_rules! logerr {
    () => {
        |e| {
            tracing::error!("Error: {:?}", e);
        }
    };
}

#[derive(Error, Debug)]
pub enum ProxyError {
    #[error("S3s wrong domain error: {0}")]
    DomainError(#[from] DomainError),
    #[error("IOError: {0}")]
    IOError(#[from] std::io::Error),
    #[error("HeedError: {0}")]
    HeedError(#[from] heed::Error),
}

use s3s::{host::DomainError, s3_error, S3Error};
use thiserror::Error;

use crate::s3;

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
    #[error("Invalid Server URI: {0}")]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error("Tonic connection error: {0}")]
    TonicError(#[from] tonic::transport::Error),
    #[error("Invalid Config: {0}")]
    InvalidConfig(String),
    #[error("Request Error: {0}")]
    RequestError(String),
    #[error("Internal Error: {0}")]
    OpendalError(#[from] opendal::Error),
    #[error("Body Error: {0}")]
    BodyError(String),
    #[error("InvalidAccessKey")]
    InvalidAccessKey,

}

impl From<ProxyError> for S3Error {
    fn from(e: ProxyError) -> Self {
        let message = e.to_string();
        s3_error!(InternalError, "{}", message)
    }
}

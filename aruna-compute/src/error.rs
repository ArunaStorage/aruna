use thiserror::Error;
use tracing::subscriber::SetGlobalDefaultError;
use tracing_subscriber::filter::ParseError;

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum ComputeError {
    #[error("Parse error: {0}")]
    TracingParseError(#[from] ParseError),
    #[error("Set default error: {0}")]
    TracingSetGlobalDefaultError(#[from] SetGlobalDefaultError),
    #[error("Request error {0}")]
    ReqwestError(#[from] reqwest::Error)
}

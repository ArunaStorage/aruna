use async_channel::{Receiver, Sender};
use async_trait::async_trait;

#[async_trait]
pub trait UploadSingleDataMiddleware {
    async fn handle_stream(&self)
        -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>;
    async fn get_sender(
        &self,
    ) -> Sender<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>>;
    async fn get_receiver(&self) -> Receiver<bytes::Bytes>;
}

#[async_trait]
pub trait UploadMultiDataMiddleware {
    async fn handle_stream(&self)
        -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>;
    async fn get_sender(
        &self,
    ) -> Sender<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>>;
    async fn get_receiver(&self) -> Receiver<bytes::Bytes>;
}

#[async_trait]
pub trait DownloadDataMiddleware {
    async fn handle_stream(&self)
        -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>;
    async fn get_sender(&self) -> Sender<bytes::Bytes>;
    async fn get_receiver(&self) -> Receiver<bytes::Bytes>;
}

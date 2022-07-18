use async_channel::{Receiver, Sender};
use async_trait::async_trait;

#[async_trait]
pub trait DataMiddleware {
    async fn handle_stream(&self);
    async fn get_sender(
        &self,
    ) -> Sender<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>>;
    async fn get_receiver(&self) -> Receiver<bytes::Bytes>;
}

use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use log::info;

use super::data_middlware::DataMiddleware;

pub struct EmptyMiddleware {
    sender: Sender<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>>,
    recv: Receiver<bytes::Bytes>,
}

impl EmptyMiddleware {
    pub async fn new(
        sender: Sender<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>>,
        recv: Receiver<bytes::Bytes>,
    ) -> Self {
        return EmptyMiddleware {
            recv: recv,
            sender: sender,
        };
    }
}

#[async_trait]
impl DataMiddleware for EmptyMiddleware {
    async fn get_sender(
        &self,
    ) -> Sender<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>> {
        return self.sender.clone();
    }
    async fn get_receiver(&self) -> Receiver<bytes::Bytes> {
        return self.recv.clone();
    }
    async fn handle_stream(&self) {
        while let Ok(data) = self.recv.recv().await {
            self.sender.send(Ok(data)).await.unwrap();
        }
    }
}

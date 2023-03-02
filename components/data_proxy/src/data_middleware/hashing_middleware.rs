use async_channel::{Receiver, Sender};
use async_trait::async_trait;

use super::data_middlware::DataMiddleware;

pub struct HashingMiddleWare {
    sender: Sender<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>>,
    recv: Receiver<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>>,
}

impl HashingMiddleWare {
    pub async fn _new(
        sender: Sender<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>>,
        recv: Receiver<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>>,
    ) -> Self {
        HashingMiddleWare { recv, sender }
    }
}

#[async_trait]
impl DataMiddleware for HashingMiddleWare {
    async fn get_sender(
        &self,
    ) -> Sender<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>> {
        return self.sender.clone();
    }
    async fn get_receiver(
        &self,
    ) -> Receiver<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>> {
        return self.recv.clone();
    }
    async fn handle_stream(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        while let Ok(data) = self.recv.recv().await? {
            self.sender.send(Ok(data)).await.unwrap();
        }

        Ok(())
    }
}

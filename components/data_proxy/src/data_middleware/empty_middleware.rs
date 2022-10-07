use async_channel::{ Receiver, Sender };
use async_trait::async_trait;

use super::data_middlware::{ DownloadDataMiddleware, UploadDataMiddleware };

pub struct EmptyMiddlewareUpload {
    sender: Sender<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>>,
    recv: Receiver<bytes::Bytes>,
}

pub struct EmptyMiddlewareDownload {
    sender: Sender<bytes::Bytes>,
    recv: Receiver<bytes::Bytes>,
}

impl EmptyMiddlewareUpload {
    pub async fn new(
        sender: Sender<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>>,
        recv: Receiver<bytes::Bytes>
    ) -> Self {
        return EmptyMiddlewareUpload {
            recv,
            sender,
        };
    }
}

#[async_trait]
impl UploadDataMiddleware for EmptyMiddlewareUpload {
    async fn get_sender(
        &self
    ) -> Sender<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>> {
        return self.sender.clone();
    }
    async fn get_receiver(&self) -> Receiver<bytes::Bytes> {
        return self.recv.clone();
    }
    async fn handle_stream(
        &self
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        while let Ok(data) = self.recv.recv().await {
            self.sender.send(Ok(data)).await.unwrap();
        }

        Ok(())
    }
}

impl EmptyMiddlewareDownload {
    pub async fn new(sender: Sender<bytes::Bytes>, recv: Receiver<bytes::Bytes>) -> Self {
        return EmptyMiddlewareDownload {
            recv: recv,
            sender: sender,
        };
    }
}

#[async_trait]
impl DownloadDataMiddleware for EmptyMiddlewareDownload {
    async fn handle_stream(
        &self
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        while let Ok(data) = self.recv.recv().await {
            match self.sender.send(data).await {
                Ok(_) => {}
                Err(err) => {
                    log::error!("{}", err);
                    break;
                }
            }
        }

        Ok(())
    }
    async fn get_sender(&self) -> Sender<bytes::Bytes> {
        return self.sender.clone();
    }
    async fn get_receiver(&self) -> Receiver<bytes::Bytes> {
        return self.recv.clone();
    }
}
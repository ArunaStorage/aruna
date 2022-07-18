use async_channel::Receiver;
use async_trait::async_trait;

#[async_trait]
pub trait StorageBackend {
    async fn upload_object(
        &self,
        recv: Receiver<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>>,
        bucket: String,
        key: String,
        content_len: i64,
    );
    async fn create_object_key(&self, object_id: String, revision_id: String) -> String;
}

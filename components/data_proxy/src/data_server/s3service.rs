use std::sync::Arc;

use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::internal::v1::internal_proxy_notifier_service_client::InternalProxyNotifierServiceClient;
use s3s::dto::*;
use s3s::S3Request;
use s3s::S3Result;
use s3s::S3;
use tonic::transport::Channel;

use crate::backends::storage_backend::StorageBackend;

#[derive(Debug)]
pub struct ServiceSettings {
    pub endpoint_id: uuid::Uuid,
    pub encrypting: bool,
    pub compressing: bool,
}

impl Default for ServiceSettings {
    fn default() -> Self {
        Self {
            endpoint_id: Default::default(),
            encrypting: true,
            compressing: true,
        }
    }
}

#[derive(Debug)]
pub struct S3ServiceServer {
    backend: Arc<Box<dyn StorageBackend>>,
    internal_notifier_service: InternalProxyNotifierServiceClient<Channel>,
    settings: ServiceSettings,
}

impl S3ServiceServer {
    pub async fn new(
        backend: Arc<Box<dyn StorageBackend>>,
        url: impl Into<String>,
        settings: ServiceSettings,
    ) -> Result<Self> {
        Ok(S3ServiceServer {
            backend,
            internal_notifier_service: InternalProxyNotifierServiceClient::connect(url.into())
                .await
                .map_err(|_| anyhow!("Unable to connect to internal notifiers"))?,
            settings,
        })
    }

    async fn move_encode(from: Location, to: Location) -> Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl S3 for S3ServiceServer {
    #[tracing::instrument]
    async fn create_bucket(
        &self,
        _req: S3Request<CreateBucketInput>,
    ) -> S3Result<CreateBucketOutput> {
        let output = CreateBucketOutput::default();
        Ok(output)
    }
}

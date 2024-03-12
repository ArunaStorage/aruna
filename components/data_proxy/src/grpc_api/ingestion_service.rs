use crate::{
    caching::cache::Cache,
    data_backends::storage_backend::StorageBackend,
};
use aruna_rust_api::api::dataproxy::services::v2::{dataproxy_ingestion_service_server::DataproxyIngestionService, IngestExistingObjectRequest, IngestExistingObjectResponse};
use std::sync::Arc;

#[derive(Clone)]
pub struct DataproxyIngestionServiceImpl {
    pub cache: Arc<Cache>,
    pub backend: Arc<Box<dyn StorageBackend>>,
}

impl DataproxyIngestionServiceImpl {
    #[tracing::instrument(level = "trace", skip(cache))]
    pub fn new(
        cache: Arc<Cache>,
        backend: Arc<Box<dyn StorageBackend>>,
    ) -> Self {
        Self {
            cache,
            backend,
        }
    }
}

#[tonic::async_trait]
impl DataproxyIngestionService for DataproxyIngestionServiceImpl {

    async fn ingest_existing_object(
        &self,
        _request: tonic::Request<IngestExistingObjectRequest>,
    ) -> std::result::Result<
        tonic::Response<IngestExistingObjectResponse>,
        tonic::Status,
    >{
        todo!()
    }

}
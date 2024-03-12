use crate::{auth::auth_helpers::get_token_from_md, caching::cache::Cache, data_backends::storage_backend::StorageBackend};
use aruna_rust_api::api::dataproxy::services::v2::{
    dataproxy_ingestion_service_server::DataproxyIngestionService, IngestExistingObjectRequest,
    IngestExistingObjectResponse,
};
use tracing::error;
use std::sync::Arc;

#[derive(Clone)]
pub struct DataproxyIngestionServiceImpl {
    pub cache: Arc<Cache>,
    pub backend: Arc<Box<dyn StorageBackend>>,
}

impl DataproxyIngestionServiceImpl {
    #[tracing::instrument(level = "trace", skip(cache))]
    pub fn new(cache: Arc<Cache>, backend: Arc<Box<dyn StorageBackend>>) -> Self {
        Self { cache, backend }
    }
}

#[tonic::async_trait]
impl DataproxyIngestionService for DataproxyIngestionServiceImpl {
    async fn ingest_existing_object(
        &self,
        request: tonic::Request<IngestExistingObjectRequest>,
    ) -> std::result::Result<tonic::Response<IngestExistingObjectResponse>, tonic::Status> {
        
        if let Some(a) = self.cache.auth.read().await.as_ref() {
            let token = get_token_from_md(request.metadata()).map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::unauthenticated(e.to_string())
            })?;

            let (u, tid, pk) = a.check_permissions(&token).map_err(|_| {
                error!(error = "Unable to authenticate user, check permissions");
                tonic::Status::unauthenticated("Unable to authenticate user")
            })?;

            if pk.is_proxy {
                error!(error = "Proxy token is not allowed to ingest objects");
                return Err(tonic::Status::unauthenticated("Proxy token is not allowed to ingest objects"));
            }

        }
        todo!()
    }
}

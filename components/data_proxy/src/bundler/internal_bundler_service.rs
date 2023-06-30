use aruna_rust_api::api::internal::v1::{
    internal_bundler_service_server::InternalBundlerService, EnableBundleRequest,
    EnableBundleResponse, InvalidateBundleRequest, InvalidateBundleResponse, PrepareBundleRequest,
    PrepareBundleResponse,
};
use std::sync::Arc;
use tokio::sync::Mutex;

use super::bundler::Bundler;

#[derive(Debug, Clone)]
pub struct InternalBundlerServiceImpl {
    bundler: Arc<Mutex<Bundler>>,
}

impl InternalBundlerServiceImpl {
    pub fn new(bundler: Arc<Mutex<Bundler>>) -> Self {
        InternalBundlerServiceImpl { bundler }
    }
}

#[tonic::async_trait]
impl InternalBundlerService for InternalBundlerServiceImpl {
    async fn prepare_bundle(
        &self,
        request: tonic::Request<PrepareBundleRequest>,
    ) -> std::result::Result<tonic::Response<PrepareBundleResponse>, tonic::Status> {
        self.bundler
            .lock()
            .await
            .bundle_store
            .insert(request.into_inner().bundle_id, None);

        Ok(tonic::Response::new(PrepareBundleResponse {}))
    }
    async fn enable_bundle(
        &self,
        request: tonic::Request<EnableBundleRequest>,
    ) -> std::result::Result<tonic::Response<EnableBundleResponse>, tonic::Status> {
        let bundle = request
            .into_inner()
            .bundle
            .ok_or_else(|| tonic::Status::invalid_argument("Invalid bundle"))?;
        self.bundler
            .lock()
            .await
            .bundle_store
            .insert(bundle.bundle_id.to_string(), Some(bundle));

        Ok(tonic::Response::new(EnableBundleResponse {}))
    }
    async fn invalidate_bundle(
        &self,
        request: tonic::Request<InvalidateBundleRequest>,
    ) -> std::result::Result<tonic::Response<InvalidateBundleResponse>, tonic::Status> {
        let bundle_id = request.into_inner().bundle_id;
        self.bundler.lock().await.bundle_store.remove(&bundle_id);
        Ok(tonic::Response::new(InvalidateBundleResponse {}))
    }
}

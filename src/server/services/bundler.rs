use aruna_rust_api::api::bundler::services::v1::{
    bundler_service_server::BundlerService, CreateBundleRequest, CreateBundleResponse,
    DeleteBundleRequest, DeleteBundleResponse,
};

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(BundlerServiceImpl);

#[tonic::async_trait]
impl BundlerService for BundlerServiceImpl {
    async fn create_bundle(
        &self,
        request: tonic::Request<CreateBundleRequest>,
    ) -> std::result::Result<tonic::Response<CreateBundleResponse>, tonic::Status> {
        todo!()
    }
    async fn delete_bundle(
        &self,
        request: tonic::Request<DeleteBundleRequest>,
    ) -> std::result::Result<tonic::Response<DeleteBundleResponse>, tonic::Status> {
        todo!()
    }
}

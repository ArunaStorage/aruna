use crate::database::{connection::Database, models::enums::UserRights};
use crate::error::{ArunaError, TypeConversionError};
use crate::server::services::authz::Authz;
use crate::server::services::utils::format_grpc_request;
use aruna_rust_api::api::bundler::services::v1::{
    bundler_service_server::BundlerService, CreateBundleRequest, CreateBundleResponse,
    DeleteBundleRequest, DeleteBundleResponse,
};
use std::str::FromStr;
use std::sync::Arc;

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(BundlerServiceImpl);

#[tonic::async_trait]
impl BundlerService for BundlerServiceImpl {
    async fn create_bundle(
        &self,
        request: tonic::Request<CreateBundleRequest>,
    ) -> std::result::Result<tonic::Response<CreateBundleResponse>, tonic::Status> {
        log::info!("Received CreateBundleRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Authorize collection - READ
        self.authz
            .collection_authorize(
                request.metadata(),
                diesel_ulid::DieselUlid::from_str(&request.get_ref().collection_id)
                    .map_err(|_| ArunaError::TypeConversionError(TypeConversionError::UUID))?,
                UserRights::WRITE,
            )
            .await?;
        todo!()
    }
    async fn delete_bundle(
        &self,
        request: tonic::Request<DeleteBundleRequest>,
    ) -> std::result::Result<tonic::Response<DeleteBundleResponse>, tonic::Status> {
        log::info!("Received DeleteBundleRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Authorize collection - READ
        self.authz
            .collection_authorize(
                request.metadata(),
                diesel_ulid::DieselUlid::from_str(&request.get_ref().collection_id)
                    .map_err(|_| ArunaError::TypeConversionError(TypeConversionError::UUID))?,
                UserRights::WRITE,
            )
            .await?;
        todo!()
    }
}

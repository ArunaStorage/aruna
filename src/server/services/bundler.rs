use crate::database::models::object::{DataProxyFeature, Endpoint};
use crate::database::{connection::Database, models::enums::UserRights};
use crate::error::{ArunaError, TypeConversionError};
use crate::server::services::authz::Authz;
use crate::server::services::utils::{format_grpc_request, format_grpc_response};
use aruna_rust_api::api::bundler::services::v1::{
    bundler_service_server::BundlerService, CreateBundleRequest, CreateBundleResponse,
    DeleteBundleRequest, DeleteBundleResponse,
};
use aruna_rust_api::api::internal::v1::internal_bundler_service_client::InternalBundlerServiceClient;
use aruna_rust_api::api::internal::v1::{
    EnableBundleRequest, InvalidateBundleRequest, PrepareBundleRequest,
};
use std::str::FromStr;
use std::sync::Arc;
use tokio::task;

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
        let database_clone = self.database.clone();

        let filename = request.get_ref().filename.to_string();

        let (bundle_id, endpoint, to_create) =
            task::spawn_blocking(move || database_clone.create_bundle(request.into_inner()))
                .await
                .map_err(ArunaError::from)??;

        if let Some(to_cr) = to_create {
            // Try to establish connection to endpoint
            let mut data_proxy = InternalBundlerServiceClient::connect(
                endpoint.get_primary_url(DataProxyFeature::INTERNAL)?.0,
            )
            .await
            .map_err(|_| ArunaError::InvalidRequest("Unable to connect to bundler".to_string()))?;

            data_proxy
                .prepare_bundle(tonic::Request::new(PrepareBundleRequest {
                    bundle_id: bundle_id.to_string(),
                }))
                .await?;

            data_proxy
                .enable_bundle(tonic::Request::new(EnableBundleRequest {
                    bundle: Some(to_cr),
                }))
                .await?;
        }

        let response = tonic::Response::new(CreateBundleResponse {
            bundle_id: bundle_id.to_string(),
            url: create_url(&endpoint, &bundle_id, &filename)?,
        });
        log::info!("Sending CreateBundleResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
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
        let database_clone = self.database.clone();

        let bid = request.get_ref().bundle_id.to_string();

        let (resp, endpoint) =
            task::spawn_blocking(move || database_clone.delete_bundle(request.into_inner()))
                .await
                .map_err(ArunaError::from)??;

        // Try to establish connection to endpoint
        let mut data_proxy = InternalBundlerServiceClient::connect(
            endpoint.get_primary_url(DataProxyFeature::INTERNAL)?.0,
        )
        .await
        .map_err(|_| ArunaError::InvalidRequest("Unable to connect to bundler".to_string()))?;

        data_proxy
            .invalidate_bundle(tonic::Request::new(InvalidateBundleRequest {
                bundle_id: bid,
            }))
            .await?;

        let response = tonic::Response::new(resp);
        log::info!("Sending DeleteBundleResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }
}

pub fn create_url(ep: &Endpoint, bundle_id: &str, filename: &str) -> Result<String, ArunaError> {
    let (bundler_url, ssl) = ep.get_primary_url(DataProxyFeature::BUNDLER)?;

    let prefix = if ssl { "https://" } else { "http://" };

    Ok(format!(
        "{prefix}{bundler_url}/{bundle_id}/{filename}.tar.gz"
    ))
}

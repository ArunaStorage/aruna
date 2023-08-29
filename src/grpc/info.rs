use crate::auth::permission_handler::PermissionHandler;
use crate::caching::cache::Cache;
use crate::middlelayer::db_handler::DatabaseHandler;
use aruna_rust_api::api::storage::models::v2::Pubkey;
use aruna_rust_api::api::storage::services::v2::storage_status_service_server::StorageStatusService;
use aruna_rust_api::api::storage::services::v2::{
    GetPubkeysRequest, GetPubkeysResponse, GetStorageStatusRequest, GetStorageStatusResponse,
    GetStorageVersionRequest, GetStorageVersionResponse,
};
use std::sync::Arc;
use tonic::Response;

crate::impl_grpc_server!(StorageStatusServiceImpl);

#[tonic::async_trait]
impl StorageStatusService for StorageStatusServiceImpl {
    /// GetStorageVersion
    ///
    /// Status: BETA
    ///
    /// A request to get the current version of the server application
    /// String representation and https://semver.org/
    async fn get_storage_version(
        &self,
        _request: tonic::Request<GetStorageVersionRequest>,
    ) -> Result<Response<GetStorageVersionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Nothing to see here!"))
    }
    /// GetStorageStatus
    ///
    /// Status: ALPHA
    ///
    /// A request to get the current status of the storage components by location(s)
    async fn get_storage_status(
        &self,
        _request: tonic::Request<GetStorageStatusRequest>,
    ) -> Result<Response<GetStorageStatusResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Nothing to see here!"))
    }

    async fn get_pubkeys(
        &self,
        _request: tonic::Request<GetPubkeysRequest>,
    ) -> Result<Response<GetPubkeysResponse>, tonic::Status> {
        let pubkeys = self
            .cache
            .pubkeys
            .iter()
            .map(|pk| Pubkey {
                id: *pk.key(),
                key: pk.value().get_key_string(),
                location: pk.value().get_name(),
            })
            .collect();

        let response = GetPubkeysResponse { pubkeys };

        Ok(Response::new(response))
    }
}

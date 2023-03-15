use super::authz::Authz;
use crate::database::connection::Database;

use crate::error::ArunaError;
use crate::server::services::utils::{format_grpc_request, format_grpc_response};

use aruna_rust_api::api::internal::v1::internal_proxy_notifier_service_server::InternalProxyNotifierService;
use aruna_rust_api::api::internal::v1::{
    FinalizeObjectRequest, FinalizeObjectResponse, GetEncryptionKeyRequest,
    GetEncryptionKeyResponse,
};
use std::sync::Arc;
use tokio::task;
use tonic::{Request, Response, Status};

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(InternalProxyNotifierServiceImpl);

/// Trait created by tonic based on gRPC service definitions from .proto files.
///   The source .proto files are defined in the ArunaStorage/ArunaAPI repo.
#[tonic::async_trait]
impl InternalProxyNotifierService for InternalProxyNotifierServiceImpl {
    /// Finalizes the object after the data proxy has worked its compression/encryption magic.
    ///
    /// ## Arguments:
    ///
    /// * `Request<FinalizeObjectRequest>` -
    ///   A gRPC request which contains the final object location and the calculated hashes of the objects data.
    ///
    /// ## Returns:
    ///
    /// * `Result<Response<FinalizeObjectResponse>, Status>` - An empty FinalizeObjectResponse signals success.
    ///
    /// ## Behaviour:
    ///
    /// Updates the sole existing object location with the provided data of the final location the
    /// object has been moved to. Also validates/creates the provided hashes depending if the individual hash
    /// already exists in the database.
    async fn finalize_object(
        &self,
        request: Request<FinalizeObjectRequest>,
    ) -> Result<Response<FinalizeObjectResponse>, Status> {
        log::info!("Received FinalizeObjectRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Consume gRPC request
        let inner_request = request.into_inner();

        // Finalize Object in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || database_clone.finalize_object(&inner_request))
                .await
                .map_err(ArunaError::from)??,
        );

        // Return gRPC response after everything succeeded
        log::info!("Sending InitializeNewObjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    /// Get the encryption key associated with the object
    ///
    /// ## Arguments:
    ///
    /// * `Request<GetEncryptionKeyRequest>` -
    ///   A gRPC request which contains the information needed to query a specific encryption key.
    ///
    /// ## Returns:
    ///
    /// * `Result<Response<GetEncryptionKeyResponse>, Status>` - Contains the object data encryption/decryption key.
    async fn get_encryption_key(
        &self,
        request: Request<GetEncryptionKeyRequest>,
    ) -> Result<Response<GetEncryptionKeyResponse>, Status> {
        log::info!("Received GetEncryptionKeyRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Consume gRPC request
        let inner_request = request.into_inner();

        // Finalize Object in database
        let database_clone = self.database.clone();
        let maybe_key =
            task::spawn_blocking(move || database_clone.get_encryption_key(&inner_request))
                .await
                .map_err(ArunaError::from)??;

        let response = match maybe_key {
            None => tonic::Response::new(GetEncryptionKeyResponse {
                encryption_key: "".to_string(),
            }),
            Some(enc_key) => tonic::Response::new(GetEncryptionKeyResponse {
                encryption_key: enc_key.encryption_key,
            }),
        };

        // Return gRPC response after everything succeeded
        log::info!("Sending GetEncryptionKeyResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }
}

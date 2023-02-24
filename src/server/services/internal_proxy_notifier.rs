use super::authz::Authz;
use crate::database::connection::Database;

use aruna_rust_api::api::internal::v1::internal_proxy_notifier_service_server::InternalProxyNotifierService;
use aruna_rust_api::api::internal::v1::{
    FinalizeObjectRequest, FinalizeObjectResponse, GetEncryptionKeyRequest,
    GetEncryptionKeyResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(InternalProxyNotifierServiceImpl);

/// Trait created by tonic based on gRPC service definitions from .proto files.
///   The source .proto files are defined in the ArunaStorage/ArunaAPI repo.
#[tonic::async_trait]
impl InternalProxyNotifierService for InternalProxyNotifierServiceImpl {
    async fn finalize_object(
        &self,
        _request: Request<FinalizeObjectRequest>,
    ) -> Result<Response<FinalizeObjectResponse>, Status> {
        Err(Status::unimplemented(
            "This service call is not yet implemented.",
        ))
    }

    async fn get_encryption_key(
        &self,
        _request: Request<GetEncryptionKeyRequest>,
    ) -> Result<Response<GetEncryptionKeyResponse>, Status> {
        Err(Status::unimplemented(
            "This service call is not yet implemented.",
        ))
    }
}

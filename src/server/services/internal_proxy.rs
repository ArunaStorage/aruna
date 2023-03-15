use super::authz::Authz;
use crate::database::connection::Database;

use aruna_rust_api::api::internal::v1::internal_proxy_service_server::InternalProxyService;
use aruna_rust_api::api::internal::v1::{
    DeleteObjectRequest, DeleteObjectResponse, FinishMultipartUploadRequest,
    FinishMultipartUploadResponse, InitMultipartUploadRequest, InitMultipartUploadResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(InternalProxyServiceImpl);

/// Trait created by tonic based on gRPC service definitions from .proto files.
///   The source .proto files are defined in the ArunaStorage/ArunaAPI repo.
#[tonic::async_trait]
impl InternalProxyService for InternalProxyServiceImpl {
    async fn init_multipart_upload(
        &self,
        _request: Request<InitMultipartUploadRequest>,
    ) -> Result<Response<InitMultipartUploadResponse>, Status> {
        Err(Status::unimplemented(
            "This service call is not yet implemented.",
        ))
    }

    async fn finish_multipart_upload(
        &self,
        _request: Request<FinishMultipartUploadRequest>,
    ) -> Result<Response<FinishMultipartUploadResponse>, Status> {
        Err(Status::unimplemented(
            "This service call is not yet implemented.",
        ))
    }

    async fn delete_object(
        &self,
        _request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        Err(Status::unimplemented(
            "This service call is not yet implemented.",
        ))
    }
}

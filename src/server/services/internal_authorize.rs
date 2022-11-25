use super::authz::Authz;
use crate::database::connection::Database;
use aruna_rust_api::api::internal::v1::{
    internal_authorize_service_server::InternalAuthorizeService, AuthorizeRequest,
    AuthorizeResponse,
};
use std::sync::Arc;

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(InternalAuthorizeServiceImpl);

#[tonic::async_trait]
impl InternalAuthorizeService for InternalAuthorizeServiceImpl {
    async fn authorize(
        &self,
        _request: tonic::Request<AuthorizeRequest>,
    ) -> Result<tonic::Response<AuthorizeResponse>, tonic::Status> {
        todo!()
    }
}

use super::authz::Authz;
use crate::database::connection::Database;
use aruna_rust_api::api::internal::v1::{
    internal_event_service_server::InternalEventService, CreateStreamGroupRequest,
    CreateStreamGroupResponse, DeleteStreamGroupRequest, DeleteStreamGroupResponse,
    GetSharedRevisionRequest, GetSharedRevisionResponse, GetStreamGroupRequest,
    GetStreamGroupResponse,
};
use std::sync::Arc;

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(InternalEventServiceImpl);

#[tonic::async_trait]
impl InternalEventService for InternalEventServiceImpl {
    async fn create_stream_group(
        &self,
        _request: tonic::Request<CreateStreamGroupRequest>,
    ) -> Result<tonic::Response<CreateStreamGroupResponse>, tonic::Status> {
        todo!()
    }
    async fn get_stream_group(
        &self,
        _request: tonic::Request<GetStreamGroupRequest>,
    ) -> Result<tonic::Response<GetStreamGroupResponse>, tonic::Status> {
        todo!()
    }
    async fn delete_stream_group(
        &self,
        _request: tonic::Request<DeleteStreamGroupRequest>,
    ) -> Result<tonic::Response<DeleteStreamGroupResponse>, tonic::Status> {
        todo!()
    }
    async fn get_shared_revision(
        &self,
        _request: tonic::Request<GetSharedRevisionRequest>,
    ) -> Result<tonic::Response<GetSharedRevisionResponse>, tonic::Status> {
        todo!()
    }
}

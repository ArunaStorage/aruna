use crate::models::requests::{AddUserRequest, CreateGroupRequest, GetGroupRequest};
use crate::transactions::controller::Controller;
use aruna_rust_api::v3::aruna::api::v3 as grpc;
use aruna_rust_api::v3::aruna::api::v3::group_service_server::GroupService;
use std::sync::Arc;

use super::grpc_helpers::get_token;

pub struct GroupServiceImpl {
    pub handler: Arc<Controller>,
}
impl GroupServiceImpl {
    pub fn new(handler: Arc<Controller>) -> Self {
        Self { handler }
    }
}

#[async_trait::async_trait]
impl GroupService for GroupServiceImpl {
    async fn create_group(
        &self,
        request: tonic::Request<grpc::CreateGroupRequest>,
    ) -> Result<tonic::Response<grpc::CreateGroupResponse>, tonic::Status> {
        let token = get_token(request.metadata());
        let controller = self.handler.clone();

        Ok(tonic::Response::new(
            controller
                .request(CreateGroupRequest::from(request.into_inner()), token)
                .await?
                .into(),
        ))
    }

    async fn add_user(
        &self,
        request: tonic::Request<grpc::AddUserRequest>,
    ) -> Result<tonic::Response<grpc::AddUserResponse>, tonic::Status> {
        let token = get_token(request.metadata());
        let controller = self.handler.clone();

        Ok(tonic::Response::new(
            controller
                .request(AddUserRequest::try_from(request.into_inner())?, token)
                .await?
                .into(),
        ))
    }

    async fn share_permission_to_group(
        &self,
        _request: tonic::Request<grpc::SharePermissionToGroupRequest>,
    ) -> Result<tonic::Response<grpc::SharePermissionToGroupResponse>, tonic::Status> {
        todo!()
    }
    async fn get_group(
        &self,
        request: tonic::Request<grpc::GetGroupRequest>,
    ) -> Result<tonic::Response<grpc::GetGroupResponse>, tonic::Status> {
        let token = get_token(request.metadata());
        let controller = self.handler.clone();

        Ok(tonic::Response::new(
            controller
                .request(GetGroupRequest::try_from(request.into_inner())?, token)
                .await?
                .into(),
        ))
    }

    async fn get_users_from_group(
        &self,
        _request: tonic::Request<grpc::GetUsersFromGroupRequest>,
    ) -> Result<tonic::Response<grpc::GetUsersFromGroupResponse>, tonic::Status> {
        todo!()
    }
}

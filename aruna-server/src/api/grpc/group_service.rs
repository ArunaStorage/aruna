use crate::requests::controller::Controller;
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
                .request(request.into_inner().into(), token)
                .await?
                .into(),
        ))
    }

    async fn add_user(
        &self,
        _request: tonic::Request<grpc::AddUserRequest>,
    ) -> Result<tonic::Response<grpc::AddUserResponse>, tonic::Status> {
        todo!()
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
                .get_group(token, request.into_inner().try_into()?)
                .await?
                .into(),
        ))
    }
}

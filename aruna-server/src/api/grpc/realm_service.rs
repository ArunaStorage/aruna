use crate::api::grpc::grpc_helpers::get_token;
use crate::transactions::controller::Controller;
use aruna_rust_api::v3::aruna::api::v3 as grpc;
use aruna_rust_api::v3::aruna::api::v3::realm_service_server::RealmService;
use std::sync::Arc;

pub struct RealmServiceImpl {
    pub handler: Arc<Controller>,
}
impl RealmServiceImpl {
    pub fn new(handler: Arc<Controller>) -> Self {
        Self { handler }
    }
}

#[tonic::async_trait]
impl RealmService for RealmServiceImpl {
    async fn create_realm(
        &self,
        request: tonic::Request<grpc::CreateRealmRequest>,
    ) -> Result<tonic::Response<grpc::CreateRealmResponse>, tonic::Status> {
        let token = get_token(request.metadata());
        let controller = self.handler.clone();

        todo!()
        // Ok(tonic::Response::new(
        //     controller
        //         .create_realm(token, request.into_inner().into())
        //         .await?
        //         .into(),
        // ))
    }

    async fn get_realm(
        &self,
        request: tonic::Request<grpc::GetRealmRequest>,
    ) -> Result<tonic::Response<grpc::GetRealmResponse>, tonic::Status> {
        let token = get_token(request.metadata());
        let controller = self.handler.clone();

        todo!()
        // Ok(tonic::Response::new(
        //     controller
        //         .get_realm(token, request.into_inner().try_into()?)
        //         .await?
        //         .into(),
        // ))
    }
    async fn add_group(
        &self,
        request: tonic::Request<grpc::AddGroupRequest>,
    ) -> Result<tonic::Response<grpc::AddGroupResponse>, tonic::Status> {
        let token = get_token(request.metadata());
        let controller = self.handler.clone();

        todo!()
        // Ok(tonic::Response::new(
        //     controller
        //         .add_group(token, request.into_inner().try_into()?)
        //         .await?
        //         .into(),
        // ))
    }

    async fn remove_group(
        &self,
        _request: tonic::Request<grpc::RemoveGroupRequest>,
    ) -> Result<tonic::Response<grpc::RemoveGroupResponse>, tonic::Status> {
        todo!()
    }
}

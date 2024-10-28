use super::grpc_helpers::get_token;
use crate::requests::controller::Controller;
use aruna_rust_api::v3::aruna::api::v3::{
    resource_service_server::ResourceService, CreateProjectRequest, CreateProjectResponse,
    CreateResourceRequest, CreateResourceResponse, GetResourceRequest, GetResourceResponse,
};
use std::{result::Result, sync::Arc};
use tonic::{Request, Response, Status};

pub struct ResourceServiceImpl {
    pub handler: Arc<Controller>,
}

impl ResourceServiceImpl {
    pub fn new(handler: Arc<Controller>) -> Self {
        Self { handler }
    }
}

#[tonic::async_trait]
impl ResourceService for ResourceServiceImpl {
    async fn create_project(
        &self,
        request: Request<CreateProjectRequest>,
    ) -> Result<Response<CreateProjectResponse>, Status> {
        let token = get_token(request.metadata());
        let controller = self.handler.clone();

        todo!()
        // Ok(tonic::Response::new(
        //     controller
        //         .create_project(token, request.into_inner().try_into()?)
        //         .await?
        //         .into(),
        // ))
    }
    async fn create_resource(
        &self,
        request: tonic::Request<CreateResourceRequest>,
    ) -> Result<tonic::Response<CreateResourceResponse>, tonic::Status> {
        let token = get_token(request.metadata());
        let controller = self.handler.clone();

        todo!()
        // Ok(tonic::Response::new(
        //     controller
        //         .create_resource(token, request.into_inner().try_into()?)
        //         .await?
        //         .into(),
        // ))
    }

    async fn get_resource(
        &self,
        request: tonic::Request<GetResourceRequest>,
    ) -> Result<tonic::Response<GetResourceResponse>, tonic::Status> {
        let token = get_token(request.metadata());
        let controller = self.handler.clone();

        todo!()
        // Ok(tonic::Response::new(
        //     controller
        //         .get_resource(token, request.into_inner().try_into()?)
        //         .await?
        //         .into(),
        // ))
    }
}

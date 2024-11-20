use super::grpc_helpers::get_token;
use crate::models;
use crate::transactions::controller::Controller;
use aruna_rust_api::v3::aruna::api::v3::{
    resource_service_server::ResourceService, CreateProjectRequest, CreateProjectResponse, CreateResourceBatchRequest, CreateResourceBatchResponse, CreateResourceRequest, CreateResourceResponse, GetRelationsRequest, GetRelationsResponse, GetResourcesRequest, GetResourcesResponse, RegisterDataRequest, RegisterDataResponse
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

        Ok(tonic::Response::new(
            controller
                .request(
                    models::requests::CreateProjectRequest::try_from(request.into_inner())?,
                    token,
                )
                .await?
                .into(),
        ))
    }
    async fn create_resource(
        &self,
        request: tonic::Request<CreateResourceRequest>,
    ) -> Result<tonic::Response<CreateResourceResponse>, tonic::Status> {
        let token = get_token(request.metadata());
        let controller = self.handler.clone();

        Ok(tonic::Response::new(
            controller
                .request(
                    models::requests::CreateResourceRequest::try_from(request.into_inner())?,
                    token,
                )
                .await?
                .into(),
        ))
    }

    async fn get_resources(
        &self,
        request: tonic::Request<GetResourcesRequest>,
    ) -> Result<tonic::Response<GetResourcesResponse>, tonic::Status> {
        let token = get_token(request.metadata());
        let controller = self.handler.clone();

        Ok(tonic::Response::new(
            controller
                .request(
                    models::requests::GetResourcesRequest::try_from(request.into_inner())?,
                    token,
                )
                .await?
                .into(),
        ))
    }

    async fn create_resource_batch(
        &self,
        request: tonic::Request<CreateResourceBatchRequest>,
    ) -> std::result::Result<
        tonic::Response<CreateResourceBatchResponse>,
        tonic::Status,
    >{
        todo!()
    }

    async fn get_relations(
        &self,
        request: tonic::Request<GetRelationsRequest>,
    ) -> std::result::Result<
        tonic::Response<GetRelationsResponse>,
        tonic::Status,
    >{
        todo!()
    }
    async fn register_data(
        &self,
        request: tonic::Request<RegisterDataRequest>,
    ) -> std::result::Result<
        tonic::Response<RegisterDataResponse>,
        tonic::Status,
    >{
        todo!()
    }
}

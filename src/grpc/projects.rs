use crate::auth::{Authorizer};
use crate::caching::cache::Cache;
use crate::database::connection::Database;

use aruna_rust_api::api::storage::services::v2::project_service_server::ProjectService;
use aruna_rust_api::api::storage::services::v2::{
    ArchiveProjectRequest, ArchiveProjectResponse, CreateProjectRequest, CreateProjectResponse,
    DeleteProjectRequest, DeleteProjectResponse, GetProjectRequest, GetProjectResponse,
    GetProjectsRequest, GetProjectsResponse, UpdateProjectDataClassRequest,
    UpdateProjectDataClassResponse, UpdateProjectDescriptionRequest,
    UpdateProjectDescriptionResponse, UpdateProjectKeyValuesRequest,
    UpdateProjectKeyValuesResponse, UpdateProjectNameRequest, UpdateProjectNameResponse,
};



use std::sync::Arc;
use std::sync::Mutex;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(ProjectServiceImpl);

#[tonic::async_trait]
impl ProjectService for ProjectServiceImpl {
    async fn create_project(
        &self,
        _request: Request<CreateProjectRequest>,
    ) -> Result<Response<CreateProjectResponse>> {
        todo!()
    }
    async fn get_project(
        &self,
        _request: Request<GetProjectRequest>,
    ) -> Result<Response<GetProjectResponse>> {
        todo!()
    }
    async fn get_projects(
        &self,
        _request: Request<GetProjectsRequest>,
    ) -> Result<Response<GetProjectsResponse>> {
        todo!()
    }
    async fn delete_project(
        &self,
        _request: Request<DeleteProjectRequest>,
    ) -> Result<Response<DeleteProjectResponse>> {
        todo!()
    }
    async fn update_project_name(
        &self,
        _request: Request<UpdateProjectNameRequest>,
    ) -> Result<Response<UpdateProjectNameResponse>> {
        todo!()
    }
    async fn update_project_description(
        &self,
        _request: Request<UpdateProjectDescriptionRequest>,
    ) -> Result<Response<UpdateProjectDescriptionResponse>> {
        todo!()
    }
    async fn update_project_key_values(
        &self,
        _request: Request<UpdateProjectKeyValuesRequest>,
    ) -> Result<Response<UpdateProjectKeyValuesResponse>> {
        todo!()
    }
    async fn update_project_data_class(
        &self,
        _request: Request<UpdateProjectDataClassRequest>,
    ) -> Result<Response<UpdateProjectDataClassResponse>> {
        todo!()
    }
    async fn archive_project(
        &self,
        _request: Request<ArchiveProjectRequest>,
    ) -> Result<Response<ArchiveProjectResponse>> {
        todo!()
    }
}

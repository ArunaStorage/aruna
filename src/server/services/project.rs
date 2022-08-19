use super::authz::Authz;
use crate::api::aruna::api::storage::services::v1::project_service_server::ProjectService;

use crate::api::aruna::api::storage::services::v1::*;
use crate::database::connection::Database;
use crate::database::models::enums::*;
use crate::error::ArunaError;
use std::sync::Arc;
use tonic::Response;

// ProjectServiceImpl struct
crate::impl_grpc_server!(ProjectServiceImpl);

#[tonic::async_trait]
impl ProjectService for ProjectServiceImpl {
    /// This creates a new authorization group.option
    /// All users and collections are bundled in a authorization group.
    async fn create_project(
        &self,
        request: tonic::Request<CreateProjectRequest>,
    ) -> Result<tonic::Response<CreateProjectResponse>, tonic::Status> {
        let user_id = self.authz.admin_authorize(request.metadata()).await?;

        Ok(Response::new(
            self.database
                .create_project(request.into_inner(), user_id)?,
        ))
    }
    /// AddUserToProject Adds a new user to a given project by its id
    async fn add_user_to_project(
        &self,
        request: tonic::Request<AddUserToProjectRequest>,
    ) -> Result<tonic::Response<AddUserToProjectResponse>, tonic::Status> {
        // Clone metadata map, TODO: Actually these should all be borrows and not moves!
        let metadata = request.metadata().clone();
        // Clone request to allow for move to database, TODO: Actually these should all be borrows and not moves!
        let req_clone = request.into_inner().clone();
        // Parse the project Uuid
        let parsed_project_id =
            uuid::Uuid::parse_str(&req_clone.project_id).map_err(ArunaError::from)?;

        // Authorize the request
        let _user_id = self
            .authz
            .project_authorize(&metadata, parsed_project_id, UserRights::ADMIN)
            .await?;

        // Add user to project
        Ok(Response::new(
            self.database.add_user_to_project(req_clone, _user_id)?,
        ))
    }
    /// GetProjectCollections Returns all collections that belong to a certain
    /// project
    async fn get_project_collections(
        &self,
        _request: tonic::Request<GetProjectCollectionsRequest>,
    ) -> Result<tonic::Response<GetProjectCollectionsResponse>, tonic::Status> {
        todo!()
    }
    /// GetProject Returns the specified project
    async fn get_project(
        &self,
        _request: tonic::Request<GetProjectRequest>,
    ) -> Result<tonic::Response<GetProjectResponse>, tonic::Status> {
        todo!()
    }
    /// This will destroy the project and all its associated data.
    /// including users, collections, and API tokens and all data associated with
    /// them.
    async fn destroy_project(
        &self,
        _request: tonic::Request<DestroyProjectRequest>,
    ) -> Result<tonic::Response<DestroyProjectResponse>, tonic::Status> {
        todo!()
    }
}

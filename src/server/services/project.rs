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
    /// CreateProject creates a new project. Only (global) admins can create new projects.
    ///
    /// ## Arguments
    ///
    /// * request: CreateProjectRequest: Contains information about the new project
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<CreateProjectResponse>, tonic::Status>: Returns information about the freshly created project
    ///
    async fn create_project(
        &self,
        request: tonic::Request<CreateProjectRequest>,
    ) -> Result<tonic::Response<CreateProjectResponse>, tonic::Status> {
        // Authorize as global admin
        let user_id = self.authz.admin_authorize(request.metadata()).await?;
        // Create new project and respond with overview
        Ok(Response::new(
            self.database
                .create_project(request.into_inner(), user_id)?,
        ))
    }

    /// AddUserToProject adds a new user to the project. Only project_admins can add a user to project.
    ///
    /// ## Arguments
    ///
    /// * request: AddUserToProjectRequest: Contains user_id and project_id.
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<AddUserToProjectResponse>, tonic::Status>: Placeholder, empty response means success
    ///
    async fn add_user_to_project(
        &self,
        request: tonic::Request<AddUserToProjectRequest>,
    ) -> Result<tonic::Response<AddUserToProjectResponse>, tonic::Status> {
        // Clone metadata map
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
    /// GetProjectCollections queries all collections of a project
    ///
    /// ## Arguments
    ///
    /// * request: GetProjectCollectionsRequest: Contains the project_id
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<GetProjectCollectionsResponse>, tonic::Status>: Returns a list with all collections that belong to this project.
    ///
    async fn get_project_collections(
        &self,
        request: tonic::Request<GetProjectCollectionsRequest>,
    ) -> Result<tonic::Response<GetProjectCollectionsResponse>, tonic::Status> {
        // Clone metadata map
        let metadata = request.metadata().clone();
        // Clone request to allow for move to database, TODO: Actually these should all be borrows and not moves!
        let req_clone = request.into_inner().clone();
        // Parse the project Uuid
        let parsed_project_id =
            uuid::Uuid::parse_str(&req_clone.project_id).map_err(ArunaError::from)?;

        let _user_id = self
            .authz
            .project_authorize(&metadata, parsed_project_id, UserRights::READ)
            .await?;

        Ok(Response::new(
            self.database.get_project_collections(req_clone, _user_id)?,
        ))
    }

    /// GetProject gets information about a project.
    ///
    /// ## Arguments
    ///
    /// * request: GetProjectRequest: project_id.
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<GetProjectResponse>, tonic::Status>: Returns a ProjectOverview that contains basic information about a project
    ///
    async fn get_project(
        &self,
        request: tonic::Request<GetProjectRequest>,
    ) -> Result<tonic::Response<GetProjectResponse>, tonic::Status> {
        // Clone metadata map
        let metadata = request.metadata().clone();
        // Clone request to allow for move to database, TODO: Actually these should all be borrows and not moves!
        let req_clone = request.into_inner().clone();
        // Parse the project Uuid
        let parsed_project_id =
            uuid::Uuid::parse_str(&req_clone.project_id).map_err(ArunaError::from)?;
        // Authorize user
        let _user_id = self
            .authz
            .project_authorize(&metadata, parsed_project_id, UserRights::READ)
            .await?;
        // Execute request and return response
        Ok(Response::new(
            self.database.get_project(req_clone, _user_id)?,
        ))
    }

    /// DestoryProject deletes a project and all associated user_permissions.
    /// Needs admin permissions and the project must be empty -> 0 collections must be associated.
    ///
    /// ## Arguments
    ///
    /// * request: DestroyProjectRequest: contains project_id.
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<DestroyProjectResponse>, tonic::Status>: Placeholder, currently empty
    ///
    async fn destroy_project(
        &self,
        request: tonic::Request<DestroyProjectRequest>,
    ) -> Result<tonic::Response<DestroyProjectResponse>, tonic::Status> {
        // Clone metadata map
        let metadata = request.metadata().clone();
        // Clone request to allow for move to database, TODO: Actually these should all be borrows and not moves!
        let req_clone = request.into_inner().clone();
        // Parse the project Uuid
        let parsed_project_id =
            uuid::Uuid::parse_str(&req_clone.project_id).map_err(ArunaError::from)?;
        // Authorize user
        let _user_id = self
            .authz
            .project_authorize(&metadata, parsed_project_id, UserRights::ADMIN)
            .await?;
        // Execute request and return response
        Ok(Response::new(
            self.database.destroy_project(req_clone, _user_id)?,
        ))
    }
}

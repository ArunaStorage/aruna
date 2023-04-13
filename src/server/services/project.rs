use super::authz::Authz;
use std::str::FromStr;


use crate::database::connection::Database;
use crate::database::models::enums::*;
use crate::error::ArunaError;
use crate::server::services::utils::{format_grpc_request, format_grpc_response};
use aruna_rust_api::api::storage::services::v1::project_service_server::ProjectService;
use aruna_rust_api::api::storage::services::v1::*;

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
        log::info!("Received CreateProjectRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Authorize as global admin
        let user_id = self.authz.admin_authorize(request.metadata()).await?;

        // Create new project and respond with overview
        let response = Response::new(
            self.database
                .create_project(request.into_inner(), user_id)?,
        );

        log::info!("Sending CreateProjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
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
        log::info!("Received AddUserToProjectRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Parse the project Uuid
        let parsed_project_id =
            diesel_ulid::DieselUlid::from_str(&request.get_ref().project_id).map_err(ArunaError::from)?;

        // Authorize the request
        let _user_id = self
            .authz
            .project_authorize(request.metadata(), parsed_project_id, UserRights::ADMIN)
            .await?;

        // Add user to project
        let response = Response::new(
            self.database
                .add_user_to_project(request.into_inner(), _user_id)?,
        );

        log::info!("Sending AddUserToProjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
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
        log::info!("Received GetProjectRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Parse the project Uuid
        let parsed_project_id =
            diesel_ulid::DieselUlid::from_str(&request.get_ref().project_id).map_err(ArunaError::from)?;

        // Authorize user
        let _user_id = self
            .authz
            .project_authorize(request.metadata(), parsed_project_id, UserRights::READ)
            .await?;

        // Execute request and return response
        let response = Response::new(self.database.get_project(request.into_inner(), _user_id)?);

        log::info!("Sending GetProjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
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
    async fn get_projects(
        &self,
        request: tonic::Request<GetProjectsRequest>,
    ) -> Result<tonic::Response<GetProjectsResponse>, tonic::Status> {
        log::info!("Received GetProjectsRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Authorize user
        let _user_id = self.authz.admin_authorize(request.metadata()).await?;

        // Execute request and return response
        let response = Response::new(self.database.get_projects(request.into_inner(), _user_id)?);

        log::info!("Sending GetProjectsResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// DestroyProject deletes a project and all associated user_permissions.
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
        log::info!("Received DestroyProjectRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Parse the project Uuid
        let parsed_project_id =
            diesel_ulid::DieselUlid::from_str(&request.get_ref().project_id).map_err(ArunaError::from)?;

        // Authorize user
        let _user_id = self
            .authz
            .project_authorize(request.metadata(), parsed_project_id, UserRights::ADMIN)
            .await?;

        // Execute request and return response
        let response = Response::new(
            self.database
                .destroy_project(request.into_inner(), _user_id)?,
        );

        log::info!("Sending DestroyProjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// UpdateProject updates a project and all associated user_permissions.
    /// Needs admin permissions and the project
    ///
    /// ## Arguments
    ///
    /// * request: UpdateProject: contains project_id and new project information.
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<UpdateProjectResponse>, tonic::Status>: ProjectOverview for the project
    async fn update_project(
        &self,
        request: tonic::Request<UpdateProjectRequest>,
    ) -> Result<tonic::Response<UpdateProjectResponse>, tonic::Status> {
        log::info!("Received UpdateProjectRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Parse the project Uuid
        let parsed_project_id =
            diesel_ulid::DieselUlid::from_str(&request.get_ref().project_id).map_err(ArunaError::from)?;

        // Authorize user
        let user_id = self
            .authz
            .project_authorize(request.metadata(), parsed_project_id, UserRights::ADMIN)
            .await?;

        // Execute request and return response
        let response = Response::new(
            self.database
                .update_project(request.into_inner(), user_id)?,
        );

        log::info!("Sending UpdateProjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// RemoveUserFromProject removes a specific user from the project
    /// Needs project admin permissions and the project
    ///
    /// ## Arguments
    ///
    /// * request: RemoveUserFromProjectRequest: contains project_id and user_id
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<RemoveUserFromProjectResponse>, tonic::Status>: Placeholder, empty response means success
    async fn remove_user_from_project(
        &self,
        request: tonic::Request<RemoveUserFromProjectRequest>,
    ) -> Result<tonic::Response<RemoveUserFromProjectResponse>, tonic::Status> {
        log::info!("Received RemoveUserFromProjectRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Parse the project Uuid
        let parsed_project_id =
            diesel_ulid::DieselUlid::from_str(&request.get_ref().project_id).map_err(ArunaError::from)?;

        // Authorize user
        let user_id = self
            .authz
            .project_authorize(request.metadata(), parsed_project_id, UserRights::ADMIN)
            .await?;

        // Execute request and return response
        let response = Response::new(
            self.database
                .remove_user_from_project(request.into_inner(), user_id)?,
        );

        log::info!("Sending RemoveUserFromProjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// Get the user_permission of a specific user for the project.
    ///
    /// ## Arguments
    ///
    /// * request: GetUserPermissionsForProjectRequest: contains project_id and user_id
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<GetUserPermissionsForProjectResponse>, tonic::Status>: Contains the specific project_permission for a user
    async fn get_user_permissions_for_project(
        &self,
        request: tonic::Request<GetUserPermissionsForProjectRequest>,
    ) -> Result<tonic::Response<GetUserPermissionsForProjectResponse>, tonic::Status> {
        log::info!("Received GetUserPermissionsForProjectRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Parse the project Uuid
        let parsed_project_id =
            diesel_ulid::DieselUlid::from_str(&request.get_ref().project_id).map_err(ArunaError::from)?;

        // Authorize user
        let _admin_user = self
            .authz
            .project_authorize(request.metadata(), parsed_project_id, UserRights::READ)
            .await?;

        // Execute request and return response
        let response = Response::new(
            self.database
                .get_user_permission_from_project(request.into_inner(), _admin_user)?,
        );

        log::info!("Sending GetUserPermissionsForProjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// Get all user permissions associated with a specific project.
    ///
    /// ## Arguments
    ///
    /// * request: GetAllUserPermissionsForProjectRequest: Contains project id
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<GetAllUserPermissionsForProjectResponse>, tonic::Status>:
    /// Contains the user permissions associated with the provided project
    ///
    async fn get_all_user_permissions_for_project(
        &self,
        request: tonic::Request<GetAllUserPermissionsForProjectRequest>,
    ) -> Result<tonic::Response<GetAllUserPermissionsForProjectResponse>, tonic::Status> {
        log::info!("Received GetAllUserPermissionsForProjectRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Parse the project Uuid
        let project_uuid =
            diesel_ulid::DieselUlid::from_str(&request.get_ref().project_id).map_err(ArunaError::from)?;

        // Authorize user
        self.authz
            .project_authorize(request.metadata(), project_uuid, UserRights::READ)
            .await?;

        // Execute request and return response
        let database_clone = self.database.clone();
        let response = tokio::task::spawn_blocking(move || {
            database_clone.get_all_user_permissions_from_project(&project_uuid)
        })
        .await
        .map_err(ArunaError::from)??;

        // Return gRPC response
        let grpc_response = tonic::Response::new(response);

        log::info!("Sending GetAllUserPermissionsForProjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&grpc_response));
        Ok(grpc_response)
    }

    /// EditUserPermissionsForProject updates the user permissions of a specific user
    /// Needs project admin permissions and the project
    ///
    /// ## Arguments
    ///
    /// * request: EditUserPermissionsForProjectRequest: contains project_id and user_permissions for a user (including user_id)
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<EditUserPermissionsForProjectResponse>, tonic::Status>: Placeholder, empty response means success
    async fn edit_user_permissions_for_project(
        &self,
        request: tonic::Request<EditUserPermissionsForProjectRequest>,
    ) -> Result<tonic::Response<EditUserPermissionsForProjectResponse>, tonic::Status> {
        log::info!("Received EditUserPermissionsForProjectRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Parse the project Uuid
        let parsed_project_id =
            diesel_ulid::DieselUlid::from_str(&request.get_ref().project_id).map_err(ArunaError::from)?;

        // Authorize user
        let user_id = self
            .authz
            .project_authorize(request.metadata(), parsed_project_id, UserRights::ADMIN)
            .await?;

        // Execute request and return response
        let response = Response::new(
            self.database
                .edit_user_permissions_for_project(request.into_inner(), user_id)?,
        );

        log::info!("Sending EditUserPermissionsForProjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }
}

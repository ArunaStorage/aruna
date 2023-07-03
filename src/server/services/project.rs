use super::authz::Authz;
use std::str::FromStr;

use crate::database::connection::Database;
use crate::database::models::enums::*;
use crate::error::ArunaError;
use crate::server::clients::event_emit_client::NotificationEmitClient;
use crate::server::services::utils::{format_grpc_request, format_grpc_response};
use aruna_rust_api::api::internal::v1::emitted_resource::Resource;
use aruna_rust_api::api::internal::v1::{EmittedResource, ProjectResource};
use aruna_rust_api::api::notification::services::v1::EventType;
use aruna_rust_api::api::storage::models::v1::ResourceType;
use aruna_rust_api::api::storage::services::v1::project_service_server::ProjectService;
use aruna_rust_api::api::storage::services::v1::*;
use tokio::task;

use std::sync::Arc;
use tonic::Response;

// ProjectServiceImpl struct
crate::impl_grpc_server!(
    ProjectServiceImpl,
    event_emitter: Option<NotificationEmitClient>
);

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

        // Consume gRPC request into its parts
        let (metadata, _, inner_request) = request.into_parts();

        // Authorize as global admin
        let user_id = self.authz.admin_authorize(&metadata).await?;

        // Create new project and respond with overview
        let project_ulid = self.database.create_project(inner_request, user_id)?;

        // Try to emit event notification
        if let Some(emit_client) = &self.event_emitter {
            let event_emitter_clone = emit_client.clone();
            task::spawn(async move {
                if let Err(err) = event_emitter_clone
                    .emit_event(
                        project_ulid.to_string(),
                        ResourceType::Project,
                        EventType::Created,
                        vec![EmittedResource {
                            resource: Some(Resource::Project(ProjectResource {
                                project_id: project_ulid.to_string(),
                            })),
                        }],
                    )
                    .await
                {
                    // Only log error but do not crash function execution at this point
                    log::error!("Failed to emit notification: {}", err)
                }
            })
            .await
            .map_err(ArunaError::from)?;
        }

        // Create gRPC response
        let response = Response::new(CreateProjectResponse {
            project_id: project_ulid.to_string(),
        });

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
        let parsed_project_id = diesel_ulid::DieselUlid::from_str(&request.get_ref().project_id)
            .map_err(ArunaError::from)?;

        // Authorize the request
        let _user_id = self
            .authz
            .project_authorize(
                request.metadata(),
                parsed_project_id,
                UserRights::ADMIN,
                false,
            )
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
        let parsed_project_id = diesel_ulid::DieselUlid::from_str(&request.get_ref().project_id)
            .map_err(ArunaError::from)?;

        // Authorize user
        let _user_id = self
            .authz
            .project_authorize(
                request.metadata(),
                parsed_project_id,
                UserRights::READ,
                true,
            )
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
        let parsed_project_id = diesel_ulid::DieselUlid::from_str(&request.get_ref().project_id)
            .map_err(ArunaError::from)?;

        // Authorize user
        let _user_id = self
            .authz
            .project_authorize(
                request.metadata(),
                parsed_project_id,
                UserRights::ADMIN,
                false,
            )
            .await?;

        // Try to delete project
        let database_clone = self.database.clone();
        let response = task::spawn_blocking(move || {
            database_clone.destroy_project(request.into_inner(), _user_id)
        })
        .await
        .map_err(ArunaError::from)??;

        // Try to emit event notification
        if let Some(emit_client) = &self.event_emitter {
            let event_emitter_clone = emit_client.clone();
            task::spawn(async move {
                if let Err(err) = event_emitter_clone
                    .emit_event(
                        parsed_project_id.to_string(),
                        ResourceType::Project,
                        EventType::Deleted,
                        vec![EmittedResource {
                            resource: Some(Resource::Project(ProjectResource {
                                project_id: parsed_project_id.to_string(),
                            })),
                        }],
                    )
                    .await
                {
                    // Only log error but do not crash function execution at this point
                    log::error!("Failed to emit notification: {}", err)
                }
            })
            .await
            .map_err(ArunaError::from)?;
        }

        // Return gRPC response
        let grpc_response = Response::new(response);

        log::info!("Sending DestroyProjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&grpc_response));
        Ok(grpc_response)
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
        let parsed_project_id = diesel_ulid::DieselUlid::from_str(&request.get_ref().project_id)
            .map_err(ArunaError::from)?;

        // Authorize user
        let user_id = self
            .authz
            .project_authorize(
                request.metadata(),
                parsed_project_id,
                UserRights::ADMIN,
                true,
            )
            .await?;

        // Create project
        let database_clone = self.database.clone();
        let response = task::spawn_blocking(move || {
            database_clone.update_project(request.into_inner(), user_id)
        })
        .await
        .map_err(ArunaError::from)??;

        // Try to emit event notification
        if let Some(emit_client) = &self.event_emitter {
            let event_emitter_clone = emit_client.clone();
            task::spawn(async move {
                if let Err(err) = event_emitter_clone
                    .emit_event(
                        parsed_project_id.to_string(),
                        ResourceType::Project,
                        EventType::Updated,
                        vec![EmittedResource {
                            resource: Some(Resource::Project(ProjectResource {
                                project_id: parsed_project_id.to_string(),
                            })),
                        }],
                    )
                    .await
                {
                    // Only log error but do not crash function execution at this point
                    log::error!("Failed to emit notification: {}", err)
                }
            })
            .await
            .map_err(ArunaError::from)?;
        }

        // Return gRPC response
        let grpc_response = Response::new(response);

        log::info!("Sending UpdateProjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&grpc_response));
        Ok(grpc_response)
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
        let parsed_project_id = diesel_ulid::DieselUlid::from_str(&request.get_ref().project_id)
            .map_err(ArunaError::from)?;

        // Authorize user
        let user_id = self
            .authz
            .project_authorize(
                request.metadata(),
                parsed_project_id,
                UserRights::ADMIN,
                false,
            )
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
        let parsed_project_id = diesel_ulid::DieselUlid::from_str(&request.get_ref().project_id)
            .map_err(ArunaError::from)?;

        // Authorize user
        let _admin_user = self
            .authz
            .project_authorize(
                request.metadata(),
                parsed_project_id,
                UserRights::READ,
                true,
            )
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
        let project_uuid = diesel_ulid::DieselUlid::from_str(&request.get_ref().project_id)
            .map_err(ArunaError::from)?;

        // Authorize user
        self.authz
            .project_authorize(request.metadata(), project_uuid, UserRights::READ, true)
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
        let parsed_project_id = diesel_ulid::DieselUlid::from_str(&request.get_ref().project_id)
            .map_err(ArunaError::from)?;

        // Authorize user
        let user_id = self
            .authz
            .project_authorize(
                request.metadata(),
                parsed_project_id,
                UserRights::ADMIN,
                false,
            )
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

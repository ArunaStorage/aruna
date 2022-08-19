//! This file contains all database methods that handle project specific actions
//!
//! Mainly this is used to:
//!
//! - Create a new project
//! - AddUserToProject
//!
use super::utils::*;
use crate::api::aruna::api::storage::services::v1::{
    AddUserToProjectRequest, AddUserToProjectResponse, CreateProjectRequest, CreateProjectResponse,
};
use crate::database::connection::Database;
use crate::database::models::auth::{Project, UserPermission};
use crate::error::ArunaError;

use chrono::Utc;
use diesel::{insert_into, prelude::*};

impl Database {
    /// Creates a new project in the database
    ///
    /// ## Arguments
    ///
    /// * request: CreateProjectRequest: Contains information about the new project
    /// * user_id: uuid::Uuid : who created this project ?
    ///
    /// ## Returns
    ///
    /// * Result<CreateProjectRequest, ArunaError>: Returns the UUID of the new project
    ///
    pub fn create_project(
        &self,
        request: CreateProjectRequest,
        user_id: uuid::Uuid,
    ) -> Result<CreateProjectResponse, ArunaError> {
        use crate::database::schema::projects::dsl::*;
        use diesel::result::Error as dError;

        // Create a new uuid for the project
        let project_id = uuid::Uuid::new_v4();

        // Create db project struct
        let project = Project {
            id: project_id,
            name: request.name,
            description: request.description,
            flag: 0, // For now this should be 0, additional flag option will come later
            created_at: Utc::now().naive_utc(),
            created_by: user_id,
        };

        // Execute db insert_into
        self.pg_connection
            .get()?
            .transaction::<_, dError, _>(|conn| {
                insert_into(projects).values(&project).execute(conn)
            })?;

        Ok(CreateProjectResponse {
            project_id: project_id.to_string(),
        })
    }

    /// Adds an existing user to a project
    ///
    /// ## Arguments
    ///
    /// * request: AddUserToProjectRequest: Which user should be added, which permissions should this user have ?
    /// * user_id: uuid::Uuid : who added the user ?
    ///
    /// ## Returns
    ///
    /// * Result<AddUserToProjectResponse, ArunaError>: Placeholder, currently empty
    ///
    pub fn add_user_to_project(
        &self,
        request: AddUserToProjectRequest,
        _user_id: uuid::Uuid,
    ) -> Result<AddUserToProjectResponse, ArunaError> {
        use crate::database::schema::user_permissions::dsl::*;
        use diesel::result::Error as dError;
        // Map grpc_perm because it is an Option by default
        let grpc_perm = request.user_permission.ok_or_else(|| {
            ArunaError::InvalidRequest(
                "Project permissions are required to add a user to project".to_string(),
            )
        })?;
        // Create new uuid for permission
        let new_id = uuid::Uuid::new_v4();
        // Create database permission struct
        let user_permission = UserPermission {
            id: new_id,
            user_id: uuid::Uuid::parse_str(&grpc_perm.user_id)?,
            user_right: map_permissions(grpc_perm.permission()).ok_or_else(|| {
                ArunaError::InvalidRequest("User permissions are required".to_string())
            })?,
            project_id: uuid::Uuid::parse_str(&request.project_id)?,
        };

        // Execute db insert
        self.pg_connection
            .get()?
            .transaction::<_, dError, _>(|conn| {
                insert_into(user_permissions)
                    .values(&user_permission)
                    .execute(conn)
            })?;

        // Return empty response
        Ok(AddUserToProjectResponse {})
    }
}

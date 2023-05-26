//! This file contains all database methods that handle project specific actions
//!
//! Mainly this is used to:
//!
//! - Create a new project
//! - AddUserToProject
//! - GetProjectCollections
//! - GetProject
//! - Destroy / Delete Project
//!
use super::utils::*;
use crate::database;
use crate::database::connection::Database;
use crate::database::models::auth::{Project, User, UserPermission};
use crate::database::models::collection::Collection;
use crate::error::ArunaError;
use aruna_rust_api::api::storage::models::v1::{
    ProjectOverview, ProjectPermission, ProjectPermissionDisplayName, User as gRPCUser,
};
use aruna_rust_api::api::storage::services::v1::{
    AddUserToProjectRequest, AddUserToProjectResponse, CreateProjectRequest, CreateProjectResponse,
    DestroyProjectRequest, DestroyProjectResponse, EditUserPermissionsForProjectRequest,
    EditUserPermissionsForProjectResponse, GetAllUserPermissionsForProjectResponse,
    GetProjectRequest, GetProjectResponse, GetProjectsRequest, GetProjectsResponse,
    GetUserPermissionsForProjectRequest, GetUserPermissionsForProjectResponse,
    RemoveUserFromProjectRequest, RemoveUserFromProjectResponse, UpdateProjectRequest,
    UpdateProjectResponse, UserWithProjectPermissions,
};
use chrono::Utc;
use diesel::result::Error;
use diesel::sql_types::Uuid;
use diesel::{delete, insert_into, prelude::*, sql_query, update};
use std::str::FromStr;

impl Database {
    /// Creates a new project in the database. Adds the creating user to the project with admin permissions.
    /// At least one user must be present per project.
    ///
    /// ## Arguments
    ///
    /// * request: CreateProjectRequest: Contains information about the new project
    /// * user_id: diesel_ulid::DieselUlid : who created this project ?
    ///
    /// ## Returns
    ///
    /// * Result<CreateProjectRequest, ArunaError>: Returns the UUID of the new project
    ///
    pub fn create_project(
        &self,
        request: CreateProjectRequest,
        user_id: diesel_ulid::DieselUlid,
    ) -> Result<CreateProjectResponse, ArunaError> {
        use crate::database::schema::projects::dsl::*;
        use crate::database::schema::user_permissions::dsl as user_perm;
        use diesel::result::Error as dError;

        // Validate project name against regex schema
        if !NAME_SCHEMA.is_match(request.name.as_str()) {
            return Err(ArunaError::InvalidRequest(
                "Invalid project name. Only ^[\\w~\\-.]+$ characters allowed.".to_string(),
            ));
        }

        // Create a new uuid for the project
        let project_id = diesel_ulid::DieselUlid::generate();

        // Create db project struct
        let project = Project {
            id: project_id,
            name: request.name,
            description: request.description,
            flag: 0, // For now this should be 0, additional flag option will come later
            created_at: Utc::now().naive_utc(),
            created_by: user_id,
        };

        // Create user permissions for the "creator"
        // Every project should have at least one user
        let user_permission = UserPermission {
            id: diesel_ulid::DieselUlid::generate(),
            user_id,
            user_right: crate::database::models::enums::UserRights::ADMIN,
            project_id,
        };

        // Execute db insert_into
        self.pg_connection
            .get()?
            .transaction::<_, dError, _>(|conn| {
                insert_into(projects).values(&project).execute(conn)?;
                insert_into(user_perm::user_permissions)
                    .values(&user_permission)
                    .execute(conn)
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
    /// * user_id: diesel_ulid::DieselUlid : who added the user ?
    ///
    /// ## Returns
    ///
    /// * Result<AddUserToProjectResponse, ArunaError>: Placeholder, currently empty
    ///
    pub fn add_user_to_project(
        &self,
        request: AddUserToProjectRequest,
        _user_id: diesel_ulid::DieselUlid,
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
        let new_id = diesel_ulid::DieselUlid::generate();
        // Create database permission struct
        let user_permission = UserPermission {
            id: new_id,
            user_id: diesel_ulid::DieselUlid::from_str(&grpc_perm.user_id)?,
            user_right: map_permissions(grpc_perm.permission()).ok_or_else(|| {
                ArunaError::InvalidRequest("User permissions are required".to_string())
            })?,
            project_id: diesel_ulid::DieselUlid::from_str(&request.project_id)?,
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

    /// Queries a project and returns basic information about it.
    ///
    /// ## Arguments
    ///
    /// * request: GetProjectRequest: Which user should be added, which permissions should this user have ?
    /// * user_id: diesel_ulid::DieselUlid : who added the user ?
    ///
    /// ## Returns
    ///
    /// * Result<GetProjectResponse, ArunaError>: Overview for a specific project
    ///
    pub fn get_project(
        &self,
        request: GetProjectRequest,
        _user_id: diesel_ulid::DieselUlid,
    ) -> Result<GetProjectResponse, ArunaError> {
        use crate::database::schema::collections::dsl::*;
        use crate::database::schema::projects::dsl::*;
        use crate::database::schema::user_permissions::dsl::*;
        use diesel::result::Error as dError;
        // Get project_id
        let p_id = diesel_ulid::DieselUlid::from_str(&request.project_id)?;

        // Execute db query
        let project_info = self.pg_connection.get()?.transaction::<Option<(
            Project,
            Option<Vec<diesel_ulid::DieselUlid>>,
            Vec<diesel_ulid::DieselUlid>,
        )>, dError, _>(|conn| {
            // Query project from database
            let project_info = projects
                .filter(crate::database::schema::projects::id.eq(p_id))
                .first::<Project>(conn)
                .optional()?;

            // Check if project_info is some
            match project_info {
                // If is_some
                Some(p_info) => {
                    // Query collection_ids
                    let colls = collections
                        .filter(crate::database::schema::collections::project_id.eq(p_id))
                        .select(crate::database::schema::collections::id)
                        .load::<diesel_ulid::DieselUlid>(conn)
                        .optional()?;
                    // Query user_ids -> Should not be optional, every project should have at least one user_permission
                    let usrs = user_permissions
                        .filter(crate::database::schema::user_permissions::project_id.eq(p_id))
                        .select(crate::database::schema::user_permissions::user_id)
                        .load::<diesel_ulid::DieselUlid>(conn)?;
                    Ok(Some((p_info, colls, usrs)))
                }
                // If is_none return none
                None => Ok(None),
            }
        })?;

        // Map result to Project_overview
        let map_project = match project_info {
            // If project_info is some
            Some((project_info, coll_ids, user_ids)) => {
                let mapped_col_ids = match coll_ids {
                    Some(c_id) => c_id.iter().map(|elem| elem.to_string()).collect::<Vec<_>>(),
                    None => Vec::new(),
                };
                Some(ProjectOverview {
                    id: project_info.id.to_string(),
                    name: project_info.name,
                    description: project_info.description,
                    collection_ids: mapped_col_ids,
                    user_ids: user_ids
                        .iter()
                        .map(|elem| elem.to_string())
                        .collect::<Vec<_>>(),
                })
            }
            // Return None if project does not exist
            None => None,
        };

        // Return empty response
        Ok(GetProjectResponse {
            project: map_project,
        })
    }

    /// Queries a project_id by collection_id
    ///
    /// ## Arguments
    ///
    /// * collection_uuid: diesel_ulid::DieselUlid : who added the user ?
    ///
    /// ## Returns
    ///
    /// * Result<diesel_ulid::DieselUlid, ArunaError>: ProjectUUID
    ///
    pub fn get_project_id_by_collection_id(
        &self,
        collection_uuid: diesel_ulid::DieselUlid,
    ) -> Result<diesel_ulid::DieselUlid, ArunaError> {
        use crate::database::schema::collections::dsl::*;
        use diesel::result::Error as dError;

        // Execute db query
        Ok(self
            .pg_connection
            .get()?
            .transaction::<diesel_ulid::DieselUlid, dError, _>(|conn| {
                // Query project from database
                Ok(collections
                    .filter(crate::database::schema::collections::id.eq(collection_uuid))
                    .first::<Collection>(conn)?
                    .project_id)
            })?)
    }

    /// Queries all projects and returns basic information about them.
    ///
    /// ## Arguments
    ///
    /// * request: GetProjectsRequest: Which user should be added, which permissions should this user have ?
    /// * user_id: diesel_ulid::DieselUlid : who added the user ?
    ///
    /// ## Returns
    ///
    /// * Result<GetProjectsResponse, ArunaError>: List of all available projects
    ///
    pub fn get_projects(
        &self,
        _request: GetProjectsRequest,
        _user_id: diesel_ulid::DieselUlid,
    ) -> Result<GetProjectsResponse, ArunaError> {
        use crate::database::schema::collections::dsl::*;
        use crate::database::schema::projects::dsl::*;
        use crate::database::schema::user_permissions::dsl::*;
        use diesel::result::Error as dError;

        // Execute db query
        let project_infos = self.pg_connection.get()?.transaction::<Vec<(
            Project,
            Vec<diesel_ulid::DieselUlid>,
            Vec<diesel_ulid::DieselUlid>,
        )>, dError, _>(|conn| {
            // Query project from database
            let project_infos = projects.load::<Project>(conn).optional()?;

            // Check if project_info is some
            match project_infos {
                // If is_some
                Some(p_infos) => {
                    // Query all collection_ids
                    let all_colls = collections.load::<Collection>(conn).optional()?;
                    // Query all user_ids
                    let usrs = user_permissions.load::<UserPermission>(conn).optional()?;

                    Ok(p_infos
                        .iter()
                        .map(|project| {
                            let pcols_ids = if let Some(acoll) = &all_colls {
                                let mut retvec = Vec::new();
                                for col in acoll {
                                    if col.project_id == project.id {
                                        retvec.push(col.id);
                                    }
                                }
                                retvec
                            } else {
                                Vec::new()
                            };

                            let userperm = if let Some(uperm) = &usrs {
                                let mut retvec = Vec::new();
                                for perm in uperm {
                                    if perm.project_id == project.id {
                                        retvec.push(perm.user_id);
                                    }
                                }
                                retvec
                            } else {
                                Vec::new()
                            };
                            (project.clone(), pcols_ids, userperm)
                        })
                        .collect::<Vec<_>>())
                }
                // If is_none return none
                None => Ok(Vec::new()),
            }
        })?;

        // Map result to Project_overview
        let map_projects = project_infos
            .iter()
            .map(|pinfo| ProjectOverview {
                id: pinfo.0.id.to_string(),
                name: pinfo.0.name.to_string(),
                description: pinfo.0.description.to_string(),
                collection_ids: pinfo.1.iter().map(|c| c.to_string()).collect::<Vec<_>>(),
                user_ids: pinfo.2.iter().map(|c| c.to_string()).collect::<Vec<_>>(),
            })
            .collect::<Vec<_>>();
        // Return empty response
        Ok(GetProjectsResponse {
            projects: map_projects,
        })
    }

    /// Deletes a project, for this the project needs to be empty. No collections should be associated.
    /// This needs project admin permissions
    ///
    /// ## Arguments
    ///
    /// * request: DestroyProjectRequest: Which project should be deleted / destroyed
    /// * user_id: diesel_ulid::DieselUlid : who added the user ?
    ///
    /// ## Returns
    ///
    /// * Result<DestroyProjectResponse, ArunaError>: Placeholder, currently empty
    ///
    pub fn destroy_project(
        &self,
        request: DestroyProjectRequest,
        _user_id: diesel_ulid::DieselUlid,
    ) -> Result<DestroyProjectResponse, ArunaError> {
        use crate::database::schema::collections::dsl::*;
        use crate::database::schema::projects::dsl::*;
        use crate::database::schema::user_permissions::dsl::*;

        // Get project_id
        let p_id = diesel_ulid::DieselUlid::from_str(&request.project_id)?;

        // Execute db query
        self.pg_connection.get()?.transaction::<_, ArunaError, _>(|conn| {
            (match
                collections
                    .filter(crate::database::schema::collections::project_id.eq(p_id))
                    .first::<Collection>(conn)
            {
                Ok(_) =>
                    Err(
                        ArunaError::InvalidRequest(
                            "Cannot delete non empty project, please delete/move all associated collections first".to_string()
                        )
                    ),
                Err(err) => {
                    match err {
                        Error::NotFound => {
                            // Delete project permissions
                            delete(
                                user_permissions.filter(
                                    crate::database::schema::user_permissions::project_id.eq(p_id)
                                )
                            ).execute(conn)?;

                            // Delete project
                            delete(
                                projects.filter(crate::database::schema::projects::id.eq(p_id))
                            ).execute(conn)?;

                            Ok(())
                        }
                        _ => Err(ArunaError::DieselError(err)),
                    }
                }
            })?;

            Ok(())
        })?;

        // Return empty response
        Ok(DestroyProjectResponse {})
    }

    /// Updates a specific project (name + description)
    /// This needs project admin permissions
    ///
    /// ## Arguments
    ///
    /// * request: UpdateProjectRequest: Which project should be updated
    /// * user_id: diesel_ulid::DieselUlid : who updated the project ?
    ///
    /// ## Returns
    ///
    /// * Result<UpdateProjectResponse, ArunaError>: Placeholder, currently empty
    ///
    pub fn update_project(
        &self,
        request: UpdateProjectRequest,
        req_user_id: diesel_ulid::DieselUlid,
    ) -> Result<UpdateProjectResponse, ArunaError> {
        use crate::database::schema::collections::dsl as collections_dsl;
        use crate::database::schema::collections::dsl::collections;
        use crate::database::schema::projects::dsl as projects_dsl;
        use crate::database::schema::projects::dsl::projects;
        use crate::database::schema::user_permissions::dsl as permissions_dsl;
        use crate::database::schema::user_permissions::dsl::user_permissions;

        // Validate project name against regex schema
        if !NAME_SCHEMA.is_match(request.name.as_str()) {
            return Err(ArunaError::InvalidRequest(
                "Invalid project name. Only ^[\\w~\\-.]+$ characters allowed.".to_string(),
            ));
        }

        // Get project_id
        let p_id = diesel_ulid::DieselUlid::from_str(&request.project_id)?;

        // Execute db query
        let project_info = self.pg_connection.get()?.transaction::<Option<(
            Project,
            Vec<diesel_ulid::DieselUlid>,
            Vec<diesel_ulid::DieselUlid>,
        )>, ArunaError, _>(|conn| {
            // Fetch current project info if present in database
            let current_project: Option<Project> = projects
                .filter(projects_dsl::id.eq(&p_id))
                .first::<Project>(conn)
                .optional()?;

            // Check if project_info is some
            match current_project {
                // If is_some -> Update
                Some(p_info) => {
                    // Query collections of project
                    let project_collections = collections
                        .filter(collections_dsl::project_id.eq(p_id))
                        .select(collections_dsl::id)
                        .load::<diesel_ulid::DieselUlid>(conn)?;

                    // Name update is only allowed for empty projects to ensure path consistency
                    if p_info.name != request.name && !project_collections.is_empty() {
                        return Err(ArunaError::InvalidRequest(
                            "Name update only allowed for empty projects".to_string(),
                        ));
                    }

                    // Update project with provided name and description
                    let updated_project = update(projects.filter(projects_dsl::id.eq(p_id)))
                        .set((
                            projects_dsl::name.eq(&request.name),
                            projects_dsl::description.eq(&request.description),
                            projects_dsl::created_by.eq(&req_user_id),
                        ))
                        .get_result::<Project>(conn)?;

                    // Query user_ids -> Should not be optional, every project should have at least one user_permission
                    let project_users = user_permissions
                        .filter(permissions_dsl::project_id.eq(p_id))
                        .select(permissions_dsl::user_id)
                        .load::<diesel_ulid::DieselUlid>(conn)?;

                    Ok(Some((updated_project, project_collections, project_users)))
                }
                // If is_none return none
                None => Ok(None),
            }
        })?;

        // Map result to Project_overview
        let map_project = match project_info {
            // If project_info is some
            Some((project_info, coll_ids, user_ids)) => {
                let mapped_col_ids = coll_ids
                    .iter()
                    .map(|elem| elem.to_string())
                    .collect::<Vec<_>>();

                Some(ProjectOverview {
                    id: project_info.id.to_string(),
                    name: project_info.name,
                    description: project_info.description,
                    collection_ids: mapped_col_ids,
                    user_ids: user_ids
                        .iter()
                        .map(|elem| elem.to_string())
                        .collect::<Vec<_>>(),
                })
            }
            // Return None if project does not exist
            None => None,
        };

        Ok(UpdateProjectResponse {
            project: map_project,
        })
    }

    /// Removes a specific user from a projects
    /// This needs project admin permissions
    ///
    /// ## Arguments
    ///
    /// * request: RemoveUserFromProjectRequest: Which user + project
    /// * _user_id: diesel_ulid::DieselUlid unused
    ///
    /// ## Returns
    ///
    /// * Result<RemoveUserFromProjectResponse, ArunaError>: Placeholder, currently empty
    ///
    pub fn remove_user_from_project(
        &self,
        request: RemoveUserFromProjectRequest,
        _req_user_id: diesel_ulid::DieselUlid,
    ) -> Result<RemoveUserFromProjectResponse, ArunaError> {
        use crate::database::schema::user_permissions::dsl::*;
        // Get project_id
        let p_id = diesel_ulid::DieselUlid::from_str(&request.project_id)?;
        let d_u_id = diesel_ulid::DieselUlid::from_str(&request.user_id)?;

        // Execute db query
        self.pg_connection
            .get()?
            .transaction::<_, ArunaError, _>(|conn| {
                // Delete user_permissions
                delete(
                    user_permissions.filter(
                        crate::database::schema::user_permissions::project_id
                            .eq(&p_id)
                            .and(crate::database::schema::user_permissions::user_id.eq(&d_u_id)),
                    ),
                )
                .execute(conn)?;

                Ok(())
            })?;

        // Return empty response
        Ok(RemoveUserFromProjectResponse {})
    }

    /// Requests a specific user_permission for a project / user combination
    /// This needs project admin permissions
    ///
    /// ## Arguments
    ///
    /// * request: GetUserPermissionsForProjectRequest: Which user + project
    /// * _user_id: diesel_ulid::DieselUlid unused
    ///
    /// ## Returns
    ///
    /// * Result<GetUserPermissionsForProjectResponse, ArunaError>: Placeholder, currently empty
    ///
    pub fn get_user_permission_from_project(
        &self,
        request: GetUserPermissionsForProjectRequest,
        _req_user_id: diesel_ulid::DieselUlid,
    ) -> Result<GetUserPermissionsForProjectResponse, ArunaError> {
        use crate::database::schema::user_permissions::dsl::*;
        use crate::database::schema::users::dsl::*;
        // Get project_id
        let p_id = diesel_ulid::DieselUlid::from_str(&request.project_id)?;
        let d_u_id = diesel_ulid::DieselUlid::from_str(&request.user_id)?;

        // Execute db query
        let permissions = self
            .pg_connection
            .get()?
            .transaction::<(Option<UserPermission>, Option<String>), ArunaError, _>(|conn| {
                let user_name = users
                    .filter(crate::database::schema::users::id.eq(d_u_id))
                    .select(crate::database::schema::users::display_name)
                    .first::<String>(conn)
                    .optional()?;

                let perm = user_permissions
                    .filter(crate::database::schema::user_permissions::user_id.eq(d_u_id))
                    .filter(crate::database::schema::user_permissions::project_id.eq(p_id))
                    .first::<UserPermission>(conn)
                    .optional()?;

                Ok((perm, user_name))
            })?;

        let resp = GetUserPermissionsForProjectResponse {
            user_permission: permissions.0.map(|perm| ProjectPermissionDisplayName {
                user_id: d_u_id.to_string(),
                project_id: p_id.to_string(),
                permission: map_permissions_rev(Some(perm.user_right)),
                display_name: permissions.1.unwrap_or_default(),
            }),
        };

        // Return empty response
        Ok(resp)
    }

    /// Requests all users of a specific project with their associated permission.
    ///
    /// ## Arguments
    ///
    /// * `request: GetUserPermissionsForProjectRequest`:
    ///
    /// ## Returns
    ///
    /// * Result<GetUserPermissionsForProjectResponse, ArunaError>: Placeholder, currently empty
    ///
    pub fn get_all_user_permissions_from_project(
        &self,
        project_uuid: &diesel_ulid::DieselUlid,
    ) -> Result<GetAllUserPermissionsForProjectResponse, ArunaError> {
        use crate::database::schema::user_permissions::dsl::*;
        use crate::database::schema::users::dsl::*;

        // Execute db query
        let permissions = self
            .pg_connection
            .get()?
            .transaction::<Vec<UserWithProjectPermissions>, ArunaError, _>(|conn| {
                // Fetch all permissions associated with the project
                let all_project_permissions = user_permissions
                    .filter(database::schema::user_permissions::project_id.eq(project_uuid))
                    .load::<UserPermission>(conn)?;

                // Fetch user info associated with the project permissions
                let perm_user_ids = all_project_permissions
                    .iter()
                    .map(|permission| permission.user_id)
                    .collect::<Vec<_>>();

                let project_users = users
                    .filter(database::schema::users::id.eq_any(&perm_user_ids))
                    .load::<User>(conn)?;

                // Collect all project users with their associated collection in proto format
                let mut users_with_permission = Vec::new();
                'outer: for user in project_users {
                    let admin_user_perm = sql_query(
                        "SELECT uperm.id, uperm.user_id, uperm.user_right, uperm.project_id
                           FROM user_permissions AS uperm
                           JOIN projects AS p
                           ON p.id = uperm.project_id
                           WHERE uperm.user_id = $1
                           AND p.flag & 1 = 1
                           LIMIT 1",
                    )
                    .bind::<Uuid, _>(user.id)
                    .get_result::<UserPermission>(conn)
                    .optional()?;

                    let proto_user = gRPCUser {
                        id: user.id.to_string(),
                        external_id: user.external_id.to_string(),
                        display_name: user.display_name.to_string(),
                        active: user.active,
                        is_admin: admin_user_perm.is_some(),
                        is_service_account: user.is_service_account,
                        email: user.email.to_string(),
                    };

                    for user_permission in all_project_permissions.iter() {
                        if user_permission.id == user.id {
                            users_with_permission.push(UserWithProjectPermissions {
                                user: Some(proto_user),
                                user_permissions: Some(ProjectPermission {
                                    user_id: user_permission.user_id.to_string(),
                                    project_id: user_permission.project_id.to_string(),
                                    permission: map_permissions_rev(Some(
                                        user_permission.user_right,
                                    )),
                                    service_account: user.is_service_account,
                                }),
                            });

                            continue 'outer;
                        }
                    }

                    return Err(ArunaError::InvalidRequest(format!(
                        "User {} is member of project {} without permission",
                        user.id, project_uuid
                    )));
                }

                Ok(users_with_permission)
            })?;

        // Return empty response
        Ok(GetAllUserPermissionsForProjectResponse { users: permissions })
    }

    /// Modifies the permissions of specific user for a project
    /// This needs project admin permissions
    ///
    /// ## Arguments
    ///
    /// * request: EditUserPermissionsForProjectRequest: Which user + project and new permissions
    /// * _user_id: diesel_ulid::DieselUlid unused
    ///
    /// ## Returns
    ///
    /// * Result<EditUserPermissionsForProjectResponse, ArunaError>: Not used, currently empty
    ///
    pub fn edit_user_permissions_for_project(
        &self,
        request: EditUserPermissionsForProjectRequest,
        _req_user_id: diesel_ulid::DieselUlid,
    ) -> Result<EditUserPermissionsForProjectResponse, ArunaError> {
        use crate::database::schema::user_permissions::dsl::*;
        // Get project_id
        let p_id = diesel_ulid::DieselUlid::from_str(&request.project_id)?;

        if let Some(perm) = request.user_permission {
            let parsed_user_id = diesel_ulid::DieselUlid::from_str(&perm.user_id)?;
            let user_perm = map_permissions(perm.permission()).ok_or_else(|| {
                ArunaError::InvalidRequest("User permissions are required".to_string())
            })?;

            // Execute db query
            self.pg_connection
                .get()?
                .transaction::<_, ArunaError, _>(|conn| {
                    // Update user_permissions
                    update(
                        user_permissions.filter(
                            crate::database::schema::user_permissions::user_id
                                .eq(parsed_user_id)
                                .and(
                                    crate::database::schema::user_permissions::project_id.eq(p_id),
                                ),
                        ),
                    )
                    .set(crate::database::schema::user_permissions::user_right.eq(user_perm))
                    .execute(conn)?;
                    Ok(())
                })?;
        } else {
            return Err(ArunaError::InvalidRequest(
                "User and permissions must be specified".to_string(),
            ));
        }
        // Return empty response
        Ok(EditUserPermissionsForProjectResponse {})
    }
}

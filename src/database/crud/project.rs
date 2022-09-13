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
use crate::api::aruna::api::storage::models::v1::{
    collection_overview::Version as CollectionVersiongRPC, CollectionOverview, CollectionStats,
    LabelOntology, Stats,
};
use crate::api::aruna::api::storage::models::v1::{ProjectOverview, ProjectPermission, Version};
use crate::api::aruna::api::storage::services::v1::{
    AddUserToProjectRequest, AddUserToProjectResponse, CreateProjectRequest, CreateProjectResponse,
    DestroyProjectRequest, DestroyProjectResponse, EditUserPermissionsForProjectRequest,
    EditUserPermissionsForProjectResponse, GetProjectCollectionsRequest,
    GetProjectCollectionsResponse, GetProjectRequest, GetProjectResponse,
    GetUserPermissionsForProjectRequest, GetUserPermissionsForProjectResponse,
    RemoveUserFromProjectRequest, RemoveUserFromProjectResponse, UpdateProjectRequest,
    UpdateProjectResponse,
};
use crate::database::connection::Database;
use crate::database::models::auth::{Project, UserPermission};
use crate::database::models::collection::{
    Collection, CollectionKeyValue, CollectionVersion, RequiredLabel,
};
use crate::database::models::enums::Dataclass;
use crate::database::models::views::CollectionStat;
use crate::error::ArunaError;

use chrono::Utc;
use diesel::{delete, insert_into, prelude::*, update};

impl Database {
    /// Creates a new project in the database. Adds the creating user to the project with admin permissions.
    /// At least one user must be present per project.
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
        use crate::database::schema::user_permissions::dsl as user_perm;
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

        // Create user permissions for the "creator"
        // Every project should have at least one user
        let user_permission = UserPermission {
            id: uuid::Uuid::new_v4(),
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

    /// Get all collections from project
    ///
    /// ## Arguments
    ///
    /// * request: GetProjectCollectionsRequest: Includes project_id to query
    /// * user_id: uuid::Uuid : Not used
    ///
    /// ## Returns
    ///
    /// * Result<GetProjectCollectionsResponse, ArunaError>: List with all collections from project
    ///
    pub fn get_project_collections(
        &self,
        request: GetProjectCollectionsRequest,
        _user_id: uuid::Uuid,
    ) -> Result<GetProjectCollectionsResponse, ArunaError> {
        use crate::database::schema::collection_stats::dsl::*;
        use crate::database::schema::collection_version::dsl::*;
        use crate::database::schema::collections::dsl::*;
        use diesel::result::Error as dError;

        // Parse the project_id
        let parsed_project_id = uuid::Uuid::parse_str(&request.project_id)?;

        // Execute db query
        let result = self.pg_connection.get()?.transaction::<Option<
            Vec<(
                Collection,
                Option<Vec<CollectionKeyValue>>,
                Option<CollectionStat>,
                Option<Vec<RequiredLabel>>,
                Option<CollectionVersion>,
            )>,
        >, dError, _>(|conn| {
            // Query collections
            let colls = collections
                .filter(project_id.eq(parsed_project_id))
                .load::<Collection>(conn)
                .optional()?;

            // Check if collections is Some()
            match colls {
                Some(coll) => {
                    // Create a new vector to return
                    let mut return_vec = Vec::new();

                    // Query db for each element
                    for elem in coll {
                        // Get all key_values (labels/hooks)
                        let key_value: Option<Vec<CollectionKeyValue>> =
                            CollectionKeyValue::belonging_to(&elem)
                                .load::<CollectionKeyValue>(conn)
                                .optional()?;

                        // Get collection stats from materialized view
                        let stats: Option<CollectionStat> = collection_stats
                            .filter(crate::database::schema::collection_stats::dsl::id.eq(elem.id))
                            .first::<CollectionStat>(conn)
                            .optional()?;

                        // Get all required labels / ontology
                        let required_lbl = RequiredLabel::belonging_to(&elem)
                            .load::<RequiredLabel>(conn)
                            .optional()?;

                        // Check if a version exists
                        let version = match elem.version_id {
                            Some(vid) => collection_version
                                .filter(
                                    crate::database::schema::collection_version::dsl::id.eq(vid),
                                )
                                .first::<CollectionVersion>(conn)
                                .optional()?,
                            None => None,
                        };

                        // Add all above to return vector
                        return_vec.push((elem, key_value, stats, required_lbl, version));
                    }
                    // Return the vector
                    Ok(Some(return_vec))
                }
                // If isNone -> Return None
                None => Ok(None),
            }
        })?;

        // Map db result to collection overview
        let to_collection_overview = match result {
            Some(elements) => elements
                .iter()
                .map(|(coll, keyvalue, stats, req_label, vers)| {
                    // Map labels / hooks
                    let (label, hook) = match keyvalue {
                        Some(kv) => from_key_values(kv.to_vec()),
                        None => (Vec::new(), Vec::new()),
                    };

                    // Map ontology
                    let map_req_label = req_label.as_ref().map(|rlbl| LabelOntology {
                        required_label_keys: rlbl
                            .iter()
                            .map(|rq_lbl| rq_lbl.label_key.clone())
                            .collect::<Vec<_>>(),
                    });

                    // Map statistics
                    let map_stats = stats.as_ref().map(|stats| CollectionStats {
                        object_stats: Some(Stats {
                            count: stats.object_count,
                            acc_size: stats.size,
                        }),
                        object_group_count: stats.object_group_count,
                        last_updated: Some(
                            naivedatetime_to_prost_time(stats.last_updated).unwrap_or_default(),
                        ),
                    });

                    // Map collection_version
                    let map_version = match vers {
                        Some(v) => Some(CollectionVersiongRPC::SemanticVersion(Version {
                            major: v.major as i32,
                            minor: v.minor as i32,
                            patch: v.patch as i32,
                        })),
                        None => Some(CollectionVersiongRPC::Latest(true)),
                    };

                    // Construct the final return construct
                    CollectionOverview {
                        id: coll.id.to_string(),
                        name: coll.name.clone(),
                        description: coll.description.clone(),
                        labels: label,
                        hooks: hook,
                        label_ontology: map_req_label,
                        created: Some(
                            naivedatetime_to_prost_time(coll.created_at).unwrap_or_default(),
                        ),
                        stats: map_stats,
                        is_public: match coll.dataclass {
                            Some(dclass) => dclass == Dataclass::PUBLIC,
                            None => false,
                        },
                        version: map_version,
                    }
                })
                .collect::<Vec<_>>(),
            None => Vec::new(),
        };

        // Return the list with collection overviews
        Ok(GetProjectCollectionsResponse {
            collection: to_collection_overview,
        })
    }

    /// Queries a project and returns basic information about it.
    ///
    /// ## Arguments
    ///
    /// * request: GetProjectRequest: Which user should be added, which permissions should this user have ?
    /// * user_id: uuid::Uuid : who added the user ?
    ///
    /// ## Returns
    ///
    /// * Result<AddUserToProjectResponse, ArunaError>: Placeholder, currently empty
    ///
    pub fn get_project(
        &self,
        request: GetProjectRequest,
        _user_id: uuid::Uuid,
    ) -> Result<GetProjectResponse, ArunaError> {
        use crate::database::schema::collections::dsl::*;
        use crate::database::schema::projects::dsl::*;
        use crate::database::schema::user_permissions::dsl::*;
        use diesel::result::Error as dError;
        // Get project_id
        let p_id = uuid::Uuid::parse_str(&request.project_id)?;

        // Execute db query
        let project_info = self.pg_connection.get()?.transaction::<Option<(
            Project,
            Option<Vec<uuid::Uuid>>,
            Vec<uuid::Uuid>,
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
                        .load::<uuid::Uuid>(conn)
                        .optional()?;
                    // Query user_ids -> Should not be optional, every project should have at least one user_permission
                    let usrs = user_permissions
                        .filter(crate::database::schema::user_permissions::project_id.eq(p_id))
                        .select(crate::database::schema::user_permissions::user_id)
                        .load::<uuid::Uuid>(conn)?;
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

    /// Deletes a project, for this the project needs to be empty. No collections should be associated.
    /// This needs project admin permissions
    ///
    /// ## Arguments
    ///
    /// * request: DestroyProjectRequest: Which project should be deleted / destroyed
    /// * user_id: uuid::Uuid : who added the user ?
    ///
    /// ## Returns
    ///
    /// * Result<DestroyProjectResponse, ArunaError>: Placeholder, currently empty
    ///
    pub fn destroy_project(
        &self,
        request: DestroyProjectRequest,
        _user_id: uuid::Uuid,
    ) -> Result<DestroyProjectResponse, ArunaError> {
        use crate::database::schema::collections::dsl::*;
        use crate::database::schema::projects::dsl::*;
        use crate::database::schema::user_permissions::dsl::*;
        // Get project_id
        let p_id = uuid::Uuid::parse_str(&request.project_id)?;

        // Execute db query
        self.pg_connection.get()?.transaction::<_, ArunaError, _>(|conn| {
            // Check if a collection is still present
            collections
                .filter(crate::database::schema::collections::project_id.eq(p_id))
                .first::<Collection>(conn)
                .optional()?
                .ok_or_else(||
                    ArunaError::InvalidRequest(
                        "Cannot delete non empty project, please delete/move all associated collections first".to_string()
                    )
                )?;

            // Delete user_permissions
            delete(
                user_permissions.filter(
                    crate::database::schema::user_permissions::project_id.eq(p_id)
                )
            ).execute(conn)?;
            // Delete project
            delete(projects.filter(crate::database::schema::projects::id.eq(p_id))).execute(conn)?;
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
    /// * user_id: uuid::Uuid : who updated the project ?
    ///
    /// ## Returns
    ///
    /// * Result<UpdateProjectResponse, ArunaError>: Placeholder, currently empty
    ///
    pub fn update_project(
        &self,
        request: UpdateProjectRequest,
        req_user_id: uuid::Uuid,
    ) -> Result<UpdateProjectResponse, ArunaError> {
        use crate::database::schema::collections::dsl::*;
        use crate::database::schema::projects::dsl::*;
        use crate::database::schema::user_permissions::dsl::*;
        use diesel::result::Error as dError;
        // Get project_id
        let p_id = uuid::Uuid::parse_str(&request.project_id)?;

        // Execute db query
        let project_info = self.pg_connection.get()?.transaction::<Option<(
            Project,
            Option<Vec<uuid::Uuid>>,
            Vec<uuid::Uuid>,
        )>, dError, _>(|conn| {
            // Update the project and return the updated project
            let project_info =
                update(projects.filter(crate::database::schema::projects::id.eq(p_id)))
                    .set((
                        crate::database::schema::projects::name.eq(request.name),
                        crate::database::schema::projects::description.eq(request.description),
                        crate::database::schema::projects::created_by.eq(req_user_id),
                    ))
                    .get_result(conn)
                    .optional()?;
            // Check if project_info is some
            match project_info {
                // If is_some
                Some(p_info) => {
                    // Query collection_ids
                    let colls = collections
                        .filter(crate::database::schema::collections::project_id.eq(p_id))
                        .select(crate::database::schema::collections::id)
                        .load::<uuid::Uuid>(conn)
                        .optional()?;
                    // Query user_ids -> Should not be optional, every project should have at least one user_permission
                    let usrs = user_permissions
                        .filter(crate::database::schema::user_permissions::project_id.eq(p_id))
                        .select(crate::database::schema::user_permissions::user_id)
                        .load::<uuid::Uuid>(conn)?;
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
    /// * _user_id: uuid::Uuid unused
    ///
    /// ## Returns
    ///
    /// * Result<RemoveUserFromProjectResponse, ArunaError>: Placeholder, currently empty
    ///
    pub fn remove_user_from_project(
        &self,
        request: RemoveUserFromProjectRequest,
        _req_user_id: uuid::Uuid,
    ) -> Result<RemoveUserFromProjectResponse, ArunaError> {
        use crate::database::schema::user_permissions::dsl::*;
        // Get project_id
        let p_id = uuid::Uuid::parse_str(&request.project_id)?;
        let d_u_id = uuid::Uuid::parse_str(&request.user_id)?;

        // Execute db query
        self.pg_connection
            .get()?
            .transaction::<_, ArunaError, _>(|conn| {
                // Delete user_permissions
                delete(
                    user_permissions.filter(
                        crate::database::schema::user_permissions::project_id
                            .eq(p_id)
                            .and(crate::database::schema::user_permissions::user_id.eq(d_u_id)),
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
    /// * _user_id: uuid::Uuid unused
    ///
    /// ## Returns
    ///
    /// * Result<GetUserPermissionsForProjectResponse, ArunaError>: Placeholder, currently empty
    ///
    pub fn get_userpermission_from_project(
        &self,
        request: GetUserPermissionsForProjectRequest,
        _req_user_id: uuid::Uuid,
    ) -> Result<GetUserPermissionsForProjectResponse, ArunaError> {
        use crate::database::schema::user_permissions::dsl::*;
        // Get project_id
        let p_id = uuid::Uuid::parse_str(&request.project_id)?;
        let d_u_id = uuid::Uuid::parse_str(&request.user_id)?;

        // Execute db query
        let permissions = self
            .pg_connection
            .get()?
            .transaction::<Option<UserPermission>, diesel::result::Error, _>(|conn| {
                user_permissions
                    .filter(crate::database::schema::user_permissions::user_id.eq(d_u_id))
                    .filter(crate::database::schema::user_permissions::project_id.eq(p_id))
                    .first::<UserPermission>(conn)
                    .optional()
            })?;

        let resp = GetUserPermissionsForProjectResponse {
            user_permission: permissions.map(|perm| ProjectPermission {
                user_id: d_u_id.to_string(),
                project_id: p_id.to_string(),
                permission: map_permissions_rev(Some(perm.user_right)),
            }),
        };

        // Return empty response
        Ok(resp)
    }

    /// Modifies the permissions of specific user for a project
    /// This needs project admin permissions
    ///
    /// ## Arguments
    ///
    /// * request: EditUserPermissionsForProjectRequest: Which user + project and new permissions
    /// * _user_id: uuid::Uuid unused
    ///
    /// ## Returns
    ///
    /// * Result<EditUserPermissionsForProjectResponse, ArunaError>: Not used, currently empty
    ///
    pub fn edit_user_permissions_for_project(
        &self,
        request: EditUserPermissionsForProjectRequest,
        _req_user_id: uuid::Uuid,
    ) -> Result<EditUserPermissionsForProjectResponse, ArunaError> {
        use crate::database::schema::user_permissions::dsl::*;
        // Get project_id
        let p_id = uuid::Uuid::parse_str(&request.project_id)?;

        if let Some(perm) = request.user_permission {
            let parsed_user_id = uuid::Uuid::parse_str(&perm.user_id)?;
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

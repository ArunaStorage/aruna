use super::authz::Authz;
use crate::database::{connection::Database, models::enums::UserRights};
use crate::error::ArunaError;
use aruna_rust_api::api::internal::v1::{GetTokenFromSecretRequest, GetTokenFromSecretResponse};
use aruna_rust_api::api::{
    internal::v1::{
        internal_authorize_service_server::InternalAuthorizeService, Authorization,
        AuthorizeRequest, AuthorizeResponse, GetSecretRequest, GetSecretResponse, IdType,
    },
    storage::models::v1::{ResourceAction, ResourceType},
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;

use chrono::Utc;
use std::sync::Arc;
use tokio::task;
use tonic::{Code, Status};

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(InternalAuthorizeServiceImpl);

// Permission list...
//  - Project
//      - Create: AOS Admin
//      - Append: ---
//      - Update: Project Admin
//      - Read:   Project Read
//      - Delete: Project Admin
//  - Collection
//      - Create: Project Write
//      - Append: ---
//      - Update: Project/Collection Write
//      - Read:   Project/Collection Read
//      - Delete: Project Admin (force) | Project/Collection Write
//  - Object
//      - Create: Project/Collection Append
//      - Append: Project/Collection Append (Same as create/update?)
//      - Update: Project/Collection Append
//      - Read:   Project/Collection Read
//      - Delete: Project/Collection Admin (force) | Project/Collection Append
//  - ObjectGroup
//      - Create: Project/Collection Append
//      - Append: Project/Collection Append (Same as create/update?)
//      - Update: Project/Collection Append
//      - Read:   Project/Collection Read
//      - Delete: Project/Collection Write
//  - All (Not yet implemented.)
#[tonic::async_trait]
impl InternalAuthorizeService for InternalAuthorizeServiceImpl {
    async fn authorize(
        &self,
        request: tonic::Request<AuthorizeRequest>,
    ) -> Result<tonic::Response<AuthorizeResponse>, tonic::Status> {
        // Consume gRPC request into its parts
        let (grpc_metadata, _, inner_request) = request.into_parts();

        // Extract and convert request fields
        let resource_type = ResourceType::from_i32(inner_request.resource)
            .ok_or_else(|| ArunaError::InvalidRequest("Invalid resource type".to_string()))?;
        let resource_action = ResourceAction::from_i32(inner_request.resource_action)
            .ok_or_else(|| ArunaError::InvalidRequest("Invalid resource action".to_string()))?;

        if resource_type == ResourceType::Unspecified {
            return Err(Status::new(
                Code::InvalidArgument,
                "Unspecified resource type is not allowed",
            ));
        } else if resource_action == ResourceAction::Unspecified {
            return Err(Status::new(
                Code::InvalidArgument,
                "Unspecified resource action is not allowed",
            ));
        }

        let identifier = match inner_request.identifier {
            Some(id) => id,
            None => {
                return Err(Status::new(
                    Code::InvalidArgument,
                    "Resource identifier has to be provided",
                ))
            }
        };

        // Process only special case -> Project Create
        if resource_type == ResourceType::Project && resource_action == ResourceAction::Create {
            return Ok(tonic::Response::new(AuthorizeResponse {
                ok: self.authz.admin_authorize(&grpc_metadata).await.is_ok(),
            }));
        }

        let authorized = if identifier.idtype == IdType::Uuid as i32 {
            // Parse provided resource id (should be the object group ulid)
            let resource_ulid = DieselUlid::from_str(&identifier.name).map_err(ArunaError::from)?;

            // Authorize depending on resource type/action combination
            match resource_type {
                ResourceType::Unspecified | ResourceType::All => {
                    return Err(Status::new(
                        Code::InvalidArgument,
                        "resource type {resource_type} is not supported",
                    ))
                }
                ResourceType::Project => match resource_action {
                    ResourceAction::Unspecified => false, // This arm should never be processed
                    ResourceAction::Create => false,      // This arm should never be processed
                    ResourceAction::Append => false,      // Invalid type/action combination
                    ResourceAction::Update | ResourceAction::Delete => self
                        .authz
                        .project_authorize(&grpc_metadata, resource_ulid, UserRights::ADMIN, true)
                        .await
                        .is_ok(),
                    ResourceAction::Read => self
                        .authz
                        .project_authorize(&grpc_metadata, resource_ulid, UserRights::READ, true)
                        .await
                        .is_ok(),
                },
                ResourceType::Collection => match resource_action {
                    ResourceAction::Unspecified => false, // This arm should never be processed
                    ResourceAction::Create => self
                        .authz
                        .project_authorize(&grpc_metadata, resource_ulid, UserRights::WRITE, true)
                        .await
                        .is_ok(),
                    ResourceAction::Append => false, // Invalid type/action combination
                    ResourceAction::Update | ResourceAction::Delete => self
                        .authz
                        .collection_authorize(&grpc_metadata, resource_ulid, UserRights::WRITE)
                        .await
                        .is_ok(),
                    ResourceAction::Read => self
                        .authz
                        .collection_authorize(&grpc_metadata, resource_ulid, UserRights::READ)
                        .await
                        .is_ok(),
                },
                ResourceType::ObjectGroup => {
                    // Fetch collection id of object group for authorization
                    let database_clone = self.database.clone();
                    let collection_ulid = task::spawn_blocking(move || {
                        database_clone.get_object_group_collection_id(&resource_ulid)
                    })
                    .await
                    .map_err(ArunaError::from)??;

                    match resource_action {
                        ResourceAction::Unspecified => false,
                        ResourceAction::Create
                        | ResourceAction::Append
                        | ResourceAction::Update => self
                            .authz
                            .collection_authorize(
                                &grpc_metadata,
                                collection_ulid,
                                UserRights::APPEND,
                            )
                            .await
                            .is_ok(),
                        ResourceAction::Read => self
                            .authz
                            .collection_authorize(&grpc_metadata, collection_ulid, UserRights::READ)
                            .await
                            .is_ok(),
                        ResourceAction::Delete => self
                            .authz
                            .collection_authorize(
                                &grpc_metadata,
                                collection_ulid,
                                UserRights::WRITE,
                            )
                            .await
                            .is_ok(),
                    }
                }
                ResourceType::Object => {
                    // Fetch collection ids (all references withput revisions) and authorize until success or ids exhausted
                    let database_clone = self.database.clone();
                    let collection_ulids = task::spawn_blocking(move || {
                        database_clone.get_references(&resource_ulid, false)
                    })
                    .await
                    .map_err(ArunaError::from)??
                    .references
                    .into_iter()
                    .map(|reference| {
                        DieselUlid::from_str(&reference.collection_id).map_err(ArunaError::from)
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                    let mut authed = false;
                    for collection_ulid in collection_ulids {
                        authed = match resource_action {
                            ResourceAction::Unspecified => false,
                            ResourceAction::Create
                            | ResourceAction::Append
                            | ResourceAction::Update
                            | ResourceAction::Delete => self
                                .authz
                                .collection_authorize(
                                    &grpc_metadata,
                                    collection_ulid,
                                    UserRights::APPEND,
                                )
                                .await
                                .is_ok(),
                            ResourceAction::Read => self
                                .authz
                                .collection_authorize(
                                    &grpc_metadata,
                                    collection_ulid,
                                    UserRights::READ,
                                )
                                .await
                                .is_ok(),
                        };

                        if authed {
                            break;
                        }
                    }
                    authed
                }
            }
        } else if identifier.idtype == IdType::Path as i32 {
            return Err(Status::new(
                Code::Unimplemented,
                "path authorization not yet implemented",
            ));

            /*
            //Note: Should only be viable for Objects?
            // Extract provided credentials.
            let _auth_creds = match inner_request.authorization {
                Some(creds) => creds,
                None => {
                    return Err(Status::new(
                        Code::InvalidArgument,
                        "Authorization credentials have to be provided for path",
                    ))
                }
            };

            // Try to fetch project id and collection id
            let database_clone = self.database.clone();
            let (project_ulid, collection_ulid_option) = task::spawn_blocking(move || {
                database_clone.get_project_collection_ids_by_path(&identifier.name, false)
            })
            .await
            .map_err(ArunaError::from)??;

            // Authorize depending which ids could be parsed from path
            match collection_ulid_option {
                Some(collection_ulid) => {
                    // Validate permission with queried collection id
                    self.authz
                        .collection_authorize(&grpc_metadata, collection_ulid, needed_permission)
                        .await
                        .is_ok()
                }
                None => {
                    // Validate permission with queried project id
                    self.authz
                        .collection_authorize(&grpc_metadata, project_ulid, needed_permission)
                        .await
                        .is_ok()
                }
            }
            */
        } else {
            return Err(Status::new(
                Code::InvalidArgument,
                "Invalid identifier type",
            ));
        };

        // Create and return gRPC response
        Ok(tonic::Response::new(AuthorizeResponse { ok: authorized }))
    }

    /// Re-generate the access secret associated with the provided access key.
    ///
    /// ## Arguments:
    ///
    ///
    /// ## Returns:
    ///
    ///
    /// ## Behaviour:
    ///
    async fn get_secret(
        &self,
        request: tonic::Request<GetSecretRequest>,
    ) -> Result<tonic::Response<GetSecretResponse>, tonic::Status> {
        // Consume gRPC request
        let inner_request = request.into_inner();

        // Extract AccessKey which is (everytime?) the token id
        let token_id = diesel_ulid::DieselUlid::from_str(&inner_request.accesskey)
            .map_err(ArunaError::from)?;

        // Fetch token from database only by its id
        let database_clone = self.database.clone();
        let api_token = task::spawn_blocking(move || database_clone.get_api_token_by_id(&token_id))
            .await
            .map_err(ArunaError::from)??;

        // Check if token is not expired
        if api_token
            .expires_at
            .ok_or_else(|| {
                tonic::Status::new(
                    Code::InvalidArgument,
                    format!("Token {token_id} has no expiry date"),
                )
            })?
            .timestamp()
            < Utc::now().timestamp()
        {
            return Err(tonic::Status::new(
                Code::InvalidArgument,
                format!("Token {token_id} is expired"),
            ));
        }

        // Return gRPC response
        Ok(tonic::Response::new(GetSecretResponse {
            authorization: Some(Authorization {
                secretkey: api_token.secretkey,
                accesskey: api_token.id.to_string(),
            }),
        }))
    }

    ///ToDo: Rust Doc
    async fn get_token_from_secret(
        &self,
        _request: tonic::Request<GetTokenFromSecretRequest>,
    ) -> Result<tonic::Response<GetTokenFromSecretResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented."))
    }
}

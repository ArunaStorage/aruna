use crate::caching::cache::Cache;
use crate::database::dsls::object_dsl::{KeyValues, ObjectWithRelations};
use crate::database::enums::{DataClass, ObjectMapping};
use crate::{auth::permission_handler::PermissionHandler, database::enums::DbPermissionLevel};
use aruna_rust_api::api::storage::models::v2::generic_resource::Resource;
use aruna_rust_api::api::storage::models::v2::PermissionLevel;
use aruna_rust_api::api::storage::services::v2::{
    GetResourcesRequest, GetResourcesResponse, RequestResourceAccessRequest,
    RequestResourceAccessResponse, ResourceWithPermission,
};
use aruna_rust_api::api::storage::{
    models::v2::GenericResource,
    services::v2::{
        search_service_server::SearchService, GetResourceRequest, GetResourceResponse,
        SearchResourcesRequest, SearchResourcesResponse,
    },
};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use postgres_types::Json;
use std::str::FromStr;
use std::sync::Arc;
use tonic::Status;

use crate::{
    auth::structs::Context,
    middlelayer::db_handler::DatabaseHandler,
    search::meilisearch_client::{MeilisearchClient, MeilisearchIndexes, ObjectDocument},
    utils::conversions::get_token_from_md,
};

crate::impl_grpc_server!(SearchServiceImpl, search_client: Arc<MeilisearchClient>);

#[tonic::async_trait]
impl SearchService for SearchServiceImpl {
    ///ToDo: Rust Doc
    async fn search_resources(
        &self,
        request: tonic::Request<SearchResourcesRequest>,
    ) -> Result<tonic::Response<SearchResourcesResponse>, Status> {
        log_received!(&request);

        // Consumer gRPC request into its parts
        let (_, _, inner_request) = request.into_parts();

        // NO AUTHORIZATION:
        // Search results are always redacted for PRIVATE
        // and this search function is a PUBLIC endpoint ON PURPOSE
        // to make everything FINDABLE

        // Check if: 0 < limit <= 100
        if (inner_request.limit < 1) || (inner_request.limit > 100) {
            return Err(Status::invalid_argument("Limit must be between 1 and 100"));
        }

        // Search meilisearch index
        let (objects, estimated_total) = tonic_internal!(
            self.search_client
                .query_generic_stuff::<ObjectDocument>(
                    &MeilisearchIndexes::OBJECT.to_string(), // Currently only one index is used for all resources
                    &inner_request.query,
                    &inner_request.filter,
                    inner_request.limit as usize,
                    inner_request.offset as usize,
                )
                .await,
            "Query search failed"
        );

        // Convert search to proto resources
        let mut proto_resources = vec![];
        for hit in objects {
            proto_resources.push(GenericResource {
                resource: Some(tonic_internal!(
                    Resource::try_from(hit),
                    "Search result to proto conversion failed"
                )),
            })
        }

        // last_index? Offset or offset+hits length?
        let last_index = inner_request.offset + proto_resources.len() as i64;

        // Create response and return
        let response = SearchResourcesResponse {
            resources: proto_resources,
            estimated_total: estimated_total as i64,
            last_index,
        };

        return_with_log!(response);
    }

    ///ToDo: Rust Doc
    async fn get_resource(
        &self,
        request: tonic::Request<GetResourceRequest>,
    ) -> Result<tonic::Response<GetResourceResponse>, Status> {
        log_received!(&request);

        // Consumer gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Validate format of provided id
        let resource_ulid = tonic_invalid!(
            DieselUlid::from_str(&inner_request.resource_id),
            "Invalid resource id format"
        );

        let (object_plus, permission) = if request_metadata.get("Authorization").is_some() {
            // Extract token and check permissions with empty context
            let token = tonic_auth!(
                get_token_from_md(&request_metadata),
                "Token extraction failed"
            );

            // Check permissions
            let ctx = Context::res_ctx(resource_ulid, DbPermissionLevel::READ, true);
            let user = tonic_auth!(
                self.authorizer.check_permissions(&token, vec![ctx]).await,
                "Permission denied"
            );
            let object = self
                .cache
                .get_object(&resource_ulid)
                .ok_or_else(|| Status::not_found("Object not found"))?;
            let user = self
                .cache
                .get_user(&user)
                .ok_or_else(|| Status::not_found("User not found"))?;
            let mapping_perm = user
                .attributes
                .0
                .permissions
                .get(&resource_ulid)
                .ok_or_else(|| Status::not_found("Permissions not found"));
            match mapping_perm {
                Ok(perm) => {
                    let permission = match *perm {
                        ObjectMapping::OBJECT(perm) => perm.into(),
                        ObjectMapping::COLLECTION(perm) => perm.into(),
                        ObjectMapping::DATASET(perm) => perm.into(),
                        ObjectMapping::PROJECT(perm) => perm.into(),
                    };
                    (object, permission)
                }
                Err(_) => {
                    let mut permission = PermissionLevel::Read;
                    for (id, perm) in user.attributes.0.permissions.clone() {
                        let all_subs = self.cache.get_subresources(&id).unwrap_or_default(); // This is empty if unwrap fails -> should not affect anything
                        if all_subs.iter().contains(&id) {
                            let tmp_perm: DbPermissionLevel = match perm {
                                ObjectMapping::OBJECT(perm) => perm,
                                ObjectMapping::COLLECTION(perm) => perm,
                                ObjectMapping::DATASET(perm) => perm,
                                ObjectMapping::PROJECT(perm) => perm,
                            };
                            permission = tmp_perm.into();
                            break;
                        }
                    }
                    (object, permission)
                }
            }
        } else {
            // Get Object from cache
            let mut object_plus = self
                .cache
                .get_object(&resource_ulid)
                .ok_or_else(|| Status::not_found("Object not found"))?;

            // Check if object metadata is publicly available
            match object_plus.object.data_class {
                DataClass::PUBLIC => {}
                DataClass::PRIVATE => {
                    // SPECIFIC private operations OTHER THAN strip labels
                    // Remove created by
                    object_plus.object.created_by = DieselUlid::default();
                    // Endpoint redaction
                    object_plus.object.endpoints = Json(DashMap::default());
                }
                _ => return Err(Status::invalid_argument("Resource is not public")),
            }

            // Strip infos
            let stripped_labels = object_plus
                .object
                .key_values
                .0
                 .0
                .into_iter()
                .filter(|kv| kv.key.contains("app.aruna-storage"))
                .filter(|kv| kv.key.contains("private"))
                .collect::<Vec<_>>();

            object_plus.object.key_values = Json(KeyValues(stripped_labels));
            object_plus.object.endpoints = Json(DashMap::default());
            (object_plus, PermissionLevel::Read)
        };

        // Convert to proto resource
        let generic_object: Resource = object_plus.into();

        // Create response and return with log
        let response = GetResourceResponse {
            resource: Some(ResourceWithPermission {
                resource: Some(GenericResource {
                    resource: Some(generic_object),
                }),
                permission: permission.into(),
            }),
        };

        return_with_log!(response);
    }

    async fn get_resources(
        &self,
        request: tonic::Request<GetResourcesRequest>,
    ) -> tonic::Result<tonic::Response<GetResourcesResponse>> {
        log_received!(&request);

        // Consumer gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Validate format of provided id
        let resource_ids = tonic_invalid!(
            inner_request
                .resource_ids
                .into_iter()
                .map(|id| DieselUlid::from_str(&id)
                    .map_err(|_| tonic::Status::invalid_argument("Invalid id")))
                .collect::<Result<Vec<DieselUlid>, Status>>()
                .clone(),
            "Invalid resource id format"
        );

        let objects = if request_metadata.get("Authorization").is_some() {
            // Extract token and check permissions with empty context
            let token = tonic_auth!(
                get_token_from_md(&request_metadata),
                "Token extraction failed"
            );

            // Check permissions
            let ctx = resource_ids
                .iter()
                .map(|id| Context::res_ctx(*id, DbPermissionLevel::READ, true))
                .collect();
            let user = tonic_auth!(
                self.authorizer.check_permissions(&token, ctx).await,
                "Permission denied"
            );
            let mut objects: Vec<(ObjectWithRelations, PermissionLevel)> = Vec::new();
            for id in resource_ids {
                let object = self
                    .cache
                    .get_object(&id)
                    .ok_or_else(|| Status::not_found("Object not found"))?;
                let user = self
                    .cache
                    .get_user(&user)
                    .clone()
                    .ok_or_else(|| Status::not_found("User not found"))?;
                let mapping_perm = user
                    .attributes
                    .0
                    .permissions
                    .get(&id)
                    .ok_or_else(|| Status::not_found("No permissions found"));
                match mapping_perm {
                    Ok(explicit_perms) => {
                        let permission = match *explicit_perms {
                            ObjectMapping::OBJECT(perm) => perm.into(),
                            ObjectMapping::COLLECTION(perm) => perm.into(),
                            ObjectMapping::DATASET(perm) => perm.into(),
                            ObjectMapping::PROJECT(perm) => perm.into(),
                        };
                        objects.push((object, permission));
                    }
                    Err(_) => {
                        let mut permission = PermissionLevel::Read;
                        for (id, perm) in user.attributes.0.permissions.clone() {
                            let all_subs = self.cache.get_subresources(&id).unwrap_or_default();
                            if all_subs.iter().contains(&id) {
                                let tmp_perm: DbPermissionLevel = match perm {
                                    ObjectMapping::OBJECT(perm) => perm,
                                    ObjectMapping::COLLECTION(perm) => perm,
                                    ObjectMapping::DATASET(perm) => perm,
                                    ObjectMapping::PROJECT(perm) => perm,
                                };
                                permission = tmp_perm.into();
                                break;
                            }
                        }
                        objects.push((object, permission));
                    }
                };
            }
            objects
        } else {
            let mut objects: Vec<(ObjectWithRelations, PermissionLevel)> = Vec::new();
            for id in resource_ids {
                // Get Object from cache
                let mut object_plus = self
                    .cache
                    .get_object(&id)
                    .ok_or_else(|| Status::not_found("Object not found"))?;

                // Check if object metadata is publicly available
                match object_plus.object.data_class {
                    DataClass::PUBLIC => {}
                    DataClass::PRIVATE => {
                        // SPECIFIC private operations OTHER THAN strip labels
                        // Remove created by
                        object_plus.object.created_by = DieselUlid::default();
                        // Endpoint redaction
                        object_plus.object.endpoints = Json(DashMap::default());
                    }
                    _ => return Err(Status::invalid_argument("Resource is not public")),
                }

                // Strip infos
                let stripped_labels = object_plus
                    .object
                    .key_values
                    .0
                     .0
                    .into_iter()
                    .filter(|kv| kv.key.contains("app.aruna-storage"))
                    .filter(|kv| kv.key.contains("private"))
                    .collect::<Vec<_>>();

                object_plus.object.key_values = Json(KeyValues(stripped_labels));
                object_plus.object.endpoints = Json(DashMap::default());
                objects.push((object_plus, PermissionLevel::Read));
            }
            objects
        };

        // Convert resources
        let resources = objects
            .into_iter()
            .map(|(object, permission)| {
                Ok::<ResourceWithPermission, Status>(ResourceWithPermission {
                    resource: Some(GenericResource {
                        resource: Some(object.into()),
                    }),
                    permission: permission.into(),
                })
            })
            .collect::<Result<Vec<ResourceWithPermission>, Status>>()?;

        // Create response and return with log
        let response = GetResourcesResponse { resources };
        return_with_log!(response);
    }

    async fn request_resource_access(
        &self,
        request: tonic::Request<RequestResourceAccessRequest>,
    ) -> tonic::Result<tonic::Response<RequestResourceAccessResponse>> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        let resource_ulid = tonic_invalid!(
            DieselUlid::from_str(&inner_request.resource_id),
            "ULID conversion error"
        );

        // Extract token from request and check permissions
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token authentication error"
        );

        let ctx = Context::default(); // User only needs to be activated
        let user_ulid = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        // Create personal notification to request access
        tonic_internal!(
            self.database_handler
                .request_resource_access(user_ulid, resource_ulid)
                .await,
            "Failed to request resource access"
        );

        // Create response and return with log
        let response = RequestResourceAccessResponse {};
        return_with_log!(response);
    }
}

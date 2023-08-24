use crate::caching::cache::Cache;
use crate::database::dsls::object_dsl::KeyValues;
use crate::database::enums::DataClass;
use crate::{auth::permission_handler::PermissionHandler, database::enums::DbPermissionLevel};
use aruna_rust_api::api::storage::models::v2::generic_resource::Resource;
use aruna_rust_api::api::storage::{
    models::v2::GenericResource,
    services::v2::{
        search_service_server::SearchService, GetPublicResourceRequest, GetPublicResourceResponse,
        SearchResourcesRequest, SearchResourcesResponse,
    },
};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
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
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract token and check permissions with empty context
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token extraction failed"
        );

        let _ = tonic_auth!(
            self.authorizer
                .check_permissions(&token, vec![Context::default()])
                .await,
            "Permission denied"
        );

        // Check if: 0 < limit <= 100
        if inner_request.limit < 1 || inner_request.limit > 100 {
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
    async fn get_public_resource(
        &self,
        request: tonic::Request<GetPublicResourceRequest>,
    ) -> Result<tonic::Response<GetPublicResourceResponse>, Status> {
        log_received!(&request);

        // Consumer gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract token and check permissions with empty context
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token extraction failed"
        );

        // Validate format of provided id
        let resource_ulid = tonic_invalid!(
            DieselUlid::from_str(&inner_request.resource_id),
            "Invalid resource id format"
        );

        // Check permissions
        let ctx = Context::res_ctx(resource_ulid, DbPermissionLevel::READ, true);
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Permission denied"
        );

        // Get Object from cache
        let mut object_plus = self
            .cache
            .get_object(&resource_ulid)
            .ok_or_else(|| Status::not_found("Object not found"))?;

        // Check if object metadata is publicly available
        match object_plus.object.data_class {
            DataClass::PUBLIC | DataClass::PRIVATE => {}
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

        // Convert to proto resource
        let generic_object: Resource = tonic_invalid!(object_plus.try_into(), "Invalid object");

        // Create response and return with log
        let response = GetPublicResourceResponse {
            resources: Some(GenericResource {
                resource: Some(generic_object),
            }),
        };

        return_with_log!(response);
    }
}

use crate::auth::permission_handler::PermissionHandler;
use crate::caching::cache::Cache;
use aruna_rust_api::api::storage::{
    models::v2::GenericResource,
    services::v2::{
        search_service_server::SearchService, GetPublicResourceRequest, GetPublicResourceResponse,
        SearchResourcesRequest, SearchResourcesResponse,
    },
};
use std::sync::Arc;

use crate::{
    auth::structs::Context,
    middlelayer::db_handler::DatabaseHandler,
    search::meilisearch_client::{MeilisearchClient, MeilisearchIndexes, ObjectDocument},
    utils::conversions::get_token_from_md,
};

crate::impl_grpc_server!(SearchServiceImpl, search_client: MeilisearchClient);

#[tonic::async_trait]
impl SearchService for SearchServiceImpl {
    ///ToDo: Rust Doc
    async fn search_resources(
        &self,
        request: tonic::Request<SearchResourcesRequest>,
    ) -> Result<tonic::Response<SearchResourcesResponse>, tonic::Status> {
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
                .check_permissions(&token, vec![Context::default()]),
            "Permission denied"
        )
        .ok_or(tonic::Status::invalid_argument("Missing user id"))?;

        // Check if: 0 < limit <= 100
        if inner_request.limit <= 0 || inner_request.limit > 100 {
            return Err(tonic::Status::invalid_argument(
                "Limit must be between 0 and 100",
            ));
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

        // Convert search in proto resources
        let proto_resources: Vec<GenericResource> = objects
            .into_iter()
            .map(|od| GenericResource {
                resource: Some(od.into()),
            })
            .collect();

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
        _request: tonic::Request<GetPublicResourceRequest>,
    ) -> Result<tonic::Response<GetPublicResourceResponse>, tonic::Status> {
        todo!()
    }
}

use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::database::enums::DbPermissionLevel;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::relations_request_types::ModifyRelations;
use crate::search::meilisearch_client::MeilisearchClient;
use crate::search::meilisearch_client::ObjectDocument;
use crate::utils::grpc_utils::get_token_from_md;
use crate::utils::search_utils;
use aruna_rust_api::api::storage::services::v2::relations_service_server::RelationsService;
use aruna_rust_api::api::storage::services::v2::GetHierarchyRequest;
use aruna_rust_api::api::storage::services::v2::GetHierarchyResponse;
use aruna_rust_api::api::storage::services::v2::ModifyRelationsRequest;
use aruna_rust_api::api::storage::services::v2::ModifyRelationsResponse;
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;
use tonic::Result;

crate::impl_grpc_server!(RelationsServiceImpl, search_client: Arc<MeilisearchClient>);

#[tonic::async_trait]
impl RelationsService for RelationsServiceImpl {
    /// ModifyRelation
    ///
    /// Status: BETA
    ///
    /// Modifies all relations to / from a resource
    async fn modify_relations(
        &self,
        request: tonic::Request<ModifyRelationsRequest>,
    ) -> Result<tonic::Response<ModifyRelationsResponse>, tonic::Status> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = ModifyRelations(request.into_inner());

        let (resource, labels_info) = tonic_invalid!(
            self.database_handler
                .get_resource(request)
                .await,
            "Request not valid"
        );
        tonic_auth!(
            self.authorizer
                .check_permissions(&token, labels_info.resources_to_check)
                .await,
            "Unauthorized"
        );

        let object = tonic_internal!(
            self.database_handler
                .modify_relations(
                    resource,
                    labels_info.relations_to_add,
                    labels_info.relations_to_remove
                )
                .await,
            "Database error"
        );

        self.cache.upsert_object(&object.object.id, object.clone());

        // Add or update object in search index
        search_utils::update_search_index(
            &self.search_client,
            &self.cache,
            vec![ObjectDocument::from(object.object.clone())],
        )
        .await;

        return_with_log!(ModifyRelationsResponse {});
    }

    /// GetHierarchy
    ///
    /// Status: BETA
    ///
    /// Gets all downstream hierarchy relations from a resource
    async fn get_hierarchy(
        &self,
        request: tonic::Request<GetHierarchyRequest>,
    ) -> Result<tonic::Response<GetHierarchyResponse>, tonic::Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let id = tonic_invalid!(
            DieselUlid::from_str(&request.into_inner().resource_id),
            "Invalid id"
        );
        let ctx = Context::res_ctx(id, DbPermissionLevel::READ, true);
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let graph = tonic_invalid!(self.cache.get_hierarchy(&id), "Invalid id");
        let result = GetHierarchyResponse { graph: Some(graph) };
        return_with_log!(result);
    }
}

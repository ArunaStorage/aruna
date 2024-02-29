use crate::auth::permission_handler::{PermissionCheck, PermissionHandler};
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::caching::structs::ObjectWrapper;
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::database::enums::DbPermissionLevel;
use crate::middlelayer::create_request_types::CreateRequest;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::delete_request_types::DeleteRequest;
use crate::middlelayer::snapshot_request_types::SnapshotRequest;
use crate::middlelayer::update_request_types::{
    DataClassUpdate, DescriptionUpdate, KeyValueUpdate, LicenseUpdate, NameUpdate,
};
use crate::search::meilisearch_client::{MeilisearchClient, ObjectDocument};
use crate::utils::grpc_utils::{get_id_and_ctx, get_token_from_md, query, IntoGenericInner};
use crate::utils::search_utils;
use aruna_rust_api::api::storage::models::v2::{generic_resource, Collection};
use aruna_rust_api::api::storage::services::v2::collection_service_server::CollectionService;
use aruna_rust_api::api::storage::services::v2::{
    CreateCollectionRequest, CreateCollectionResponse, DeleteCollectionRequest,
    DeleteCollectionResponse, GetCollectionRequest, GetCollectionResponse, GetCollectionsRequest,
    GetCollectionsResponse, SnapshotCollectionRequest, SnapshotCollectionResponse,
    UpdateCollectionAuthorsRequest, UpdateCollectionAuthorsResponse,
    UpdateCollectionDataClassRequest, UpdateCollectionDataClassResponse,
    UpdateCollectionDescriptionRequest, UpdateCollectionDescriptionResponse,
    UpdateCollectionKeyValuesRequest, UpdateCollectionKeyValuesResponse,
    UpdateCollectionLicensesRequest, UpdateCollectionLicensesResponse, UpdateCollectionNameRequest,
    UpdateCollectionNameResponse, UpdateCollectionTitleRequest, UpdateCollectionTitleResponse,
};
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use std::str::FromStr;
use std::sync::Arc;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(CollectionServiceImpl, search_client: Arc<MeilisearchClient>);

#[tonic::async_trait]
impl CollectionService for CollectionServiceImpl {
    async fn create_collection(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = CreateRequest::Collection(request.into_inner());
        let mut ctxs = request.get_relation_contexts()?;
        let parent_ctx = tonic_invalid!(
            request
                .get_parent()
                .ok_or(tonic::Status::invalid_argument("Parent missing."))?
                .get_context(),
            "invalid parent"
        );
        ctxs.push(parent_ctx);

        let PermissionCheck {
            user_id, is_proxy, ..
        } = tonic_auth!(
            self.authorizer
                .check_permissions_verbose(&token, ctxs)
                .await,
            "Unauthorized"
        );

        let is_service_account = self
            .cache
            .get_user(&user_id)
            .ok_or_else(|| tonic::Status::not_found("User not found"))?
            .attributes
            .0
            .service_account;
        if is_service_account && (request.get_data_class() != 4) {
            return Err(tonic::Status::invalid_argument(
                "Workspaces have to be claimed for dataclass changes",
            ));
        }

        let (collection, _) = tonic_internal!(
            self.database_handler
                .create_resource(request, user_id, is_proxy,)
                .await,
            "Internal database error"
        );

        // Already done in create_resource
        // self.cache.add_object(collection.clone());

        // Add or update collection in search index
        search_utils::update_search_index(
            &self.search_client,
            &self.cache,
            vec![ObjectDocument::from(collection.object.clone())],
        )
        .await;

        let wrapped = ObjectWrapper {
            object_with_relations: collection.clone(),
            rules: self
                .cache
                .get_rule_bindings(&collection.object.id)
                .ok_or_else(|| tonic::Status::not_found("Object not found"))?,
        };
        let generic_collection: generic_resource::Resource = wrapped.into();

        let response = CreateCollectionResponse {
            collection: Some(generic_collection.into_inner()?),
        };

        return_with_log!(response);
    }

    async fn get_collection(
        &self,
        request: Request<GetCollectionRequest>,
    ) -> Result<Response<GetCollectionResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = request.into_inner();

        let collection_id = tonic_invalid!(
            DieselUlid::from_str(&request.collection_id),
            "ULID conversion error"
        );

        let ctx = Context::res_ctx(collection_id, DbPermissionLevel::READ, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let res = query(&self.cache, &collection_id)?;
        let response = GetCollectionResponse {
            collection: Some(res.into_inner()?),
        };

        return_with_log!(response);
    }

    async fn get_collections(
        &self,
        request: Request<GetCollectionsRequest>,
    ) -> Result<Response<GetCollectionsResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = request.into_inner();

        let (ids, ctxs): (Vec<DieselUlid>, Vec<Context>) = get_id_and_ctx(request.collection_ids)?;

        tonic_auth!(
            self.authorizer.check_permissions(&token, ctxs).await,
            "Unauthorized"
        );

        let res: Result<Vec<Collection>> = ids
            .iter()
            .map(|id| -> Result<Collection> { query(&self.cache, id)?.into_inner() })
            .collect();

        let response = GetCollectionsResponse { collections: res? };

        return_with_log!(response);
    }

    async fn delete_collection(
        &self,
        request: Request<DeleteCollectionRequest>,
    ) -> Result<Response<DeleteCollectionResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = DeleteRequest::Collection(request.into_inner());
        let id = tonic_invalid!(request.get_id(), "Invalid collection id");

        let ctx = Context::res_ctx(id, DbPermissionLevel::ADMIN, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized."
        );

        let updates: Vec<ObjectWithRelations> = tonic_internal!(
            self.database_handler.delete_resource(request).await,
            "Internal database error"
        );

        // Remove deleted resources from search index
        search_utils::remove_from_search_index(
            &self.search_client,
            updates.iter().map(|o| o.object.id).collect_vec(),
        )
        .await;

        let response = DeleteCollectionResponse {};

        return_with_log!(response);
    }

    async fn update_collection_name(
        &self,
        request: Request<UpdateCollectionNameRequest>,
    ) -> Result<Response<UpdateCollectionNameResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = NameUpdate::Collection(request.into_inner());
        let collection_id = tonic_invalid!(request.get_id(), "Invalid collection id.");
        let ctx = Context::res_ctx(collection_id, DbPermissionLevel::WRITE, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let mut collection = tonic_internal!(
            self.database_handler.update_name(request).await,
            "Internal database error."
        );
        self.cache
            .upsert_object(&collection.object.id, collection.clone());
        self.cache.add_stats_to_object(&mut collection);

        // Add or update collection in search index
        search_utils::update_search_index(
            &self.search_client,
            &self.cache,
            vec![ObjectDocument::from(collection.object.clone())],
        )
        .await;

        let rules = self.cache.get_rule_bindings(&collection_id).unwrap_or_default();

        let collection: generic_resource::Resource = ObjectWrapper{
            object_with_relations:collection,
            rules,
        }.into();

        let response = UpdateCollectionNameResponse {
            collection: Some(collection.into_inner()?),
        };
        return_with_log!(response);
    }

    async fn update_collection_description(
        &self,
        request: Request<UpdateCollectionDescriptionRequest>,
    ) -> Result<Response<UpdateCollectionDescriptionResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = DescriptionUpdate::Collection(request.into_inner());
        let collection_id = tonic_invalid!(request.get_id(), "Invalid collection id.");
        let ctx = Context::res_ctx(collection_id, DbPermissionLevel::WRITE, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let mut collection = tonic_internal!(
            self.database_handler.update_description(request).await,
            "Internal database error."
        );
        self.cache
            .upsert_object(&collection.object.id, collection.clone());
        self.cache.add_stats_to_object(&mut collection);

        // Add or update collection in search index
        search_utils::update_search_index(
            &self.search_client,
            &self.cache,
            vec![ObjectDocument::from(collection.object.clone())],
        )
        .await;

        let rules = self.cache.get_rule_bindings(&collection_id).unwrap_or_default();

        let collection: generic_resource::Resource = ObjectWrapper {
            object_with_relations: collection,
            rules,
        }.into();

        let response = UpdateCollectionDescriptionResponse {
            collection: Some(collection.into_inner()?),
        };
        return_with_log!(response);
    }

    async fn update_collection_key_values(
        &self,
        request: Request<UpdateCollectionKeyValuesRequest>,
    ) -> Result<Response<UpdateCollectionKeyValuesResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = KeyValueUpdate::Collection(request.into_inner());
        let collection_id = tonic_invalid!(request.get_id(), "Invalid collection id.");
        let ctx = Context::res_ctx(collection_id, DbPermissionLevel::WRITE, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let mut collection = tonic_internal!(
            self.database_handler.update_keyvals(request).await,
            "Internal database error."
        );
        self.cache
            .upsert_object(&collection.object.id, collection.clone());
        self.cache.add_stats_to_object(&mut collection);

        // Add or update collection in search index
        search_utils::update_search_index(
            &self.search_client,
            &self.cache,
            vec![ObjectDocument::from(collection.object.clone())],
        )
        .await;

        let rules = self.cache.get_rule_bindings(&collection_id).unwrap_or_default();
        let collection: generic_resource::Resource = ObjectWrapper {
            object_with_relations: collection,
            rules,
        }.into();
        let response = UpdateCollectionKeyValuesResponse {
            collection: Some(collection.into_inner()?),
        };
        return_with_log!(response);
    }

    async fn update_collection_data_class(
        &self,
        request: Request<UpdateCollectionDataClassRequest>,
    ) -> Result<Response<UpdateCollectionDataClassResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = DataClassUpdate::Collection(request.into_inner());
        let collection_id = tonic_invalid!(request.get_id(), "Invalid collection id.");
        // Dataclass can only be changed by non-servcieaccounts
        let ctx = Context::res_ctx(collection_id, DbPermissionLevel::WRITE, false);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let mut collection = tonic_internal!(
            self.database_handler.update_dataclass(request).await,
            "Internal database error."
        );
        self.cache
            .upsert_object(&collection.object.id, collection.clone());
        self.cache.add_stats_to_object(&mut collection);

        // Add or update collection in search index
        search_utils::update_search_index(
            &self.search_client,
            &self.cache,
            vec![ObjectDocument::from(collection.object.clone())],
        )
        .await;

        let rules = self.cache.get_rule_bindings(&collection_id).unwrap_or_default();
        let collection: generic_resource::Resource = ObjectWrapper{
            object_with_relations: collection,
            rules,
        }.into();
        let response = UpdateCollectionDataClassResponse {
            collection: Some(collection.into_inner()?),
        };
        return_with_log!(response);
    }

    async fn snapshot_collection(
        &self,
        request: Request<SnapshotCollectionRequest>,
    ) -> Result<Response<SnapshotCollectionResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = SnapshotRequest::Collection(request.into_inner());
        let collection_id = tonic_invalid!(request.get_id(), "Invalid collection id.");
        let ctx = Context::res_ctx(collection_id, DbPermissionLevel::ADMIN, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let (new_id, resources) = tonic_internal!(
            self.database_handler.snapshot(request).await,
            "Internal database error."
        );

        let mut search_update: Vec<ObjectDocument> = vec![];
        for resource in resources {
            self.cache
                .upsert_object(&resource.object.id, resource.clone());
            search_update.push(ObjectDocument::from(resource.object))
        }

        // Add or update collection in search index
        search_utils::update_search_index(&self.search_client, &self.cache, search_update).await;

        let collection: generic_resource::Resource = self
            .cache
            .get_protobuf_object(&new_id)
            .ok_or_else(|| tonic::Status::not_found("Collection not found"))?;

        let response = SnapshotCollectionResponse {
            collection: Some(collection.into_inner()?),
        };
        return_with_log!(response);
    }

    async fn update_collection_licenses(
        &self,
        request: Request<UpdateCollectionLicensesRequest>,
    ) -> Result<Response<UpdateCollectionLicensesResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = LicenseUpdate::Collection(request.into_inner());
        let collection_id = tonic_invalid!(request.get_id(), "Invalid collection id");
        let ctx = Context::res_ctx(collection_id, DbPermissionLevel::WRITE, false);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let mut collection = tonic_invalid!(
            self.database_handler.update_license(request).await,
            "Invalid update license request"
        );
        self.cache
            .upsert_object(&collection.object.id, collection.clone());
        self.cache.add_stats_to_object(&mut collection);

        // Add or update collection in search index
        search_utils::update_search_index(
            &self.search_client,
            &self.cache,
            vec![ObjectDocument::from(collection.object.clone())],
        )
        .await;

        let rules = self.cache.get_rule_bindings(&collection_id).unwrap_or_default();
        let generic_resource: generic_resource::Resource = ObjectWrapper {
            object_with_relations: collection,
            rules,
        }.into();
        let response = UpdateCollectionLicensesResponse {
            collection: Some(generic_resource.into_inner()?),
        };
        return_with_log!(response);
    }

    async fn update_collection_authors(
        &self,
        _request: Request<UpdateCollectionAuthorsRequest>,
    ) -> Result<Response<UpdateCollectionAuthorsResponse>> {
        // TODO
        Err(tonic::Status::unimplemented(
            "Updating collection authors is not yet implemented",
        ))
    }
    async fn update_collection_title(
        &self,
        _request: Request<UpdateCollectionTitleRequest>,
    ) -> Result<Response<UpdateCollectionTitleResponse>> {
        // TODO
        Err(tonic::Status::unimplemented(
            "Updating collection titles is not yet implemented",
        ))
    }
}

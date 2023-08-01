use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::database::enums::DbPermissionLevel;
use crate::middlelayer::create_request_types::CreateRequest;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::delete_request_types::DeleteRequest;
use crate::middlelayer::snapshot_request_types::SnapshotRequest;
use crate::middlelayer::update_request_types::{
    DataClassUpdate, DescriptionUpdate, KeyValueUpdate, NameUpdate,
};
use crate::utils::conversions::get_token_from_md;
use crate::utils::grpc_utils::IntoGenericInner;
use aruna_rust_api::api::storage::models::v2::generic_resource;
use aruna_rust_api::api::storage::services::v2::collection_service_server::CollectionService;
use aruna_rust_api::api::storage::services::v2::{
    CreateCollectionRequest, CreateCollectionResponse, DeleteCollectionRequest,
    DeleteCollectionResponse, GetCollectionRequest, GetCollectionResponse, GetCollectionsRequest,
    GetCollectionsResponse, SnapshotCollectionRequest, SnapshotCollectionResponse,
    UpdateCollectionDataClassRequest, UpdateCollectionDataClassResponse,
    UpdateCollectionDescriptionRequest, UpdateCollectionDescriptionResponse,
    UpdateCollectionKeyValuesRequest, UpdateCollectionKeyValuesResponse,
    UpdateCollectionNameRequest, UpdateCollectionNameResponse,
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(CollectionServiceImpl);

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

        let parent_ctx = tonic_invalid!(
            request
                .get_parent()
                .ok_or(tonic::Status::invalid_argument("Parent missing."))?
                .get_context(),
            "invalid parent"
        );

        let user_id = tonic_auth!(
            self.authorizer
                .check_permissions(&token, vec![parent_ctx])
                .await,
            "Unauthorized"
        )
        .ok_or(tonic::Status::invalid_argument("Missing user id"))?;

        let object_with_rel = tonic_internal!(
            self.database_handler
                .create_resource(request, user_id)
                .await,
            "Internal database error"
        );

        self.cache.add_object(object_with_rel.clone());

        let generic_collection: generic_resource::Resource =
            tonic_invalid!(object_with_rel.try_into(), "Invalid collection");

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

        let res = self
            .cache
            .get_object(&collection_id)
            .ok_or_else(|| tonic::Status::not_found("Collection not found"))?;

        let generic_collection: generic_resource::Resource =
            tonic_invalid!(res.try_into(), "Invalid collection");

        let response = GetCollectionResponse {
            collection: Some(generic_collection.into_inner()?),
        };

        return_with_log!(response);
    }

    async fn get_collections(
        &self,
        _request: Request<GetCollectionsRequest>,
    ) -> Result<Response<GetCollectionsResponse>> {
        todo!()
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

        for o in updates {
            self.cache.update_object(&o.object.id, o.clone());
        }

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

        let collection = tonic_internal!(
            self.database_handler.update_name(request).await,
            "Internal database error."
        );
        self.cache
            .update_object(&collection.object.id, collection.clone());
        let collection: generic_resource::Resource =
            tonic_internal!(collection.try_into(), "Collection conversion error");

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

        let collection = tonic_internal!(
            self.database_handler.update_description(request).await,
            "Internal database error."
        );
        self.cache
            .update_object(&collection.object.id, collection.clone());
        let collection: generic_resource::Resource =
            tonic_internal!(collection.try_into(), "Collection conversion error");

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

        let collection = tonic_internal!(
            self.database_handler.update_keyvals(request).await,
            "Internal database error."
        );
        self.cache
            .update_object(&collection.object.id, collection.clone());
        let collection: generic_resource::Resource =
            tonic_internal!(collection.try_into(), "Collection conversion error");
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
        let ctx = Context::res_ctx(collection_id, DbPermissionLevel::WRITE, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let collection = tonic_internal!(
            self.database_handler.update_dataclass(request).await,
            "Internal database error."
        );
        self.cache
            .update_object(&collection.object.id, collection.clone());
        let collection: generic_resource::Resource =
            tonic_internal!(collection.try_into(), "Collection conversion error");
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

        // this only contains one entry with a collection
        let resources = tonic_internal!(
            self.database_handler.snapshot(request).await,
            "Internal database error."
        );
        for resource in &resources {
            self.cache
                .update_object(&resource.object.id, resource.clone());
        }
        let collection: generic_resource::Resource =
        // First entry is always the snapshot collection
            tonic_internal!(resources[0].clone().try_into(), "Collection conversion error");

        let response = SnapshotCollectionResponse {
            collection: Some(collection.into_inner()?),
        };
        return_with_log!(response);
    }
}

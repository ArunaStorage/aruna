use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::database::enums::DbPermissionLevel;
use crate::middlelayer::clone_request_types::CloneObject;
use crate::middlelayer::create_request_types::CreateRequest;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::delete_request_types::DeleteRequest;
use crate::middlelayer::presigned_url_handler::{PresignedDownload, PresignedUpload};
use crate::middlelayer::update_request_types::UpdateObject;
use crate::search::meilisearch_client::{MeilisearchClient, ObjectDocument};
use crate::utils::conversions::get_token_from_md;
use crate::utils::grpc_utils::{self, get_id_and_ctx, query, IntoGenericInner};
use aruna_rust_api::api::storage::models::v2::{generic_resource, Object};
use aruna_rust_api::api::storage::services::v2::object_service_server::ObjectService;
use aruna_rust_api::api::storage::services::v2::{
    CloneObjectRequest, CloneObjectResponse, CreateObjectRequest, CreateObjectResponse,
    DeleteObjectRequest, DeleteObjectResponse, FinishObjectStagingRequest,
    FinishObjectStagingResponse, GetDownloadUrlRequest, GetDownloadUrlResponse, GetObjectRequest,
    GetObjectResponse, GetObjectsRequest, GetObjectsResponse, GetUploadUrlRequest,
    GetUploadUrlResponse, UpdateObjectRequest, UpdateObjectResponse,
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;
use tonic::{Request, Response, Result, Status};

crate::impl_grpc_server!(ObjectServiceImpl, search_client: Arc<MeilisearchClient>);

#[tonic::async_trait]
impl ObjectService for ObjectServiceImpl {
    async fn create_object(
        &self,
        request: Request<CreateObjectRequest>,
    ) -> Result<Response<CreateObjectResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = CreateRequest::Object(request.into_inner());

        let parent_ctx = tonic_invalid!(
            request
                .get_parent()
                .ok_or(tonic::Status::invalid_argument("Parent missing."))?
                .get_context(),
            "invalid parent"
        );
        let (user_id, _, is_dataproxy) = tonic_auth!(
            self.authorizer
                .check_permissions_verbose(&token, vec![parent_ctx])
                .await,
            "Unauthorized"
        );

        let (object_plus, _) = tonic_internal!(
            self.database_handler
                .create_resource(self.authorizer.clone(), request, user_id, is_dataproxy)
                .await,
            "Internal database error"
        );

        self.cache.add_object(object_plus.clone());

        // Add or update object in search index
        grpc_utils::update_search_index(
            &self.search_client,
            vec![ObjectDocument::from(object_plus.object.clone())],
        )
        .await;

        let generic_object: generic_resource::Resource =
            tonic_invalid!(object_plus.try_into(), "Invalid object");

        let response = CreateObjectResponse {
            object: Some(generic_object.into_inner()?),
        };

        return_with_log!(response);
    }

    async fn get_upload_url(
        &self,
        request: Request<GetUploadUrlRequest>,
    ) -> Result<Response<GetUploadUrlResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = PresignedUpload(request.into_inner());

        let object_id = tonic_invalid!(request.get_id(), "Invalid id");
        let user_id = tonic_auth!(
            self.authorizer
                .check_permissions(
                    &token,
                    vec![Context::res_ctx(object_id, DbPermissionLevel::WRITE, true)]
                )
                .await,
            "Unauthorized"
        );

        let signed_url = tonic_internal!(
            self.database_handler
                .get_presigend_upload(
                    self.cache.clone(),
                    request,
                    self.authorizer.clone(),
                    user_id,
                )
                .await,
            "Error while building presigned url"
        );

        let result = GetUploadUrlResponse { url: signed_url };

        return_with_log!(result);
    }

    async fn get_download_url(
        &self,
        request: Request<GetDownloadUrlRequest>,
    ) -> Result<Response<GetDownloadUrlResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = PresignedDownload(request.into_inner());

        let object_id = tonic_invalid!(request.get_id(), "Invalid id");
        let user_id = tonic_auth!(
            self.authorizer
                .check_permissions(
                    &token,
                    vec![Context::res_ctx(object_id, DbPermissionLevel::READ, true)]
                )
                .await,
            "Unauthorized"
        );

        let signed_url = tonic_internal!(
            self.database_handler
                .get_presigned_download(
                    self.cache.clone(),
                    self.authorizer.clone(),
                    request,
                    user_id,
                )
                .await,
            "Error while building presigned url"
        );

        let result = GetDownloadUrlResponse { url: signed_url };

        return_with_log!(result);
    }

    async fn finish_object_staging(
        &self,
        request: Request<FinishObjectStagingRequest>,
    ) -> Result<Response<FinishObjectStagingResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = request.into_inner();

        let (_, _, is_dataproxy) = tonic_auth!(
            self.authorizer
                .check_permissions_verbose(
                    &token,
                    vec![Context::res_ctx(
                        tonic_invalid!(
                            DieselUlid::from_str(&request.object_id),
                            "Invalid object_id"
                        ),
                        DbPermissionLevel::APPEND,
                        true
                    )]
                )
                .await,
            "Unauthorized"
        );
        if !is_dataproxy {
            let object = self
                .cache
                .get_object(&tonic_invalid!(
                    DieselUlid::from_str(&request.object_id),
                    "Invalid id"
                ))
                .ok_or_else(|| Status::not_found("Object not found"))?;
            let object: generic_resource::Resource =
                tonic_internal!(object.try_into(), "Object conversion error");
            let response = FinishObjectStagingResponse {
                object: Some(object.into_inner()?),
            };
            return_with_log!(response);
        }

        let object = tonic_internal!(
            self.database_handler.finish_object(request).await,
            "Internal database error."
        );

        self.cache.upsert_object(&object.object.id, object.clone());

        // Add or update object in search index
        grpc_utils::update_search_index(
            &self.search_client,
            vec![ObjectDocument::from(object.object.clone())],
        )
        .await;

        let object: generic_resource::Resource =
            tonic_internal!(object.try_into(), "Object conversion error");
        let response = FinishObjectStagingResponse {
            object: Some(object.into_inner()?),
        };
        return_with_log!(response);
    }

    async fn update_object(
        &self,
        request: Request<UpdateObjectRequest>,
    ) -> Result<Response<UpdateObjectResponse>> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );
        let inner = request.into_inner();
        let req = UpdateObject(inner.clone());
        let object_id = tonic_invalid!(req.get_id(), "Invalid object id.");

        let ctx = Context::res_ctx(object_id, DbPermissionLevel::WRITE, true);

        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let (object, new_revision) = tonic_internal!(
            self.database_handler
                .update_grpc_object(self.authorizer.clone(), inner, user_id)
                .await,
            "Internal database error."
        );

        self.cache.upsert_object(&object.object.id, object.clone());

        // Add or update object in search index
        grpc_utils::update_search_index(
            &self.search_client,
            vec![ObjectDocument::from(object.object.clone())],
        )
        .await;

        let object: generic_resource::Resource =
            tonic_internal!(object.try_into(), "Object conversion error");
        let response = UpdateObjectResponse {
            object: Some(object.into_inner()?),
            new_revision,
        };
        return_with_log!(response);
    }

    async fn clone_object(
        &self,
        request: Request<CloneObjectRequest>,
    ) -> Result<Response<CloneObjectResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = CloneObject(request.into_inner());
        let object_id = tonic_invalid!(request.get_object_id(), "Invalid object id");
        let (parent_id, parent_mapping) = tonic_invalid!(request.get_parent(), "Invalid object id");
        let ctx = Context::res_ctx(parent_id, DbPermissionLevel::WRITE, true);
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        let new = tonic_internal!(
            self.database_handler
                .clone_object(&user_id, &object_id, parent_mapping)
                .await,
            "Internal clone object error"
        );
        self.cache.add_object(new.clone());

        // Add or update object in search index
        grpc_utils::update_search_index(
            &self.search_client,
            vec![ObjectDocument::from(new.object.clone())],
        )
        .await;

        let converted: generic_resource::Resource =
            tonic_internal!(new.try_into(), "Conversion error");
        let response = CloneObjectResponse {
            object: Some(converted.into_inner()?),
        };
        return_with_log!(response);
    }

    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = DeleteRequest::Object(request.into_inner());
        let id = tonic_invalid!(request.get_id(), "Invalid object id");

        let ctx = Context::res_ctx(id, DbPermissionLevel::ADMIN, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized."
        );

        let updates: Vec<ObjectWithRelations> = tonic_internal!(
            self.database_handler.delete_resource(request).await,
            "Internal database error"
        );

        let mut search_update: Vec<ObjectDocument> = vec![];
        for o in updates {
            self.cache.remove_object(&o.object.id);
            search_update.push(ObjectDocument::from(o.object))
        }

        // Add or update object(s) in search index
        grpc_utils::update_search_index(&self.search_client, search_update).await;

        let response = DeleteObjectResponse {};

        return_with_log!(response);
    }

    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<GetObjectResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = request.into_inner();

        let object_id = tonic_invalid!(
            DieselUlid::from_str(&request.object_id),
            "ULID conversion error"
        );

        let ctx = Context::res_ctx(object_id, DbPermissionLevel::READ, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let res = self
            .cache
            .get_object(&object_id)
            .ok_or_else(|| Status::not_found("Object not found"))?;

        let generic_object: generic_resource::Resource =
            tonic_invalid!(res.try_into(), "Invalid object");

        let response = GetObjectResponse {
            object: Some(generic_object.into_inner()?),
        };

        return_with_log!(response);
    }

    async fn get_objects(
        &self,
        request: Request<GetObjectsRequest>,
    ) -> Result<Response<GetObjectsResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = request.into_inner();

        let (ids, ctxs): (Vec<DieselUlid>, Vec<Context>) = get_id_and_ctx(request.object_ids)?;

        tonic_auth!(
            self.authorizer.check_permissions(&token, ctxs).await,
            "Unauthorized"
        );

        let res: Result<Vec<Object>> = ids
            .iter()
            .map(|id| -> Result<Object> {
                let obj = query(&self.cache, id)?;
                obj.into_inner()
            })
            .collect();

        let response = GetObjectsResponse { objects: res? };

        return_with_log!(response);
    }
}

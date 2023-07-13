use crate::auth::{Authorizer, ResourcePermission};
use crate::caching::cache::Cache;
use crate::database::connection::Database;
use crate::database::object_dsl::Object;
use crate::utils::conversions::get_token_from_md;
use aruna_rust_api::api::storage::services::v2::create_object_request::Parent;
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
use std::sync::Mutex;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(ObjectServiceImpl);

#[tonic::async_trait]
impl ObjectService for ObjectServiceImpl {
    async fn create_object(
        &self,
        request: Request<CreateObjectRequest>,
    ) -> Result<Response<CreateObjectResponse>> {
        log::info!("Recieved CreateObjectRequest.");
        log::debug!("{:?}", &request);

        let token = get_token_from_md(request.metadata()).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;

        let inner_request = request.into_inner();
        let parent_id = match inner_request.parent {
            Some(parent) => {
                let id = match parent {
                    Parent::ProjectId(id) => id,
                    Parent::CollectionId(id) => id,
                    Parent::DatasetId(id) => id,
                };
                DieselUlid::from_str(&id).map_err(|e| {
                    log::debug!("{}", e);
                    tonic::Status::internal("ULID parsing error")
                })?
            }
            None => return Err(tonic::Status::invalid_argument("Object has no parent")),
        };
        let client = &self.database.get_client().await;

        // let ctx = crate::auth::Context::Object(ResourcePermission {
        //     id: parent_id,
        //     level: todo!(),
        //     allow_sa: todo!(),
        // });

        // match &self.authorizer.check_permissions(&token, ctx) {
        //     _ => false,
        // };

        let create_object = Object {
            id: DieselUlid::generate(),
            // ToDo!
            shared_id: DieselUlid::generate(),
            revision_number: 0,
            name: inner_request.name,
            description: inner_request.description,
            created_at: None,
            content_len: todo!(),
            created_by: todo!(),
            count: todo!(),
            key_values: Json(KeyValues(vec![])),
            object_status: crate::database::enums::ObjectStatus::AVAILABLE,
            data_class: crate::database::enums::DataClass::CONFIDENTIAL,
            object_type: crate::database::enums::ObjectType::OBJECT,
            external_relations: Json(ExternalRelations(vec![])),
            hashes: vec![],
        };
        todo!()
    }
    async fn get_upload_url(
        &self,
        request: Request<GetUploadUrlRequest>,
    ) -> Result<Response<GetUploadUrlResponse>> {
        todo!()
    }
    async fn get_download_url(
        &self,
        request: Request<GetDownloadUrlRequest>,
    ) -> Result<Response<GetDownloadUrlResponse>> {
        todo!()
    }
    async fn finish_object_staging(
        &self,
        request: Request<FinishObjectStagingRequest>,
    ) -> Result<Response<FinishObjectStagingResponse>> {
        todo!()
    }
    async fn update_object(
        &self,
        request: Request<UpdateObjectRequest>,
    ) -> Result<Response<UpdateObjectResponse>> {
        todo!()
    }
    async fn clone_object(
        &self,
        request: Request<CloneObjectRequest>,
    ) -> Result<Response<CloneObjectResponse>> {
        todo!()
    }
    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>> {
        todo!()
    }
    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<GetObjectResponse>> {
        todo!()
    }
    async fn get_objects(
        &self,
        request: Request<GetObjectsRequest>,
    ) -> Result<Response<GetObjectsResponse>> {
        todo!()
    }
}

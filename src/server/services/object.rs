use std::sync::Arc;
use tonic::{Code, Request, Response, Status};
use tonic::transport::Channel;

use crate::api::aruna::api::storage::services::v1::*;
use crate::api::aruna::api::storage::services::v1::object_service_server::ObjectService;
use crate::api::aruna::api::storage::internal::v1::{
    internal_proxy_service_client::InternalProxyServiceClient,
    InitPresignedUploadRequest
};

use crate::database::connection::Database;
use crate::database::models::enums::{Resources, UserRights};

use crate::server::services::authz::{Authz, Context};

///
pub struct ObjectServiceImpl {
    pub database: Arc<Database>,
    pub data_proxy: InternalProxyServiceClient<Channel>,
}

///
impl ObjectServiceImpl {
    pub async fn new(database: Arc<Database>, data_proxy: InternalProxyServiceClient<Channel>) -> Self {
        let collection_service = ObjectServiceImpl {
            database,
            data_proxy
        };

        return collection_service;
    }
}

///
#[tonic::async_trait]
impl ObjectService for ObjectServiceImpl {
    async fn initialize_new_object(
        &self,
        request: Request<InitializeNewObjectRequest>
    ) -> Result<Response<InitializeNewObjectResponse>, Status> {
        todo!()
    }

    async fn get_upload_url(
        &self,
        _request: Request<GetUploadUrlRequest>
    ) -> Result<Response<GetUploadUrlResponse>, Status> {
        todo!()
    }

    async fn finish_object_staging(
        &self, _request: Request<FinishObjectStagingRequest>
    ) -> Result<Response<FinishObjectStagingResponse>, Status> {
        todo!()
    }

    async fn update_object(
        &self,
        _request: Request<UpdateObjectRequest>
    ) -> Result<Response<UpdateObjectResponse>, Status> {
        todo!()
    }

    async fn borrow_object(
        &self,
        _request: Request<BorrowObjectRequest>
    ) -> Result<Response<BorrowObjectResponse>, Status> {
        todo!()
    }

    async fn clone_object(
        &self,
        _request: Request<CloneObjectRequest>
    ) -> Result<Response<CloneObjectResponse>, Status> {
        todo!()
    }

    async fn delete_object(
        &self,
        _request: Request<DeleteObjectRequest>
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        todo!()
    }

    async fn get_object_by_id(
        &self,
        _request: Request<GetObjectByIdRequest>
    ) -> Result<Response<GetObjectByIdResponse>, Status> {
        todo!()
    }

    async fn get_objects(
        &self,
        _request: Request<GetObjectsRequest>
    ) -> Result<Response<GetObjectsResponse>, Status> {
        todo!()
    }

    async fn get_object_history_by_id(
        &self,
        _request: Request<GetObjectHistoryByIdRequest>
    ) -> Result<Response<GetObjectHistoryByIdResponse>, Status> {
        todo!()
    }

    async fn get_download_url(
        &self,
        _request: Request<GetDownloadUrlRequest>
    ) -> Result<Response<GetDownloadUrlResponse>, Status> {
        todo!()
    }

    async fn get_download_links_batch(
        &self,
        _request: Request<GetDownloadLinksBatchRequest>
    ) -> Result<Response<GetDownloadLinksBatchResponse>, Status> {
        todo!()
    }

    type CreateDownloadLinksStreamStream = tonic::Streaming<CreateDownloadLinksStreamResponse>;
    async fn create_download_links_stream(
        &self, _request: Request<CreateDownloadLinksStreamRequest>
    ) -> Result<Response<Self::CreateDownloadLinksStreamStream>, Status> {
        todo!()
    }
}

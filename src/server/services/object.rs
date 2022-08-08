use std::sync::Arc;
use tonic::transport::Channel;
use tonic::{Code, Request, Response, Status};

use crate::api::aruna::api::storage::internal::v1::{
    internal_proxy_service_client::InternalProxyServiceClient, InitPresignedUploadRequest,
};
use crate::api::aruna::api::storage::services::v1::object_service_server::ObjectService;
use crate::api::aruna::api::storage::services::v1::*;

use crate::database::connection::Database;
use crate::database::models::enums::{Resources, UserRights};

use crate::error::ArunaError;
use crate::server::services::authz::{Authz, Context};

///
pub struct ObjectServiceImpl {
    pub database: Arc<Database>,
    pub data_proxy: InternalProxyServiceClient<Channel>,
}

///
impl ObjectServiceImpl {
    pub async fn new(
        database: Arc<Database>,
        data_proxy: InternalProxyServiceClient<Channel>,
    ) -> Self {
        let collection_service = ObjectServiceImpl {
            database,
            data_proxy,
        };

        return collection_service;
    }
}

///
#[tonic::async_trait]
impl ObjectService for ObjectServiceImpl {
    async fn initialize_new_object(
        &self,
        request: Request<InitializeNewObjectRequest>,
    ) -> Result<Response<InitializeNewObjectResponse>, Status> {
        // Check if user is authorized to create objects in this collection
        let collection_id = uuid::Uuid::parse_str(&request.get_ref().collection_id)
            .map_err(|e| ArunaError::from(e))?;

        let creator_id = Authz::authorize(
            self.database.clone(),
            &request.metadata(),
            Context {
                user_right: UserRights::APPEND, // User needs at least append permission to create an object
                resource_type: Resources::COLLECTION, // Creating a new object needs at least collection level permissions
                resource_id: collection_id, // This is the collection uuid in which this object should be created
                admin: false,
            },
        )?;

        // Create mutable data proxy object
        let mut data_proxy_mut = self.data_proxy.clone();

        // Extract request body
        let inner_request = request.into_inner(); // Consumes the gRPC request

        // Try to create object in database with all its assets
        let db_result = self.database.create_object(&inner_request, &creator_id);

        return match db_result {
            Err(_) => Err(Status::new(
                Code::Internal,
                "Failed to create object in database.",
            )),
            Ok((mut response, location)) => {
                let proxy_response = data_proxy_mut
                    .init_presigned_upload(InitPresignedUploadRequest {
                        location: Some(location),
                        multipart: inner_request.multipart.clone(),
                    })
                    .await;

                match proxy_response {
                    Ok(proxy_response) => {
                        //staging_id = uuid!(response.into_inner().upload_id);
                        response.staging_id = proxy_response.into_inner().upload_id; // Consumes gRPC response

                        return Ok(Response::new(response));
                    }
                    Err(_) => Err(Status::new(
                        Code::Unavailable,
                        "Could not retrieve upload id from data proxy server.",
                    )),
                }
            }
        };
    }

    async fn get_upload_url(
        &self,
        _request: Request<GetUploadUrlRequest>,
    ) -> Result<Response<GetUploadUrlResponse>, Status> {
        todo!()
    }

    async fn finish_object_staging(
        &self,
        _request: Request<FinishObjectStagingRequest>,
    ) -> Result<Response<FinishObjectStagingResponse>, Status> {
        todo!()
    }

    async fn update_object(
        &self,
        _request: Request<UpdateObjectRequest>,
    ) -> Result<Response<UpdateObjectResponse>, Status> {
        todo!()
    }

    async fn borrow_object(
        &self,
        _request: Request<BorrowObjectRequest>,
    ) -> Result<Response<BorrowObjectResponse>, Status> {
        todo!()
    }

    async fn clone_object(
        &self,
        _request: Request<CloneObjectRequest>,
    ) -> Result<Response<CloneObjectResponse>, Status> {
        todo!()
    }

    async fn delete_object(
        &self,
        _request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        todo!()
    }

    async fn get_object_by_id(
        &self,
        _request: Request<GetObjectByIdRequest>,
    ) -> Result<Response<GetObjectByIdResponse>, Status> {
        todo!()
    }

    async fn get_objects(
        &self,
        _request: Request<GetObjectsRequest>,
    ) -> Result<Response<GetObjectsResponse>, Status> {
        todo!()
    }

    async fn get_object_history_by_id(
        &self,
        _request: Request<GetObjectHistoryByIdRequest>,
    ) -> Result<Response<GetObjectHistoryByIdResponse>, Status> {
        todo!()
    }

    async fn get_download_url(
        &self,
        _request: Request<GetDownloadUrlRequest>,
    ) -> Result<Response<GetDownloadUrlResponse>, Status> {
        todo!()
    }

    async fn get_download_links_batch(
        &self,
        _request: Request<GetDownloadLinksBatchRequest>,
    ) -> Result<Response<GetDownloadLinksBatchResponse>, Status> {
        todo!()
    }

    type CreateDownloadLinksStreamStream = tonic::Streaming<CreateDownloadLinksStreamResponse>;
    async fn create_download_links_stream(
        &self,
        _request: Request<CreateDownloadLinksStreamRequest>,
    ) -> Result<Response<Self::CreateDownloadLinksStreamStream>, Status> {
        todo!()
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_init_new_object() {}
}

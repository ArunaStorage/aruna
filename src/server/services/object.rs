use std::sync::Arc;
use tokio::task;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};


use crate::api::aruna::api::storage::{
    internal::v1::{
        internal_proxy_service_client::InternalProxyServiceClient,
        InitPresignedUploadRequest, Location, CreatePresignedDownloadRequest, Range
    },
    models::v1::object_location::Location::S3Location,
    services::v1::{
        object_service_server::ObjectService,
        BorrowObjectRequest, BorrowObjectResponse,
        CloneObjectRequest, CloneObjectResponse,
        CreateDownloadLinksStreamRequest, CreateDownloadLinksStreamResponse,
        DeleteObjectRequest, DeleteObjectResponse,
        FinishObjectStagingRequest, FinishObjectStagingResponse,
        GetDownloadLinksBatchRequest, GetDownloadLinksBatchResponse,
        GetDownloadUrlRequest, GetDownloadUrlResponse,
        GetObjectByIdRequest, GetObjectByIdResponse,
        GetObjectHistoryByIdRequest, GetObjectHistoryByIdResponse,
        GetObjectsRequest, GetObjectsResponse,
        GetUploadUrlRequest, GetUploadUrlResponse,
        InitializeNewObjectRequest, InitializeNewObjectResponse,
        UpdateObjectRequest, UpdateObjectResponse
    },
};
use crate::api::aruna::api::storage::services::v1::ObjectWithUrl;

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

        // Generate upload_id for object through data proxy
        let location = Location {
            r#type: S3Location as i32,
            bucket: uuid::Uuid::new_v4().to_string(),
            path: uuid::Uuid::new_v4().to_string()
        };

        let upload_id = data_proxy_mut.init_presigned_upload(
            InitPresignedUploadRequest {
                location: Some(location.clone()),
                multipart: inner_request.multipart.clone(),
            }).await?.into_inner().upload_id;

        // Create Object in database
        let database_clone = self.database.clone();
        let response = task::spawn_blocking(move || {
            database_clone.create_object(&inner_request, &creator_id, &location, upload_id)
        }).await
          .map_err(|join_error| ArunaError::from(join_error))??;

        // Return gRPC response after everything succeeded
        return Ok(Response::new(response));
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
        request: Request<GetObjectByIdRequest>,
    ) -> Result<Response<GetObjectByIdResponse>, Status> {
        // Check if user is authorized to create objects in this collection
        let collection_id = uuid::Uuid::parse_str(&request.get_ref().collection_id)
            .map_err(|e| ArunaError::from(e))?;

        let creator_id = Authz::authorize(
            self.database.clone(),
            &request.metadata(),
            Context {
                user_right: UserRights::READ, // User needs at least append permission to create an object
                resource_type: Resources::COLLECTION, // Creating a new object needs at least collection level permissions
                resource_id: collection_id, // This is the collection uuid in which this object should be created
                admin: false,
            },
        )?;

        // Create mutable data proxy object
        let mut data_proxy_mut = self.data_proxy.clone();

        // Extract request body
        let inner_request = request.into_inner(); // Consumes the gRPC request

        // Get object and its location
        let database_clone = self.database.clone();
        let request_clone = inner_request.clone();
        let (proto_object, proto_location) = task::spawn_blocking(move || {
            database_clone.get_object(&request_clone)
        }).await
          .map_err(|join_error| ArunaError::from(join_error))??;

        //Note: Only request url from data proxy if request.with_url == true
        let response = match &inner_request.with_url {
            true => {
                let data_proxy_request = CreatePresignedDownloadRequest {
                    location: Some(proto_location),
                    range: Some(Range {start: 0, end: proto_object.content_len.clone()})
                };

                GetObjectByIdResponse {
                    object: Some(ObjectWithUrl {
                        object: Some(proto_object),
                        url: data_proxy_mut.create_presigned_download(data_proxy_request).await?.into_inner().url
                    })
                }
            },
            false => GetObjectByIdResponse {
                object: Some(ObjectWithUrl {
                    object: Some(proto_object),
                    url: "".to_string()
                })
            },
        };

        return Ok(Response::new(response));
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

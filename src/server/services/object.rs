use std::sync::Arc;
use tokio::task;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::api::aruna::api::storage::internal::v1::CreatePresignedUploadUrlRequest;
use crate::api::aruna::api::storage::services::v1::{AddLabelToObjectRequest, AddLabelToObjectResponse, GetLatestObjectRevisionRequest, GetLatestObjectRevisionResponse, GetObjectEndpointsRequest, GetObjectEndpointsResponse, GetObjectRevisionsRequest, GetObjectRevisionsResponse, ObjectWithUrl, SetHooksOfObjectRequest, SetHooksOfObjectResponse, Url};
use crate::api::aruna::api::storage::{
    internal::v1::{
        internal_proxy_service_client::InternalProxyServiceClient, CreatePresignedDownloadRequest,
        InitPresignedUploadRequest, Location, Range,
    },
    services::v1::{
        object_service_server::ObjectService, BorrowObjectRequest, BorrowObjectResponse,
        CloneObjectRequest, CloneObjectResponse, CreateDownloadLinksStreamRequest,
        CreateDownloadLinksStreamResponse, DeleteObjectRequest, DeleteObjectResponse,
        FinishObjectStagingRequest, FinishObjectStagingResponse, GetDownloadLinksBatchRequest,
        GetDownloadLinksBatchResponse, GetDownloadUrlRequest, GetDownloadUrlResponse,
        GetObjectByIdRequest, GetObjectByIdResponse, GetObjectsRequest, GetObjectsResponse, GetUploadUrlRequest,
        GetUploadUrlResponse, InitializeNewObjectRequest, InitializeNewObjectResponse,
        UpdateObjectRequest, UpdateObjectResponse,
    },
};

use crate::database::connection::Database;
use crate::database::models::enums::{EndpointType, Resources, UserRights};

use crate::error::ArunaError;
use crate::server::services::authz::{Authz, Context};

///
pub struct ObjectServiceImpl {
    pub database: Arc<Database>,
    pub authz: Arc<Authz>,
    pub data_proxy: InternalProxyServiceClient<Channel>,
}

///
impl ObjectServiceImpl {
    pub async fn new(
        database: Arc<Database>,
        authz: Arc<Authz>,
        data_proxy: InternalProxyServiceClient<Channel>,
    ) -> Self {
        ObjectServiceImpl {
            database,
            authz,
            data_proxy,
        }
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
        let collection_id =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;

        let creator_id = self
            .authz
            .authorize(
                request.metadata(),
                Context {
                    user_right: UserRights::APPEND, // User needs at least append permission to create an object
                    resource_type: Resources::COLLECTION, // Creating a new object needs at least collection level permissions
                    resource_id: collection_id, // This is the collection uuid in which this object should be created
                    admin: false,
                    oidc_context: false,
                    personal: false,
                },
            )
            .await?;

        // Create mutable data proxy object
        let mut data_proxy_mut = self.data_proxy.clone();

        // Extract request body
        let inner_request = request.into_inner(); // Consumes the gRPC request

        // Generate upload_id for object through data proxy
        let location = Location {
            r#type: EndpointType::S3 as i32,
            bucket: uuid::Uuid::new_v4().to_string(),
            path: uuid::Uuid::new_v4().to_string(),
        };

        let upload_id = data_proxy_mut
            .init_presigned_upload(InitPresignedUploadRequest {
                location: Some(location.clone()),
                multipart: inner_request.multipart,
            })
            .await?
            .into_inner()
            .upload_id;

        // Create Object in database
        let database_clone = self.database.clone();
        let response = task::spawn_blocking(move || {
            database_clone.create_object(&inner_request, &creator_id, &location, upload_id)
        })
        .await
        .map_err(ArunaError::from)??;

        // Return gRPC response after everything succeeded
        return Ok(Response::new(response));
    }

    async fn get_upload_url(
        &self,
        request: Request<GetUploadUrlRequest>,
    ) -> Result<Response<GetUploadUrlResponse>, Status> {
        // Check if user is authorized to upload object data in this collection
        let collection_id =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;

        let _creator_id = self
            .authz
            .authorize(
                request.metadata(),
                Context {
                    user_right: UserRights::APPEND, // User needs at least append permission to create an object
                    resource_type: Resources::COLLECTION, // Creating a new object needs at least collection level permissions
                    resource_id: collection_id, // This is the collection uuid in which this object should be created
                    admin: false,
                    oidc_context: false,
                    personal: false,
                },
            )
            .await?;

        // Extract request body
        let inner_request = request.into_inner(); // Consumes the gRPC request

        // Get primary object location
        let object_id = uuid::Uuid::parse_str(&inner_request.object_id).map_err(ArunaError::from)?;
        let location = self.database.get_primary_object_location(&object_id)?;

        // Get upload url through data proxy
        let mut data_proxy_mut = self.data_proxy.clone();
        let upload_url = data_proxy_mut
            .create_presigned_upload_url(CreatePresignedUploadUrlRequest {
                location: Some(location),
                upload_id: inner_request.upload_id, //Note: Can be moved, only used here
            })
            .await?
            .into_inner()
            .url;

        return Ok(Response::new(GetUploadUrlResponse {
            url: Some(Url { url: upload_url }),
        }));
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
        let collection_id =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;

        let _creator_id = self
            .authz
            .authorize(
                request.metadata(),
                Context {
                    user_right: UserRights::READ, // User needs at least append permission to create an object
                    resource_type: Resources::COLLECTION, // Creating a new object needs at least collection level permissions
                    resource_id: collection_id, // This is the collection uuid in which this object should be created
                    admin: false,
                    oidc_context: false,
                    personal: false,
                },
            )
            .await?;

        // Create mutable data proxy object
        let mut data_proxy_mut = self.data_proxy.clone();

        // Extract request body
        let inner_request = request.into_inner(); // Consumes the gRPC request

        // Get object and its location
        let database_clone = self.database.clone();
        let request_clone = inner_request.clone();
        let (proto_object, proto_location) =
            task::spawn_blocking(move || database_clone.get_object(&request_clone))
                .await
                .map_err(ArunaError::from)??;

        //Note: Only request url from data proxy if request.with_url == true
        let response = match &inner_request.with_url {
            true => {
                let data_proxy_request = CreatePresignedDownloadRequest {
                    location: Some(proto_location),
                    range: Some(Range {
                        start: 0,
                        end: proto_object.content_len,
                    }),
                };

                GetObjectByIdResponse {
                    object: Some(ObjectWithUrl {
                        object: Some(proto_object),
                        url: data_proxy_mut
                            .create_presigned_download(data_proxy_request)
                            .await?
                            .into_inner()
                            .url,
                    }),
                }
            }
            false => GetObjectByIdResponse {
                object: Some(ObjectWithUrl {
                    object: Some(proto_object),
                    url: "".to_string(),
                }),
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

    async fn get_object_revisions(&self, _request: Request<GetObjectRevisionsRequest>) -> Result<Response<GetObjectRevisionsResponse>, Status> {
        todo!()
    }

    async fn get_latest_object_revision(&self, _request: Request<GetLatestObjectRevisionRequest>) -> Result<Response<GetLatestObjectRevisionResponse>, Status> {
        todo!()
    }

    async fn get_object_endpoints(&self, _request: Request<GetObjectEndpointsRequest>) -> Result<Response<GetObjectEndpointsResponse>, Status> {
        todo!()
    }

    async fn add_label_to_object(&self, _request: Request<AddLabelToObjectRequest>) -> Result<Response<AddLabelToObjectResponse>, Status> {
        todo!()
    }

    async fn set_hooks_of_object(&self, _request: Request<SetHooksOfObjectRequest>) -> Result<Response<SetHooksOfObjectResponse>, Status> {
        todo!()
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_init_new_object() {}
}

use std::sync::Arc;
use tokio::task;
use tonic::transport::Channel;
use tonic::{Code, Request, Response, Status};

use crate::api::aruna::api::storage::{
    internal::v1::internal_proxy_service_client::InternalProxyServiceClient, internal::v1::*,
    services::v1::object_service_server::ObjectService, services::v1::*,
};

use crate::database::connection::Database;
use crate::database::models::enums::{Resources, UserRights};
use crate::database::models::object::{Endpoint};

use crate::error::ArunaError;
use crate::server::services::authz::{Authz, Context};

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(ObjectServiceImpl, default_endpoint: Endpoint);

///
impl ObjectServiceImpl {
    /// This helper method tries to establish a connection to the default data proxy endpoint defined in the config.
    ///
    /// ## Results
    ///
    /// `InternalProxyServiceClient<Channel>` - Open connection to the default data proxy endpoint
    ///
    /// ## Behaviour
    ///
    /// On success returns the open connection to the data proxy endpoint.
    /// On failure returns an `ArunaError::DataProxyError`.
    ///
    async fn try_connect_default_endpoint(
        &self,
    ) -> Result<InternalProxyServiceClient<Channel>, ArunaError> {
        // Evaluate endpoint url
        let endpoint_url = match &self.default_endpoint.is_public {
            true => &self.default_endpoint.proxy_hostname,
            false => &self.default_endpoint.internal_hostname,
        };

        // Try to establish connection to endpoint
        let data_proxy = InternalProxyServiceClient::connect(endpoint_url.to_string()).await;

        match data_proxy {
            Ok(dp) => Ok(dp),
            Err(_) => Err(ArunaError::DataProxyError(Status::new(
                Code::Internal,
                "Could not connect to default data proxy endpoint",
            ))),
        }
    }

    /// This helper method tries to establish a connection to a specific data proxy endpoint.
    ///
    /// ## Arguments
    ///
    /// `endpoint_uuid` - Unique endpoint id
    ///
    /// ## Results
    ///
    /// `InternalProxyServiceClient<Channel>` - Open connection to the specific data proxy endpoint.
    ///
    /// ## Behaviour
    ///
    /// On success returns the open connection to the specific data proxy endpoint.
    /// On failure returns an `ArunaError::DataProxyError`.
    ///
    async fn try_connect_endpoint(
        &self,
        endpoint_uuid: uuid::Uuid,
    ) -> Result<InternalProxyServiceClient<Channel>, ArunaError> {
        // Get endpoint from database
        let endpoint = self.database.get_endpoint(&endpoint_uuid)?;

        // Evaluate endpoint url
        let endpoint_url = match &endpoint.is_public {
            true => &self.default_endpoint.proxy_hostname,
            false => &self.default_endpoint.internal_hostname,
        };

        // Try to establish connection to endpoint
        let data_proxy = InternalProxyServiceClient::connect(endpoint_url.to_string()).await;

        match data_proxy {
            Ok(dp) => Ok(dp),
            Err(_) => Err(ArunaError::DataProxyError(Status::new(
                Code::Internal,
                "Could not connect to data proxy endpoint",
            ))),
        }
    }

    /// This helper method tries to establish a connection to one of the endpoints associated with the specific object.
    ///
    /// ## Arguments
    ///
    /// `object_uuid` - Unique object id
    ///
    /// ## Results
    ///
    /// `(InternalProxyServiceClient<Channel>, ProtoLocation)` - Open connection to one of the objects data proxy endpoints with its corresponding location
    ///
    /// ## Behaviour
    ///
    /// The first attempt is always made with the primary endpoint of the object. If this fails, the other endpoints are tried in no particular order.
    /// * On success returns the open connection to the internal data proxy with its corresponding location.
    /// * On failure returns an `ArunaError::DataProxyError`.
    ///
    async fn try_connect_object_endpoint(
        &self,
        object_uuid: &uuid::Uuid,
    ) -> Result<(InternalProxyServiceClient<Channel>, Location), ArunaError> {
        // Get primary location with its endpoint from database
        let (location, endpoint) = self
            .database
            .get_primary_object_location_with_endpoint(object_uuid)?;

        // Evaluate endpoint url
        let endpoint_url = match &endpoint.is_public {
            true => &endpoint.proxy_hostname,
            false => &endpoint.internal_hostname,
        };

        // Try to establish connection to endpoint
        let data_proxy = InternalProxyServiceClient::connect(endpoint_url.to_string()).await;

        match data_proxy {
            Ok(dp) => {
                let proto_location = Location {
                    r#type: endpoint.endpoint_type as i32,
                    bucket: location.bucket,
                    path: location.path,
                };
                Ok((dp, proto_location))
            }
            Err(_) => Err(ArunaError::DataProxyError(Status::new(
                Code::Internal,
                "Could not connect to objects primary data proxy endpoint",
            ))),
        }

        /*
        match InternalProxyServiceClient::connect(endpoint_url).await {
            Ok(data_proxy) => (location, data_proxy),
            Err(err) => {
                //ToDo: Try remaining location/endpoint pairs if available.
            }
        }
        */
    }
}

///ToDo: Rust Doc
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

        // Connect to default data proxy endpoint
        let mut data_proxy = self.try_connect_default_endpoint().await?;

        // Extract request body
        let inner_request = request.into_inner(); // Consumes the gRPC request

        // Generate upload_id for object through (currently only default) storage endpoint
        let location = Location {
            r#type: self.default_endpoint.endpoint_type as i32,
            bucket: uuid::Uuid::new_v4().to_string(),
            path: uuid::Uuid::new_v4().to_string(),
        };

        let upload_id = data_proxy
            .init_presigned_upload(InitPresignedUploadRequest {
                location: Some(location.clone()),
                multipart: inner_request.multipart,
            })
            .await?
            .into_inner()
            .upload_id;

        // Create Object in database
        let database_clone = self.database.clone();
        let endpoint_clone = self.default_endpoint.clone();
        let response = task::spawn_blocking(move || {
            database_clone.create_object(
                &inner_request,
                &creator_id,
                &location,
                upload_id,
                endpoint_clone.id,
            )
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
        let object_id =
            uuid::Uuid::parse_str(&inner_request.object_id).map_err(ArunaError::from)?;

        // Try to connect to one of the objects data proxy endpoints (currently only primary location endpoint)
        let (mut data_proxy, location) = self.try_connect_object_endpoint(&object_id).await?;

        // Get upload url through data proxy
        let upload_url = data_proxy
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
        _request: Request<GetObjectByIdRequest>,
    ) -> Result<Response<GetObjectByIdResponse>, Status> {
        todo!()
        /*
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
        */
    }

    async fn get_objects(
        &self,
        _request: Request<GetObjectsRequest>,
    ) -> Result<Response<GetObjectsResponse>, Status> {
        todo!()
    }

    async fn get_object_revisions(
        &self,
        _request: Request<GetObjectRevisionsRequest>,
    ) -> Result<Response<GetObjectRevisionsResponse>, Status> {
        todo!()
    }

    async fn get_latest_object_revision(
        &self,
        _request: Request<GetLatestObjectRevisionRequest>,
    ) -> Result<Response<GetLatestObjectRevisionResponse>, Status> {
        todo!()
    }

    async fn get_object_endpoints(
        &self,
        _request: Request<GetObjectEndpointsRequest>,
    ) -> Result<Response<GetObjectEndpointsResponse>, Status> {
        todo!()
    }

    async fn add_label_to_object(
        &self,
        _request: Request<AddLabelToObjectRequest>,
    ) -> Result<Response<AddLabelToObjectResponse>, Status> {
        todo!()
    }

    async fn set_hooks_of_object(
        &self,
        _request: Request<SetHooksOfObjectRequest>,
    ) -> Result<Response<SetHooksOfObjectResponse>, Status> {
        todo!()
    }

    ///ToDo: Rust Doc
    async fn get_download_url(
        &self,
        request: Request<GetDownloadUrlRequest>,
    ) -> Result<Response<GetDownloadUrlResponse>, Status> {
        // Check if user is authorized to download object data in this collection
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

        // Extract request body
        let inner_request = request.into_inner(); // Consumes the gRPC request

        // Check if request contains valid object id and get object from database
        let object_uuid =
            uuid::Uuid::parse_str(inner_request.object.as_str()).map_err(ArunaError::from)?;
        let object = self.database.get_object_by_id(&object_uuid)?;

        // Connect to one of the objects data proxy endpoints
        let (mut data_proxy, location) = self.try_connect_object_endpoint(&object_uuid).await?;

        // Get download url from data proxy endpoint
        let download_url = data_proxy
            .create_presigned_download(CreatePresignedDownloadRequest {
                location: Some(location),
                range: Some(Range {
                    start: 0,
                    end: object.content_len,
                }),
            })
            .await?
            .into_inner()
            .url;

        Ok(Response::new(GetDownloadUrlResponse {
            url: Some(Url { url: download_url }),
        }))
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

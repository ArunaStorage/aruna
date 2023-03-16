use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Code, Request, Response, Status};

use crate::database::connection::Database;
use crate::database::models::enums::{Resources, UserRights};
use crate::database::models::object::Endpoint;
use crate::error::ArunaError;
use crate::server::services::authz::{Authz, Context};
use crate::server::services::utils::{format_grpc_request, format_grpc_response};
use aruna_rust_api::api::internal::v1::internal_proxy_service_client::InternalProxyServiceClient;
use aruna_rust_api::api::internal::v1::{
    CreatePresignedDownloadRequest, CreatePresignedUploadUrlRequest, FinishPresignedUploadRequest,
    InitPresignedUploadRequest, Location, PartETag, Range,
};
use aruna_rust_api::api::storage::models::v1::{Object, Status as ProtoStatus};
use aruna_rust_api::api::storage::{
    services::v1::object_service_server::ObjectService, services::v1::*,
};

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
        /*
        let endpoint_url = match &self.default_endpoint.is_public {
            true => &self.default_endpoint.proxy_hostname,
            false => &self.default_endpoint.internal_hostname,
        };
        */
        let endpoint_url = &self.default_endpoint.internal_hostname;

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
    async fn _try_connect_endpoint(
        &self,
        _endpoint_uuid: uuid::Uuid,
    ) -> Result<InternalProxyServiceClient<Channel>, ArunaError> {
        // Get endpoint from database
        /*
        let endpoint_url = match &self.default_endpoint.is_public {
            true => &self.default_endpoint.proxy_hostname,
            false => &self.default_endpoint.internal_hostname,
        };
        */
        let endpoint_url = &self.default_endpoint.internal_hostname;

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
        let (location, endpoint, encryption_key) = self
            .database
            .get_primary_object_location_with_endpoint(object_uuid)?;

        /*
        let endpoint_url = match &self.default_endpoint.is_public {
            true => &self.default_endpoint.proxy_hostname,
            false => &self.default_endpoint.internal_hostname,
        };
        */
        let endpoint_url = &self.default_endpoint.internal_hostname;

        // Try to establish connection to endpoint
        let data_proxy = InternalProxyServiceClient::connect(endpoint_url.to_string()).await;

        match data_proxy {
            Ok(dp) => {
                let proto_location = Location {
                    r#type: endpoint.endpoint_type as i32,
                    bucket: location.bucket,
                    path: location.path,
                    endpoint_id: self.default_endpoint.id.to_string(),
                    is_compressed: location.is_compressed,
                    is_encrypted: location.is_encrypted,
                    encryption_key: if let Some(key) = encryption_key {
                        key.encryption_key
                    } else {
                        "".to_string()
                    }, // ...
                };
                Ok((dp, proto_location))
            }
            Err(_) => Err(ArunaError::DataProxyError(Status::new(
                Code::Internal,
                "Could not connect to objects primary data proxy endpoint",
            ))),
        }
    }
}

///ToDo: Rust Doc
#[tonic::async_trait]
impl ObjectService for ObjectServiceImpl {
    async fn initialize_new_object(
        &self,
        request: Request<InitializeNewObjectRequest>,
    ) -> Result<Response<InitializeNewObjectResponse>, Status> {
        log::info!("Received InitializeNewObjectRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check if user is authorized to create objects in this collection
        let collection_id =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;

        let creator_id = self
            .authz
            .collection_authorize(
                request.metadata(),
                collection_id, // This is the collection uuid in which this object should be created
                UserRights::APPEND, // User needs at least append permission to create an object
            )
            .await?;

        let new_object_uuid = uuid::Uuid::new_v4();
        // Connect to default data proxy endpoint
        let mut data_proxy = self.try_connect_default_endpoint().await?;

        // Extract request body
        let inner_request = request.into_inner(); // Consumes the gRPC request

        // Generate upload_id for object through (currently only default) storage endpoint
        let location = Location {
            r#type: self.default_endpoint.endpoint_type as i32,
            bucket: collection_id.to_string(),
            path: new_object_uuid.to_string(),
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
        // TODO: CREATE url based on endpoint!
        let _endpoint_id = self.default_endpoint.id;
        let response = Response::new(
            task::spawn_blocking(move || {
                database_clone.create_object(
                    &inner_request,
                    &creator_id,
                    upload_id,
                    new_object_uuid,
                )
            })
            .await
            .map_err(ArunaError::from)??,
        );

        // Return gRPC response after everything succeeded
        log::info!("Sending InitializeNewObjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    async fn get_upload_url(
        &self,
        request: Request<GetUploadUrlRequest>,
    ) -> Result<Response<GetUploadUrlResponse>, Status> {
        log::info!("Received GetUploadUrlRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check if user is authorized to upload object data in this collection
        let object_id =
            uuid::Uuid::parse_str(&request.get_ref().object_id).map_err(ArunaError::from)?;
        let collection_id =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;

        let _creator_id = self
            .authz
            .collection_authorize(
                request.metadata(),
                collection_id, // This is the collection uuid in which this object should be created
                UserRights::APPEND, // User needs at least append permission to create an object
            )
            .await?;

        // Extract request body
        let inner_request = request.into_inner(); // Consumes the gRPC request

        // Check object status == INITIALIZING before data proxy requests
        let database_clone = self.database.clone();
        let proto_object_url = task::spawn_blocking(move || {
            database_clone.get_object_by_id(&object_id, &collection_id)
        })
        .await
        .map_err(ArunaError::from)??;

        if let Some(object_info) = proto_object_url.object {
            if object_info.status != ProtoStatus::Initializing as i32 {
                return Err(tonic::Status::invalid_argument(format!(
                    "object {object_id} is not in staging phase."
                )));
            }
        }

        // Try to connect to one of the objects data proxy endpoints (currently only primary location endpoint)
        let (mut data_proxy, location) = self.try_connect_object_endpoint(&object_id).await?;

        let part_number: i64 = if inner_request.multipart && inner_request.part_number < 1 {
            return Err(tonic::Status::invalid_argument(
                "Invalid part number, must be greater or equal 1",
            ));
        } else if !inner_request.multipart {
            1
        } else {
            inner_request.part_number as i64
        };

        // Get upload url through data proxy
        let upload_url = data_proxy
            .create_presigned_upload_url(CreatePresignedUploadUrlRequest {
                multipart: inner_request.multipart,
                part_number,
                location: Some(location),
                upload_id: inner_request.upload_id, //Note: Can be moved, only used here
            })
            .await?
            .into_inner()
            .url;

        let response = Response::new(GetUploadUrlResponse {
            url: Some(Url { url: upload_url }),
        });

        log::info!("Sending GetUploadUrlResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    async fn finish_object_staging(
        &self,
        request: Request<FinishObjectStagingRequest>,
    ) -> Result<Response<FinishObjectStagingResponse>, Status> {
        log::info!("Received FinishObjectStagingRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Parse the provided collection id (string) to UUID
        let collection_id = uuid::Uuid::parse_str(&request.get_ref().collection_id)
            .map_err(|_| Status::invalid_argument("Unable to parse collection id"))?;

        // Authorize the request
        let creator_id = self
            .authz
            .collection_authorize(
                request.metadata(),
                collection_id, // This is the collection uuid in which this object should be created
                UserRights::APPEND, // User needs at least append permission to create an object
            )
            .await?;

        // Only finish the upload if no_upload == false
        // This will otherwise skip the data proxy finish routine
        if !request.get_ref().no_upload {
            // Create the finished parts vec from request
            let finished_parts = request
                .get_ref()
                .completed_parts
                .iter()
                .map(|part| PartETag {
                    part_number: part.part,
                    etag: part.etag.to_string(),
                })
                .collect::<Vec<_>>();

            // If finished parts is not empty --> multipart upload
            if !finished_parts.is_empty() {
                // Create multipart upload finish request for data proxy
                let finished_presigned = FinishPresignedUploadRequest {
                    upload_id: request.get_ref().upload_id.to_string(),
                    bucket: collection_id.to_string(),
                    key: request.get_ref().object_id.to_string(),
                    part_etags: finished_parts,
                    multipart: true, //multipart: *is_empty,
                };

                // Get the data_proxy
                let (mut data_proxy, _location) = self
                    .try_connect_object_endpoint(
                        &uuid::Uuid::parse_str(&request.get_ref().object_id).map_err(|_| {
                            Status::invalid_argument("Unable to parse object_id to uuid")
                        })?,
                    )
                    .await?;

                // Execute the proxy request and get the result
                let proxy_result = data_proxy
                    .finish_presigned_upload(finished_presigned)
                    .await?
                    .into_inner();

                // Only proceed when proxy did not fail
                if !proxy_result.ok {
                    return Err(Status::aborted("Proxy failed to finish object"));
                }
            }
        }

        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || {
                database_clone.finish_object_staging(&request.into_inner(), &creator_id)
            })
            .await
            .map_err(ArunaError::from)??,
        );

        log::info!("Sending FinishObjectStagingResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    async fn update_object(
        &self,
        request: Request<UpdateObjectRequest>,
    ) -> Result<Response<UpdateObjectResponse>, Status> {
        log::info!("Received UpdateObjectRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check if user is authorized to create objects in this collection
        let collection_id =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;

        let creator_id = self
            .authz
            .collection_authorize(
                request.metadata(),
                collection_id, // This is the collection uuid in which this object should be created
                UserRights::WRITE, // User needs at least append permission to create an object
            )
            .await?;

        let new_object_uuid = uuid::Uuid::new_v4();

        // Extract request body
        let inner_request = request.into_inner(); // Consumes the gRPC request

        let (_location, upload_id) = if inner_request.reupload {
            // Connect to default data proxy endpoint
            let mut data_proxy = self.try_connect_default_endpoint().await?;

            // Generate upload_id for object through (currently only default) storage endpoint
            let location = Location {
                r#type: self.default_endpoint.endpoint_type as i32,
                bucket: collection_id.to_string(),
                path: new_object_uuid.to_string(),
            };

            let upload_id = data_proxy
                .init_presigned_upload(InitPresignedUploadRequest {
                    location: Some(location.clone()),
                    multipart: inner_request.multi_part,
                })
                .await?
                .into_inner()
                .upload_id;
            (Some(location), Some(upload_id))
        } else {
            (None, None)
        };

        // Create Object in database
        let database_clone = self.database.clone();
        // TODO: CREATE URL based on preferred endpoint
        let _endpoint_id = self.default_endpoint.id;
        let mut response = task::spawn_blocking(move || {
            database_clone.update_object(&inner_request, &creator_id, new_object_uuid)
        })
        .await
        .map_err(ArunaError::from)??;

        if let Some(up_id) = upload_id {
            response.staging_id = up_id;
        }

        // Return gRPC response after everything succeeded
        let grpc_response = Response::new(response);

        log::info!("Sending UpdateObjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&grpc_response));
        return Ok(grpc_response);
    }

    /// Creates a reference to an object in another collection.
    ///
    /// ## Arguments:
    ///
    /// * `Request<BorrowObjectRequest>` -
    ///
    /// ## Returns:
    ///
    /// * `Result<Response<BorrowObjectResponse>, Status>` - An empty BorrowObjectResponse signals success
    ///
    /// ## Behaviour:
    ///
    /// Returns an error if `collection_id == target_collection_id` and/or the object is already borrowed
    /// to the target collection as object duplicates in collections are not allowed.
    ///
    async fn create_object_reference(
        &self,
        request: Request<CreateObjectReferenceRequest>,
    ) -> Result<Response<CreateObjectReferenceResponse>, Status> {
        log::info!("Received CreateObjectReferenceRequest.");
        log::debug!("{}", format_grpc_request(&request));

        let src_collection_id =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;
        let dst_collection_id =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;

        // Need WRITE permission for writeable == true; READ else
        let needed_permission = match request.get_ref().writeable {
            true => UserRights::WRITE,
            false => UserRights::READ,
        };

        // Check if user is authorized to borrow object from source collection
        self.authz
            .collection_authorize(request.metadata(), src_collection_id, needed_permission)
            .await?;
        // Check if user is authorized to borrow object to target collection
        self.authz
            .collection_authorize(request.metadata(), dst_collection_id, UserRights::APPEND)
            .await?;

        // Consume request
        let inner_request = request.into_inner();

        // Try to create object reference
        let response = Response::new(self.database.create_object_reference(inner_request)?);

        // Return response if everything passed successfully
        log::info!("Sending CreateObjectReferenceResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    ///ToDo: Rust Doc
    async fn get_references(
        &self,
        request: Request<GetReferencesRequest>,
    ) -> Result<Response<GetReferencesResponse>, Status> {
        log::info!("Received GetReferencesRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check if user is authorized to create objects in this collection
        let collection_id =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;

        self.authz
            .collection_authorize(
                request.metadata(),
                collection_id, // This is the collection uuid in which this object should be created
                UserRights::READ, // User needs at least append permission to create an object
            )
            .await?;

        // Create Object in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || database_clone.get_references(request.get_ref()))
                .await
                .map_err(ArunaError::from)??,
        );

        // Return gRPC response after everything succeeded
        log::info!("Sending GetReferencesResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    async fn clone_object(
        &self,
        request: Request<CloneObjectRequest>,
    ) -> Result<Response<CloneObjectResponse>, Status> {
        log::info!("Received CloneObjectRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check if user is authorized to create objects in this collection
        let collection_id =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;

        // Authorize "ORIGIN" TODO: Include project_id to use project_authorize
        let creator_uuid = self
            .authz
            .collection_authorize(
                request.metadata(),
                collection_id, // This is the collection uuid in which this object should be created
                UserRights::READ, // User needs at least append permission to create an object
            )
            .await?;

        let target_collection_uuid =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;
        // Authorize "TARGET"
        self.authz
            .collection_authorize(
                request.metadata(),
                target_collection_uuid, // This is the collection uuid in which this object should be created
                UserRights::APPEND,     // User needs at least append permission to create an object
            )
            .await?;

        // Create Object in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || {
                database_clone.clone_object(request.get_ref(), &creator_uuid)
            })
            .await
            .map_err(ArunaError::from)??,
        );

        // Return gRPC response after everything succeeded
        log::info!("Sending CloneObjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    async fn delete_objects(
        &self,
        request: Request<DeleteObjectsRequest>,
    ) -> Result<Response<DeleteObjectsResponse>, Status> {
        log::info!("Received DeleteObjectsRequest.");
        log::debug!("{}", format_grpc_request(&request));

        let user: uuid::Uuid = if request.get_ref().force {
            let target_collection_uuid = uuid::Uuid::parse_str(&request.get_ref().collection_id)
                .map_err(ArunaError::from)?;
            // Authorize "TARGET"
            self.authz
                .project_authorize_by_collectionid(
                    request.metadata(),
                    target_collection_uuid, // This is the collection uuid which the project_id will be based
                    UserRights::ADMIN, // User needs at least append permission to create an object
                )
                .await?
        } else {
            let target_collection_uuid = uuid::Uuid::parse_str(&request.get_ref().collection_id)
                .map_err(ArunaError::from)?;
            // Authorize "TARGET"
            self.authz
                .collection_authorize(
                    request.metadata(),
                    target_collection_uuid, // This is the collection uuid in which this object should be created
                    UserRights::APPEND, // User needs at least append permission to create an object
                )
                .await?
        };

        // Create Object in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || database_clone.delete_objects(request.into_inner(), user))
                .await
                .map_err(ArunaError::from)??,
        );

        // Return gRPC response after everything succeeded
        log::info!("Sending DeleteObjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        log::info!("Received DeleteObjectRequest.");
        log::debug!("{}", format_grpc_request(&request));

        let user: uuid::Uuid = if request.get_ref().force {
            let target_collection_uuid = uuid::Uuid::parse_str(&request.get_ref().collection_id)
                .map_err(ArunaError::from)?;
            // Authorize "TARGET"
            self.authz
                .project_authorize_by_collectionid(
                    request.metadata(),
                    target_collection_uuid, // This is the collection uuid which the project_id will be based
                    UserRights::ADMIN, // User needs at least admin permission to force delete an object
                )
                .await?
        } else {
            let target_collection_uuid = uuid::Uuid::parse_str(&request.get_ref().collection_id)
                .map_err(ArunaError::from)?;
            // Authorize "TARGET"
            self.authz
                .collection_authorize(
                    request.metadata(),
                    target_collection_uuid, // This is the collection uuid in which this object should be created
                    UserRights::APPEND, // User needs at least append permission to delete an object
                )
                .await?
        };

        // Create Object in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || database_clone.delete_object(request.into_inner(), user))
                .await
                .map_err(ArunaError::from)??,
        );

        // Return gRPC response after everything succeeded
        log::info!("Sending DeleteObjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    /// This functions returns a fully populated Object from the database and depending
    /// on the request additionally a direct download URL.
    ///
    /// ## Arguments:
    ///
    /// * Request<GetObjectByIdRequest> -
    /// gRPC request which contains the necessary information to get the specific object.
    ///
    /// ## Returns:
    ///
    /// * Response<GetObjectByIdResponse> -
    /// gRPC response which contains the Object and a direct download URL which can be empty,
    /// depending on the request.
    ///
    async fn get_object_by_id(
        &self,
        request: Request<GetObjectByIdRequest>,
    ) -> Result<Response<GetObjectByIdResponse>, Status> {
        log::info!("Received GetObjectByIdRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check if user is authorized to create objects in this collection
        let object_uuid =
            uuid::Uuid::parse_str(&request.get_ref().object_id).map_err(ArunaError::from)?;
        let collection_uuid =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;

        let _creator_id = self
            .authz
            .collection_authorize(
                request.metadata(),
                collection_uuid, // This is the collection uuid in which this object should be created
                UserRights::READ,
            )
            .await?;

        // Consume request and extract inner body
        let inner_request = request.into_inner(); // Consumes the gRPC request

        // Get object and its location
        let database_clone = self.database.clone();
        let mut proto_object_url = task::spawn_blocking(move || {
            database_clone.get_object_by_id(&object_uuid, &collection_uuid)
        })
        .await
        .map_err(ArunaError::from)??;

        let object_data = match proto_object_url.object.clone() {
            Some(p) => p,
            None => {
                return Err(tonic::Status::invalid_argument("object not found"));
            }
        };

        // Only request url from data proxy if:
        //  - object_data.status == ObjectStatus::AVAILABLE
        //  - request.with_url   == true
        let response =
            match inner_request.with_url && object_data.status == ProtoStatus::Available as i32 {
                true => {
                    // Establish connection to data proxy endpoint
                    let (mut data_proxy, location) =
                        self.try_connect_object_endpoint(&object_uuid).await?;

                    let data_proxy_request = CreatePresignedDownloadRequest {
                        location: Some(location),
                        is_public: false,
                        filename: object_data.filename.clone(),
                        range: Some(Range {
                            start: 0,
                            end: object_data.content_len,
                        }),
                    };

                    proto_object_url.url = data_proxy
                        .create_presigned_download(data_proxy_request)
                        .await?
                        .into_inner()
                        .url;

                    GetObjectByIdResponse {
                        object: Some(proto_object_url),
                    }
                }
                false => GetObjectByIdResponse {
                    object: Some(proto_object_url),
                },
            };

        let grpc_response = Response::new(response);
        log::info!("Sending GetObjectByIdResponse back to client.");
        log::debug!("{}", format_grpc_response(&grpc_response));
        return Ok(grpc_response);
    }

    async fn get_objects(
        &self,
        request: Request<GetObjectsRequest>,
    ) -> Result<Response<GetObjectsResponse>, Status> {
        log::info!("Received GetObjectsRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check if user is authorized to create objects in this collection
        let collection_id =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;

        self.authz
            .collection_authorize(
                request.metadata(),
                collection_id, // This is the collection uuid in which this object should be created
                UserRights::READ, // User needs at least append permission to create an object
            )
            .await?;

        let req_clone = request.get_ref().clone();
        // Create Object in database
        let database_clone = self.database.clone();
        let response = task::spawn_blocking(move || database_clone.get_objects(req_clone))
            .await
            .map_err(ArunaError::from)??;

        let result = if let Some(object_with_urls) = response {
            for mut object_add_url in object_with_urls.clone() {
                let object_info = if let Some(info) = object_add_url.object {
                    info
                } else {
                    Object::default()
                };

                if request.get_ref().with_url && object_info.status == ProtoStatus::Available as i32
                {
                    // Connect to one of the objects data proxy endpoints
                    let (mut data_proxy, location) = self
                        .try_connect_object_endpoint(
                            &uuid::Uuid::parse_str(&object_info.id).map_err(ArunaError::from)?,
                        )
                        .await?;
                    // Get download url from data proxy endpoint
                    object_add_url.url = data_proxy
                        .create_presigned_download(CreatePresignedDownloadRequest {
                            location: Some(location),
                            is_public: false,
                            filename: object_info.filename.clone(),
                            range: Some(Range {
                                start: 0,
                                end: object_info.content_len,
                            }),
                        })
                        .await?
                        .into_inner()
                        .url;
                };
            }
            object_with_urls
        } else {
            Vec::new()
        };

        // Return gRPC response after everything succeeded
        let grpc_response = Response::new(GetObjectsResponse { objects: result });

        log::info!("Sending GetObjectsResponse back to client.");
        log::debug!("{}", format_grpc_response(&grpc_response));
        return Ok(grpc_response);
    }

    async fn get_object_revisions(
        &self,
        request: Request<GetObjectRevisionsRequest>,
    ) -> Result<Response<GetObjectRevisionsResponse>, Status> {
        log::info!("Received GetObjectRevisionsRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check if user is authorized to create objects in this collection
        let collection_id =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;

        self.authz
            .collection_authorize(
                request.metadata(),
                collection_id, // This is the collection uuid in which this object should be created
                UserRights::READ, // User needs at least append permission to create an object
            )
            .await?;

        let req_clone = request.get_ref().clone();
        // Create Object in database
        let database_clone = self.database.clone();
        let response = task::spawn_blocking(move || database_clone.get_object_revisions(req_clone))
            .await
            .map_err(ArunaError::from)??;

        let result = {
            for mut object_add_url in response.clone() {
                let object_info = if let Some(info) = object_add_url.object {
                    info
                } else {
                    Object::default()
                };

                if request.get_ref().with_url && object_info.status == ProtoStatus::Available as i32
                {
                    // Connect to one of the objects data proxy endpoints
                    let (mut data_proxy, location) = self
                        .try_connect_object_endpoint(
                            &uuid::Uuid::parse_str(&object_info.id).map_err(ArunaError::from)?,
                        )
                        .await?;
                    // Get download url from data proxy endpoint
                    object_add_url.url = data_proxy
                        .create_presigned_download(CreatePresignedDownloadRequest {
                            location: Some(location),
                            is_public: false,
                            filename: object_info.filename.clone(),
                            range: Some(Range {
                                start: 0,
                                end: object_info.content_len,
                            }),
                        })
                        .await?
                        .into_inner()
                        .url;
                };
            }
            response
        };

        // Return gRPC response after everything succeeded
        let grpc_response = Response::new(GetObjectRevisionsResponse { objects: result });

        log::info!("Sending GetObjectRevisionsResponse back to client.");
        log::debug!("{}", format_grpc_response(&grpc_response));
        return Ok(grpc_response);
    }

    async fn get_latest_object_revision(
        &self,
        request: Request<GetLatestObjectRevisionRequest>,
    ) -> Result<Response<GetLatestObjectRevisionResponse>, Status> {
        log::info!("Received GetLatestObjectRevisionRequest.");
        log::debug!("{}", format_grpc_request(&request));

        let target_collection_uuid =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;
        let object_uuid =
            uuid::Uuid::parse_str(&request.get_ref().object_id).map_err(ArunaError::from)?;

        self.authz
            .collection_authorize(
                request.metadata(),
                target_collection_uuid, // This is the collection uuid in which this object should be created
                UserRights::READ,       // User needs at least append permission to create an object
            )
            .await?;

        // Consume tonic gRPC request
        let inner_request = request.into_inner();

        // Fetch latest Object revision from database
        let database_clone = self.database.clone();
        let inner_request_clone = inner_request.clone();
        let mut object_add_url = task::spawn_blocking(move || {
            database_clone.get_latest_object_revision(inner_request_clone)
        })
        .await
        .map_err(ArunaError::from)??;

        // Extract object meta info
        let object_info = if let Some(info) = object_add_url.object.clone() {
            info
        } else {
            Object::default()
        };

        // Only request url from data proxy if:
        //  - object_info.status == ObjectStatus::AVAILABLE
        //  - request.with_url   == true
        let response =
            match inner_request.with_url && object_info.status == ProtoStatus::Available as i32 {
                true => {
                    // Establish connection to data proxy endpoint
                    let (mut data_proxy, location) =
                        self.try_connect_object_endpoint(&object_uuid).await?;

                    let data_proxy_request = CreatePresignedDownloadRequest {
                        location: Some(location),
                        is_public: false,
                        filename: object_info.filename.clone(),
                        range: Some(Range {
                            start: 0,
                            end: object_info.content_len,
                        }),
                    };

                    object_add_url.url = data_proxy
                        .create_presigned_download(data_proxy_request)
                        .await?
                        .into_inner()
                        .url;

                    GetLatestObjectRevisionResponse {
                        object: Some(object_add_url),
                    }
                }
                false => GetLatestObjectRevisionResponse {
                    object: Some(object_add_url),
                },
            };

        // Return gRPC response after everything succeeded
        let grpc_response = Response::new(response);
        log::info!("Sending GetLatestObjectRevisionResponse back to client.");
        log::debug!("{}", format_grpc_response(&grpc_response));
        return Ok(grpc_response);
    }

    async fn get_object_endpoints(
        &self,
        request: Request<GetObjectEndpointsRequest>,
    ) -> Result<Response<GetObjectEndpointsResponse>, Status> {
        log::info!("Received GetObjectEndpointsRequest.");
        log::debug!("{}", format_grpc_request(&request));

        let target_collection_uuid =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;
        self.authz
            .collection_authorize(
                request.metadata(),
                target_collection_uuid, // This is the collection uuid in which this object should be created
                UserRights::READ,       // User needs at least append permission to create an object
            )
            .await?;

        // Create Object in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || database_clone.get_object_endpoints(request.into_inner()))
                .await
                .map_err(ArunaError::from)??,
        );

        // Return gRPC response after everything succeeded
        log::info!("Sending GetObjectEndpointsResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    async fn add_labels_to_object(
        &self,
        request: Request<AddLabelsToObjectRequest>,
    ) -> Result<Response<AddLabelsToObjectResponse>, Status> {
        log::info!("Received AddLabelToObjectRequest.");
        log::debug!("{}", format_grpc_request(&request));

        let target_collection_uuid =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;
        self.authz
            .collection_authorize(
                request.metadata(),
                target_collection_uuid, // This is the collection uuid in which this object should be created
                UserRights::WRITE,      // User needs at least append permission to create an object
            )
            .await?;

        // Create Object in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || database_clone.add_labels_to_object(request.into_inner()))
                .await
                .map_err(ArunaError::from)??,
        );

        // Return gRPC response after everything succeeded
        log::info!("Sending AddLabelToObjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    async fn set_hooks_of_object(
        &self,
        request: Request<SetHooksOfObjectRequest>,
    ) -> Result<Response<SetHooksOfObjectResponse>, Status> {
        log::info!("Received SetHooksOfObjectRequest.");
        log::debug!("{}", format_grpc_request(&request));

        let target_collection_uuid =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;
        self.authz
            .collection_authorize(
                request.metadata(),
                target_collection_uuid, // This is the collection uuid in which this object should be created
                UserRights::WRITE,      // User needs at least append permission to create an object
            )
            .await?;

        // Create Object in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || database_clone.set_hooks_of_object(request.into_inner()))
                .await
                .map_err(ArunaError::from)??,
        );

        // Return gRPC response after everything succeeded
        log::info!("Sending SetHooksOfObjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    ///ToDo: Rust Doc
    async fn get_download_url(
        &self,
        request: Request<GetDownloadUrlRequest>,
    ) -> Result<Response<GetDownloadUrlResponse>, Status> {
        log::info!("Received GetDownloadUrlRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Validate uuid format of collection id provided in the request
        let collection_uuid =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;

        // Authorize user action
        let _creator_id = self
            .authz
            .authorize(
                request.metadata(),
                &(Context {
                    user_right: UserRights::READ, // User needs at least append permission to create an object
                    resource_type: Resources::COLLECTION, // Creating a new object needs at least collection level permissions
                    resource_id: collection_uuid, // This is the collection uuid in which this object should be created
                    admin: false,
                    oidc_context: false,
                    personal: false,
                }),
            )
            .await?;

        // Extract request body
        let inner_request = request.into_inner(); // Consumes the gRPC request

        // Validate uuid format of object id provided in the request
        let object_uuid =
            uuid::Uuid::parse_str(inner_request.object_id.as_str()).map_err(ArunaError::from)?;

        // Get
        let database_clone = self.database.clone();
        let proto_object_url = task::spawn_blocking(move || {
            database_clone.get_object_by_id(&object_uuid, &collection_uuid)
        })
        .await
        .map_err(ArunaError::from)??;

        let object_data = match proto_object_url.object.clone() {
            Some(p) => p,
            None => {
                return Err(Status::invalid_argument("object not found"));
            }
        };

        // Check object status == AVAILABLE before data proxy requests
        if object_data.status != ProtoStatus::Available as i32 {
            return Err(tonic::Status::unavailable(format!(
                "object {object_uuid} is currently not available."
            )));
        }

        // Connect to one of the objects data proxy endpoints
        let (mut data_proxy, location) = self.try_connect_object_endpoint(&object_uuid).await?;

        // Get download url from data proxy endpoint
        let download_url = data_proxy
            .create_presigned_download(CreatePresignedDownloadRequest {
                location: Some(location),
                is_public: false,
                filename: object_data.filename,
                range: Some(Range {
                    start: 0,
                    end: object_data.content_len,
                }),
            })
            .await?
            .into_inner()
            .url;

        // Return gRPC response after everything succeeded
        let response = Response::new(GetDownloadUrlResponse {
            url: Some(Url { url: download_url }),
        });

        log::info!("Sending GetDownloadUrlResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    async fn get_download_links_batch(
        &self,
        request: Request<GetDownloadLinksBatchRequest>,
    ) -> Result<Response<GetDownloadLinksBatchResponse>, Status> {
        log::info!("Received GetDownloadLinksBatchRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Validate uuid format of collection id provided in the request
        let collection_uuid =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;

        // Authorize user action
        let _creator_id = self
            .authz
            .authorize(
                request.metadata(),
                &(Context {
                    user_right: UserRights::READ, // User needs at least append permission to create an object
                    resource_type: Resources::COLLECTION, // Creating a new object needs at least collection level permissions
                    resource_id: collection_uuid, // This is the collection uuid in which this object should be created
                    admin: false,
                    oidc_context: false,
                    personal: false,
                }),
            )
            .await?;

        // Extract request body
        let inner_request = request.into_inner(); // Consumes the gRPC request
        let mapped_uuids = inner_request
            .objects
            .iter()
            .map(|obj_str| uuid::Uuid::parse_str(obj_str))
            .collect::<Result<Vec<uuid::Uuid>, _>>()
            .map_err(ArunaError::from)?;

        let mut urls: Vec<String> = Vec::new();

        for object_uuid in mapped_uuids {
            let (mut data_proxy, location) = self.try_connect_object_endpoint(&object_uuid).await?;

            let object_data = match self
                .database
                .get_object_by_id(&object_uuid, &collection_uuid)?
                .object
            {
                Some(proto_object) => proto_object,
                None => {
                    return Err(Status::invalid_argument("object not found"));
                }
            };

            // Check object status == AVAILABLE before data proxy requests
            if object_data.status != ProtoStatus::Available as i32 {
                return Err(tonic::Status::unavailable(format!(
                    "object {object_uuid} is currently not available."
                )));
            }

            // Get download url from data proxy endpoint
            urls.push(
                data_proxy
                    .create_presigned_download(CreatePresignedDownloadRequest {
                        location: Some(location),
                        is_public: false,
                        filename: object_data.filename,
                        range: Some(Range {
                            start: 0,
                            end: object_data.content_len,
                        }),
                    })
                    .await?
                    .into_inner()
                    .url,
            );
        }

        let mapped_urls = urls
            .iter()
            .map(|e| Url { url: e.to_string() })
            .collect::<Vec<_>>();

        // Return gRPC response after everything succeeded
        let response = Response::new(GetDownloadLinksBatchResponse { urls: mapped_urls });

        log::info!("Sending GetDownloadLinksBatchResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    type CreateDownloadLinksStreamStream =
        ReceiverStream<Result<CreateDownloadLinksStreamResponse, Status>>;
    async fn create_download_links_stream(
        &self,
        request: Request<CreateDownloadLinksStreamRequest>,
    ) -> Result<Response<Self::CreateDownloadLinksStreamStream>, Status> {
        log::info!("Received CreateDownloadLinksStreamRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Validate uuid format of collection id provided in the request
        let collection_uuid =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;

        // Authorize user action
        let _creator_id = self
            .authz
            .authorize(
                request.metadata(),
                &(Context {
                    user_right: UserRights::READ, // User needs at least append permission to create an object
                    resource_type: Resources::COLLECTION, // Creating a new object needs at least collection level permissions
                    resource_id: collection_uuid, // This is the collection uuid in which this object should be created
                    admin: false,
                    oidc_context: false,
                    personal: false,
                }),
            )
            .await?;

        // Extract request body
        let inner_request = request.into_inner(); // Consumes the gRPC request
        let mapped_uuids = inner_request
            .objects
            .iter()
            .map(|obj_str| uuid::Uuid::parse_str(obj_str))
            .collect::<Result<Vec<uuid::Uuid>, _>>()
            .map_err(ArunaError::from)?;

        let (tx, rx) = mpsc::channel(4);
        let db_clone = self.database.clone();

        tokio::spawn(async move {
            for object_uuid in mapped_uuids {
                let try_connect =
                    try_connect_object_endpoint_moveable(db_clone.clone(), &object_uuid).await;

                if let Ok((mut data_proxy, location)) = try_connect {
                    match db_clone.get_object_by_id(&object_uuid, &collection_uuid) {
                        Ok(object_with_url) => {
                            if let Some(object_data) = object_with_url.object {
                                // Check object status == AVAILABLE before data proxy requests
                                if object_data.status != ProtoStatus::Available as i32 {
                                    tx.send(Err(tonic::Status::unavailable(format!(
                                        "object {object_uuid} is currently not available."
                                    ))))
                                    .await
                                    .unwrap();
                                }

                                tx.send(Ok(CreateDownloadLinksStreamResponse {
                                    url: Some(Url {
                                        url: data_proxy
                                            .create_presigned_download(
                                                CreatePresignedDownloadRequest {
                                                    location: Some(location),
                                                    is_public: false,
                                                    filename: object_data.filename,
                                                    range: Some(Range {
                                                        start: 0,
                                                        end: object_data.content_len,
                                                    }),
                                                },
                                            )
                                            .await
                                            .unwrap()
                                            .into_inner()
                                            .url,
                                    }),
                                }))
                                .await
                                .unwrap();
                            } else {
                                tx.send(Err(Status::invalid_argument(
                                    ArunaError::DieselError(diesel::result::Error::NotFound)
                                        .to_string(),
                                )))
                                .await
                                .unwrap();
                            }
                        }
                        Err(e) => {
                            tx.send(Err(Status::invalid_argument(e.to_string())))
                                .await
                                .unwrap();
                        }
                    }
                }
            }
        });

        // Return gRPC response after everything succeeded
        let response = Response::new(ReceiverStream::new(rx));

        log::info!("Sending CreateDownloadLinksStreamStream back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// GetObjectPath
    ///
    /// Status: BETA
    ///
    /// Get all object_paths for this object in a specific collection
    /// !! Paths are collection specific !!
    async fn get_object_path(
        &self,
        request: tonic::Request<GetObjectPathRequest>,
    ) -> Result<tonic::Response<GetObjectPathResponse>, tonic::Status> {
        log::info!("Received GetObjectPathRequest.");
        log::debug!("{}", format_grpc_request(&request));

        let target_collection_uuid =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;
        self.authz
            .collection_authorize(
                request.metadata(),
                target_collection_uuid, // This is the collection uuid context for the object
                UserRights::READ,       // User needs at least read permission to get a path
            )
            .await?;

        // Get Objectpath from database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || database_clone.get_object_path(request.into_inner()))
                .await
                .map_err(ArunaError::from)??,
        );
        // Return gRPC response after everything succeeded
        log::info!("Sending GetObjectPathResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    /// GetObjectPaths
    ///
    /// Status: BETA
    ///
    /// Get all object_paths for a specific collection
    /// !! Paths are collection specific !!
    async fn get_object_paths(
        &self,
        request: tonic::Request<GetObjectPathsRequest>,
    ) -> Result<tonic::Response<GetObjectPathsResponse>, tonic::Status> {
        log::info!("Received GetObjectPathsRequest.");
        log::debug!("{}", format_grpc_request(&request));

        let target_collection_uuid =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;
        self.authz
            .collection_authorize(
                request.metadata(),
                target_collection_uuid, // This is the collection uuid context for the object
                UserRights::READ,       // User needs at least read permission to get a path
            )
            .await?;

        // Get Objectpaths from database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || database_clone.get_object_paths(request.into_inner()))
                .await
                .map_err(ArunaError::from)??,
        );
        // Return gRPC response after everything succeeded
        log::info!("Sending GetObjectPathsResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    /// CreateObjectPath
    ///
    /// Status: BETA
    ///
    /// Create collection_specific object_paths for an object
    /// !! Paths are collection specific !!
    async fn create_object_path(
        &self,
        request: tonic::Request<CreateObjectPathRequest>,
    ) -> Result<tonic::Response<CreateObjectPathResponse>, tonic::Status> {
        log::info!("Received CreateObjectPathRequest.");
        log::debug!("{}", format_grpc_request(&request));

        let target_collection_uuid =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;
        self.authz
            .collection_authorize(
                request.metadata(),
                target_collection_uuid, // This is the collection uuid context for the object
                UserRights::WRITE,      // User needs at least read permission to get a path
            )
            .await?;

        // Create Objectpaths in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || database_clone.create_object_path(request.into_inner()))
                .await
                .map_err(ArunaError::from)??,
        );
        // Return gRPC response after everything succeeded
        log::info!("Sending CreateObjectPathResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    /// SetObjectPathVisibility
    ///
    /// Status: BETA
    ///
    /// Updates the visibility setting for an object_path (hide/unhide)
    /// !! Paths are collection specific !!
    async fn set_object_path_visibility(
        &self,
        request: tonic::Request<SetObjectPathVisibilityRequest>,
    ) -> Result<tonic::Response<SetObjectPathVisibilityResponse>, tonic::Status> {
        log::info!("Received SetObjectPathVisibilityRequest.");
        log::debug!("{}", format_grpc_request(&request));

        let target_collection_uuid =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;
        self.authz
            .collection_authorize(
                request.metadata(),
                target_collection_uuid, // This is the collection uuid context for the object
                UserRights::WRITE,      // User needs at least read permission to get a path
            )
            .await?;

        // Create Objectpaths in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || {
                database_clone.set_object_path_visibility(request.into_inner())
            })
            .await
            .map_err(ArunaError::from)??,
        );
        // Return gRPC response after everything succeeded
        log::info!("Sending SetObjectPathVisibilityResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    /// GetObjectsByPath
    ///
    /// Status: BETA
    ///
    /// Gets a specific object by object_path
    /// !! Paths are collection specific !!
    async fn get_objects_by_path(
        &self,
        request: tonic::Request<GetObjectsByPathRequest>,
    ) -> Result<tonic::Response<GetObjectsByPathResponse>, tonic::Status> {
        log::info!("Received GetObjectsByPathRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Save gRPC request metadata for later
        let metadata = request.metadata().clone();

        // Consume gRPC request
        let inner_request = request.into_inner();

        let database_clone = self.database.clone();
        let inner_request_clone = inner_request.clone();
        let (_, coll_id) = task::spawn_blocking(move || {
            database_clone.get_project_collection_ids_by_path(&inner_request_clone.path)
        })
        .await
        .map_err(ArunaError::from)??;

        match coll_id {
            None => {
                return Err(Status::from(ArunaError::InvalidRequest(
                    "Collection from path does not exist".to_string(),
                )));
            }
            Some(target_collection_uuid) => {
                self.authz
                    .collection_authorize(
                        &metadata,
                        target_collection_uuid, // This is the collection uuid context for the object
                        UserRights::READ,       // User needs at least read permission to get a path
                    )
                    .await?;
            }
        }

        // Create Objectpaths in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || database_clone.get_objects_by_path(inner_request))
                .await
                .map_err(ArunaError::from)??,
        );
        // Return gRPC response after everything succeeded
        log::info!("Sending GetObjectsByPathResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    /// Fetches the project and collection ids associated with the provided path.
    ///
    /// Status: BETA
    ///
    /// ## Arguments
    ///
    /// `request` - A gRPC request containing the fully-qualified object path.
    ///
    /// ## Results
    ///
    /// `GetProjectCollectionIDsByPathResponse` - A gRPC response containing at least the project id
    /// and the collection id if the collection exists. Returns an error if the project does not exist as well.
    ///
    async fn get_project_collection_i_ds_by_path(
        &self,
        request: Request<GetProjectCollectionIDsByPathRequest>,
    ) -> Result<Response<GetProjectCollectionIDsByPathResponse>, Status> {
        log::info!("Received GetProjectCollectionIDsByPathRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Save gRPC request metadata for later usage
        let grpc_metadata = request.metadata().clone();

        // Consume gRPC request
        let inner_request = request.into_inner();

        // Create Objectpaths in database
        let database_clone = self.database.clone();
        let (project_uuid, collection_uuid_option) = task::spawn_blocking(move || {
            database_clone.get_project_collection_ids_by_path(&inner_request.path)
        })
        .await
        .map_err(ArunaError::from)??;

        // Validate permissions with fetched ids
        if let Some(collection_uuid) = collection_uuid_option {
            self.authz
                .collection_authorize(
                    &grpc_metadata,
                    collection_uuid.clone(), // This is the collection uuid context for the object
                    UserRights::READ,        // User needs at least read permission to get ids
                )
                .await?;
        } else {
            self.authz
                .project_authorize(
                    &grpc_metadata,
                    project_uuid.clone(), // This is the project uuid context for the object
                    UserRights::READ,     // User needs at least read permission to get ids
                )
                .await?;
        }

        // Create gRPC response
        let response = tonic::Response::new(GetProjectCollectionIDsByPathResponse {
            project_id: project_uuid.to_string(),
            collection_id: match collection_uuid_option {
                None => "".to_string(),
                Some(collection_uuid) => collection_uuid.to_string(),
            },
        });

        // Return gRPC response after everything succeeded
        log::info!("Sending GetProjectCollectionIDsByPathResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }
}

// This is a moveable version of the connect_object_endpoint
// That can be transferred to
pub async fn try_connect_object_endpoint_moveable(
    database: Arc<Database>,
    object_uuid: &uuid::Uuid,
) -> Result<(InternalProxyServiceClient<Channel>, Location), ArunaError> {
    // Get primary location with its endpoint from database
    let (location, endpoint) = database.get_primary_object_location_with_endpoint(object_uuid)?;

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
}

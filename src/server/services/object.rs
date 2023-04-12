use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Code, Request, Response, Status};

use crate::database::connection::Database;
use crate::database::crud::utils::grpc_to_db_object_status;
use crate::database::models::auth::ApiToken;
use crate::database::models::enums::{ObjectStatus, Resources, UserRights};
use crate::database::models::object::Endpoint;
use crate::error::ArunaError;
use crate::server::services::authz::{sign_download_url, sign_url, Authz, Context};
use crate::server::services::utils::{format_grpc_request, format_grpc_response};
use aruna_rust_api::api::internal::v1::internal_proxy_service_client::InternalProxyServiceClient;
use aruna_rust_api::api::internal::v1::{
    FinishMultipartUploadRequest, InitMultipartUploadRequest, Location, PartETag,
};
use aruna_rust_api::api::storage::models::v1::{LabelOrIdQuery, Status as ProtoStatus};
use aruna_rust_api::api::storage::{
    services::v1::object_service_server::ObjectService, services::v1::*,
};
use http::Method;

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
    async fn _try_connect_default_endpoint(
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
    async fn try_connect_endpoint(
        &self,
        _endpoint_uuid: &uuid::Uuid,
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
    async fn _try_connect_object_endpoint(
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
        let collection_uuid =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;

        let creator_id = self
            .authz
            .collection_authorize(
                request.metadata(),
                collection_uuid, // This is the collection uuid in which this object should be created
                UserRights::APPEND, // User needs at least append permission to create an object
            )
            .await?;

        // Extract request body
        let inner_request = request.into_inner(); // Consumes the gRPC request

        // Generate uuid for staging object
        let new_object_uuid = uuid::Uuid::new_v4();

        // Evaluate endpoint id
        let endpoint_uuid = if inner_request.preferred_endpoint_id.is_empty() {
            self.default_endpoint.id
        } else {
            uuid::Uuid::parse_str(&inner_request.preferred_endpoint_id).map_err(ArunaError::from)?
        };

        // Create Object in database
        let database_clone = self.database.clone();
        let inner_request_clone = inner_request.clone();
        let mut response = task::spawn_blocking(move || {
            database_clone.create_object(
                &inner_request_clone,
                &creator_id,
                new_object_uuid,
                &endpoint_uuid,
            )
        })
        .await
        .map_err(ArunaError::from)??;

        // Fill upload id of response
        response.upload_id = if inner_request.multipart {
            // Connect to default data proxy endpoint
            let mut data_proxy = self.try_connect_endpoint(&endpoint_uuid).await?;

            // Init multipart upload
            let response = data_proxy
                .init_multipart_upload(InitMultipartUploadRequest {
                    object_id: new_object_uuid.to_string(),
                    collection_id: collection_uuid.to_string(),
                    path: "".to_string(), // TODO: For now this is unused -> might be used later
                })
                .await?
                .into_inner();

            response.upload_id
        } else {
            new_object_uuid.to_string()
        };

        let grpc_response = tonic::Response::new(response);

        // Return gRPC response after everything succeeded
        log::info!("Sending InitializeNewObjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&grpc_response));
        return Ok(grpc_response);
    }

    ///ToDo: Rust Doc
    async fn get_upload_url(
        &self,
        request: Request<GetUploadUrlRequest>,
    ) -> Result<Response<GetUploadUrlResponse>, Status> {
        log::info!("Received GetUploadUrlRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check if user is authorized to upload object data in this collection
        let object_uuid =
            uuid::Uuid::parse_str(&request.get_ref().object_id).map_err(ArunaError::from)?;
        let collection_uuid =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;

        let api_token = self
            .authz
            .authorize_verbose(
                request.metadata(),
                &Context {
                    user_right: UserRights::APPEND,
                    resource_type: Resources::COLLECTION,
                    resource_id: collection_uuid,
                    admin: false,
                    personal: false,
                    oidc_context: false,
                },
            )
            .await?
            .1
            .ok_or_else(|| {
                ArunaError::InvalidRequest("Request is missing api token".to_string())
            })?;

        // Extract request body
        let inner_request = request.into_inner(); // Consumes the gRPC request

        // Validate part number if multipart upload
        let part_number: i32 = if inner_request.multipart && inner_request.part_number < 1 {
            return Err(tonic::Status::invalid_argument(
                "Invalid multipart upload part number, must be greater or equal 1",
            ));
        } else if !inner_request.multipart {
            1
        } else {
            inner_request.part_number
        };

        // Check object status == INITIALIZING before url creation
        let database_clone = self.database.clone();
        let endpoint_clone = self.default_endpoint.clone();
        let response = task::spawn_blocking(move || {
            let proto_object_url =
                database_clone.get_object_by_id(&object_uuid, &collection_uuid)?;

            let object_data = match &proto_object_url.object {
                Some(p) => p,
                None => {
                    return Err(Status::invalid_argument("object not found"));
                }
            };
            if grpc_to_db_object_status(&object_data.status) != ObjectStatus::INITIALIZING {
                return Err(Status::new(
                    Code::InvalidArgument,
                    "Upload urls can only be generated for objects in staging phase",
                ));
            }

            // Create presigned upload url
            let mut endpoint_option: Option<String> = None;
            let mut s3bucket_option: Option<String> = None;
            let mut s3key_option: Option<String> = None;
            if let Some(object_info) = proto_object_url.object {
                if object_info.status != ProtoStatus::Initializing as i32 {
                    return Err(tonic::Status::invalid_argument(format!(
                        "object {object_uuid} is not in staging phase."
                    )));
                }

                for label in object_info.labels {
                    if label.key == *"app.aruna-storage.org/new_path" {
                        s3key_option = Some(label.value.to_string());
                    } else if label.key == *"app.aruna-storage.org/bucket" {
                        s3bucket_option = Some(label.value.to_string());
                    } else if label.key == *"app.aruna-storage.org/endpoint_id" {
                        endpoint_option = Some(label.value.to_string());
                    }

                    if s3bucket_option.is_some()
                        && s3key_option.is_some()
                        && endpoint_option.is_some()
                    {
                        break;
                    }
                }
            }
            let s3key = s3key_option
                .ok_or_else(|| {
                    Status::new(Code::Internal, "Staging object has no internal path label")
                })?
                .replace('/', "");

            let s3bucket = s3bucket_option.ok_or_else(|| {
                Status::new(
                    Code::Internal,
                    "Staging object has no internal bucket label",
                )
            })?;

            let endpoint_proxy_hostname = if let Some(endpoint_uuid) = endpoint_option {
                let ep_uuid = uuid::Uuid::parse_str(&endpoint_uuid).map_err(ArunaError::from)?;
                database_clone.get_endpoint(&ep_uuid)?.proxy_hostname
            } else {
                endpoint_clone.proxy_hostname.to_string()
            };

            Ok(GetUploadUrlResponse {
                url: Some(Url {
                    url: sign_url(
                        Method::PUT,
                        &api_token.id.to_string(),
                        &api_token.secretkey,
                        endpoint_proxy_hostname.starts_with("https://"),
                        inner_request.multipart,
                        part_number,
                        &inner_request.upload_id,
                        &s3bucket,
                        &s3key,
                        endpoint_proxy_hostname.as_str(), // Will be "sanitized" in the sign_url(...) function
                        604800, // Default 1 week until requests support custom duration
                    )
                    .map_err(|err| {
                        tonic::Status::new(Code::Internal, format!("Url signing failed: {err}"))
                    })?,
                }),
            })
        })
        .await
        .map_err(ArunaError::from)??;

        // Self sign upload url
        let grpc_response = Response::new(response);

        log::info!("Sending GetUploadUrlResponse back to client.");
        log::debug!("{}", format_grpc_response(&grpc_response));
        Ok(grpc_response)
    }

    async fn finish_object_staging(
        &self,
        request: Request<FinishObjectStagingRequest>,
    ) -> Result<Response<FinishObjectStagingResponse>, Status> {
        log::info!("Received FinishObjectStagingRequest.");
        log::debug!("{}", format_grpc_request(&request));

        let object_uuid = uuid::Uuid::parse_str(&request.get_ref().object_id)
            .map_err(|_| Status::invalid_argument("Unable to parse object id"))?;

        // Parse the provided collection id (string) to UUID
        let collection_uuid = uuid::Uuid::parse_str(&request.get_ref().collection_id)
            .map_err(|_| Status::invalid_argument("Unable to parse collection id"))?;

        // Authorize the request
        let creator_id = self
            .authz
            .collection_authorize(
                request.metadata(),
                collection_uuid, // This is the collection uuid in which this object should be created
                UserRights::APPEND, // User needs at least append permission to create an object
            )
            .await?;

        // Consume gRPC request
        let inner_request = request.into_inner();

        // Fetch staging object to get temp path from init
        let staging_object = self
            .database
            .get_object_by_id(&object_uuid, &collection_uuid)?
            .object
            .ok_or(ArunaError::InvalidRequest(format!(
                "Could not find object {object_uuid} in collection {collection_uuid}"
            )))?;

        // Check if object status is still INITIALIZING
        if grpc_to_db_object_status(&staging_object.status) != ObjectStatus::INITIALIZING {
            return Err(tonic::Status::invalid_argument(
                "Cannot finish object which is not in staging phase",
            ));
        }

        // Process internal labels
        let mut upload_path = "".to_string();
        let mut endpoint_label_value = "".to_string();
        for label in staging_object.labels {
            if label.key == *"app.aruna-storage.org/new_path" {
                if upload_path.is_empty() {
                    upload_path = label.value;
                } else {
                    upload_path = upload_path + &label.value;

                    if !endpoint_label_value.is_empty() {
                        break;
                    }
                }
            } else if label.key == *"app.aruna-storage.org/bucket" {
                if upload_path.is_empty() {
                    upload_path = label.value;
                } else {
                    upload_path = label.value + &upload_path;

                    if !endpoint_label_value.is_empty() {
                        break;
                    }
                }
            } else if label.key == *"app.aruna-storage.org/endpoint" {
                endpoint_label_value = label.value;
            }
        }
        if upload_path.is_empty() {
            return Err(tonic::Status::internal(
                "No temp upload path for object available",
            ));
        }
        if endpoint_label_value.is_empty() {
            endpoint_label_value = self.default_endpoint.id.to_string();
        }

        let endpoint_uuid = uuid::Uuid::parse_str(&endpoint_label_value)
            .map_err(|_| Status::invalid_argument("Unable to parse provided endpoint id"))?;

        // Only finish the upload if no_upload == false
        // This will otherwise skip the data proxy finish routine
        if !inner_request.no_upload {
            // Create the finished parts vec from request
            let finished_parts = inner_request
                .completed_parts
                .iter()
                .map(|part| PartETag {
                    part_number: part.part,
                    etag: part.etag.to_string(),
                })
                .collect::<Vec<_>>();

            // If finished parts is not empty --> multipart upload
            if !finished_parts.is_empty() {
                let finish_multipart_request = FinishMultipartUploadRequest {
                    upload_id: inner_request.upload_id.to_string(),
                    collection_id: collection_uuid.to_string(),
                    object_id: object_uuid.to_string(),
                    path: format!("s3://{upload_path}"),
                    part_etags: finished_parts,
                };

                // Connect to the data proxy where the user (hopefully) uploaded the data
                let mut data_proxy = self.try_connect_endpoint(&endpoint_uuid).await?;

                // Execute the proxy request and get the result
                let proxy_result = data_proxy
                    .finish_multipart_upload(finish_multipart_request)
                    .await;

                // Only proceed when proxy did not fail
                if proxy_result.is_err() {
                    return Err(Status::aborted(
                        "Proxy failed to finish object multipart upload.",
                    ));
                }
            }
        }

        let database_clone = self.database.clone();
        let inner_request_clone = inner_request.clone();
        let response = Response::new(
            task::spawn_blocking(move || {
                database_clone.finish_object_staging(&inner_request_clone, &creator_id)
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
        let collection_uuid =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;

        let creator_id = self
            .authz
            .collection_authorize(
                request.metadata(),
                collection_uuid, // This is the collection uuid in which this object should be created
                UserRights::WRITE, // User needs at least append permission to create an object
            )
            .await?;

        let new_object_uuid = uuid::Uuid::new_v4();

        // Extract request body
        let inner_request = request.into_inner(); // Consumes the gRPC request

        // Evaluate endpoint id
        let endpoint_uuid = if inner_request.preferred_endpoint_id.is_empty() {
            self.default_endpoint.id
        } else {
            uuid::Uuid::parse_str(&inner_request.preferred_endpoint_id).map_err(ArunaError::from)?
        };

        let upload_id = if inner_request.reupload {
            if inner_request.multi_part {
                // Connect to default data proxy endpoint
                let mut data_proxy = self.try_connect_endpoint(&endpoint_uuid).await?;

                // Init multipart upload
                let response = data_proxy
                    .init_multipart_upload(InitMultipartUploadRequest {
                        collection_id: collection_uuid.to_string(),
                        object_id: new_object_uuid.to_string(),
                        path: String::new(), // TODO: Empty for now
                    })
                    .await?
                    .into_inner();

                Some(response.upload_id)
            } else {
                Some(new_object_uuid.to_string())
            }
        } else {
            None
        };

        // Create Object in database
        let database_clone = self.database.clone();
        let mut response = task::spawn_blocking(move || {
            database_clone.update_object(
                inner_request,
                &creator_id,
                new_object_uuid,
                &endpoint_uuid,
            )
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

        let api_token = self
            .authz
            .authorize_verbose(
                request.metadata(),
                &Context {
                    user_right: UserRights::READ,
                    resource_type: Resources::COLLECTION,
                    resource_id: collection_uuid,
                    admin: false,
                    personal: false,
                    oidc_context: false,
                },
            )
            .await?
            .1
            .ok_or_else(|| {
                ArunaError::InvalidRequest("Request is missing api token".to_string())
            })?;

        // Consume gRPC request
        let inner_request = request.into_inner();

        // Get object and its location
        let database_clone = self.database.clone();
        let proto_object = task::spawn_blocking(move || {
            let mut proto_object =
                database_clone.get_object_by_id(&object_uuid, &collection_uuid)?;

            let object_data = match &proto_object.object {
                Some(p) => p,
                None => {
                    return Err(Status::invalid_argument("object not found"));
                }
            };

            // Generate presigned url if desired and object is eligible
            if inner_request.with_url
                && grpc_to_db_object_status(&object_data.status) == ObjectStatus::AVAILABLE
            {
                proto_object.url = get_object_download_url(
                    database_clone,
                    &object_uuid,
                    &collection_uuid,
                    &api_token,
                )?;
            }

            Ok(proto_object)
        })
        .await
        .map_err(ArunaError::from)??;

        let response = GetObjectByIdResponse {
            object: Some(proto_object),
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

        // Validate format of provided uuids
        let collection_uuid =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;

        // Check if user is authorized to fetch objects from collection
        let api_token = self
            .authz
            .authorize_verbose(
                request.metadata(),
                &Context {
                    user_right: UserRights::READ,
                    resource_type: Resources::COLLECTION,
                    resource_id: collection_uuid,
                    admin: false,
                    personal: false,
                    oidc_context: false,
                },
            )
            .await?
            .1
            .ok_or_else(|| {
                ArunaError::InvalidRequest("Request is missing api token".to_string())
            })?;

        // Consume gRPC request
        let inner_request = request.into_inner();

        // Fetch objects from database and create presigned download urls if
        let req_clone = inner_request.clone();
        let database_clone = self.database.clone();
        let response = task::spawn_blocking(move || {
            let proto_objects_option = database_clone.get_objects(req_clone)?;

            if let Some(proto_objects) = proto_objects_option {
                let mut finished_proto_objects = Vec::new();
                for mut proto_object in proto_objects {
                    let object_data = match &proto_object.object {
                        Some(p) => p,
                        None => {
                            return Err(Status::invalid_argument("object not found"));
                        }
                    };
                    let object_uuid =
                        uuid::Uuid::parse_str(&object_data.id).map_err(ArunaError::from)?;

                    if inner_request.with_url
                        && grpc_to_db_object_status(&object_data.status) == ObjectStatus::AVAILABLE
                    {
                        proto_object.url = get_object_download_url(
                            database_clone.clone(),
                            &object_uuid,
                            &collection_uuid,
                            &api_token,
                        )?;
                    }

                    finished_proto_objects.push(proto_object)
                }
                Ok(GetObjectsResponse {
                    objects: finished_proto_objects,
                })
            } else {
                Ok(GetObjectsResponse { objects: vec![] })
            }
        })
        .await
        .map_err(ArunaError::from)??;

        // Return gRPC response after everything succeeded
        let grpc_response = Response::new(response);

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

        // Validate format of provided uuids
        let collection_uuid =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;
        let _object_uuid =
            uuid::Uuid::parse_str(&request.get_ref().object_id).map_err(ArunaError::from)?;

        // Check if user is authorized to fetch revisions of specific object
        let api_token = self
            .authz
            .authorize_verbose(
                request.metadata(),
                &Context {
                    user_right: UserRights::READ,
                    resource_type: Resources::COLLECTION,
                    resource_id: collection_uuid,
                    admin: false,
                    personal: false,
                    oidc_context: false,
                },
            )
            .await?
            .1
            .ok_or_else(|| {
                ArunaError::InvalidRequest("Request is missing api token".to_string())
            })?;

        // Consume gRPC request
        let inner_request = request.into_inner();

        // Create Object in database
        let req_clone = inner_request.clone();
        let database_clone = self.database.clone();
        let response = task::spawn_blocking(move || {
            // Fetch all revisions from database
            let object_revisions = database_clone.get_object_revisions(req_clone)?;

            // Create presigned download urls for elligible objects
            let mut finished_proto_objects = Vec::new();
            for mut proto_object in object_revisions {
                let object_data = match &proto_object.object {
                    Some(p) => p,
                    None => {
                        return Err(Status::invalid_argument("object not found"));
                    }
                };
                let proto_object_uuid =
                    uuid::Uuid::parse_str(&object_data.id).map_err(ArunaError::from)?;

                if inner_request.with_url
                    && grpc_to_db_object_status(&object_data.status) == ObjectStatus::AVAILABLE
                {
                    proto_object.url = get_object_download_url(
                        database_clone.clone(),
                        &proto_object_uuid,
                        &collection_uuid,
                        &api_token,
                    )?;
                }

                finished_proto_objects.push(proto_object)
            }

            Ok(GetObjectRevisionsResponse {
                objects: finished_proto_objects,
            })
        })
        .await
        .map_err(ArunaError::from)??;

        // Return gRPC response after everything succeeded
        let grpc_response = Response::new(response);

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

        // Validate format of provided uuids
        let collection_uuid =
            uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(ArunaError::from)?;
        let _object_uuid =
            uuid::Uuid::parse_str(&request.get_ref().object_id).map_err(ArunaError::from)?;

        // Check if user is authorized to fetch latest object revision
        let api_token = self
            .authz
            .authorize_verbose(
                request.metadata(),
                &Context {
                    user_right: UserRights::READ,
                    resource_type: Resources::COLLECTION,
                    resource_id: collection_uuid,
                    admin: false,
                    personal: false,
                    oidc_context: false,
                },
            )
            .await?
            .1
            .ok_or_else(|| {
                ArunaError::InvalidRequest("Request is missing api token".to_string())
            })?;

        // Consume tonic gRPC request
        let inner_request = request.into_inner();

        // Fetch latest Object revision from database
        let database_clone = self.database.clone();
        let inner_request_clone = inner_request.clone();
        let proto_object = task::spawn_blocking(move || {
            let mut proto_object =
                database_clone.get_latest_object_revision(inner_request_clone)?;

            let object_data = match &proto_object.object {
                Some(p) => p,
                None => {
                    return Err(Status::invalid_argument("object not found"));
                }
            };

            let proto_object_uuid =
                uuid::Uuid::parse_str(&object_data.id).map_err(ArunaError::from)?;

            if inner_request.with_url
                && grpc_to_db_object_status(&object_data.status) == ObjectStatus::AVAILABLE
            {
                proto_object.url = get_object_download_url(
                    database_clone.clone(),
                    &proto_object_uuid,
                    &collection_uuid,
                    &api_token,
                )?;

                Ok(proto_object)
            } else {
                Ok(proto_object)
            }
        })
        .await
        .map_err(ArunaError::from)??;

        let response = GetLatestObjectRevisionResponse {
            object: Some(proto_object),
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
        let object_uuid =
            uuid::Uuid::parse_str(&request.get_ref().object_id).map_err(ArunaError::from)?;

        let metadata = request.metadata().clone();

        // Authorize user action
        let api_token = self
            .authz
            .authorize_verbose(
                &metadata,
                &(Context {
                    user_right: UserRights::READ, // User needs at least append permission to create an object
                    resource_type: Resources::COLLECTION, // Creating a new object needs at least collection level permissions
                    resource_id: collection_uuid, // This is the collection uuid in which this object should be created
                    admin: false,
                    oidc_context: false,
                    personal: false,
                }),
            )
            .await?
            .1
            .ok_or_else(|| {
                ArunaError::InvalidRequest("Request is missing api token".to_string())
            })?;

        // Get object with maybe url
        let database_clone = self.database.clone();
        let object_with_maybe_url = task::spawn_blocking(move || {
            let mut proto_object =
                database_clone.get_object_by_id(&object_uuid, &collection_uuid)?;

            let object_data = match &proto_object.object {
                Some(p) => p,
                None => {
                    return Err(Status::invalid_argument("object not found"));
                }
            };

            // Generate presigned download url if object is eligible
            if grpc_to_db_object_status(&object_data.status) == ObjectStatus::AVAILABLE {
                proto_object.url = get_object_download_url(
                    database_clone.clone(),
                    &object_uuid,
                    &collection_uuid,
                    &api_token,
                )?;

                Ok(proto_object)
            } else {
                Ok(proto_object)
            }
        })
        .await
        .map_err(ArunaError::from)??;

        let response = Response::new(GetDownloadUrlResponse {
            url: Some(Url {
                url: object_with_maybe_url.url,
            }),
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
        let api_token = self
            .authz
            .authorize_verbose(
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
            .await?
            .1
            .ok_or_else(|| {
                ArunaError::InvalidRequest("Request is missing api token".to_string())
            })?;

        // Extract request body
        let inner_request = request.into_inner(); // Consumes the gRPC request

        let database_clone = self.database.clone();
        let urls: Vec<String> = task::spawn_blocking(move || {
            let mut download_urls: Vec<String> = Vec::new();

            // Get objects to check their status
            let proto_objects = if let Some(proto_objects) =
                database_clone.get_objects(GetObjectsRequest {
                    collection_id: collection_uuid.to_string(),
                    page_request: None,
                    label_id_filter: Some(LabelOrIdQuery {
                        labels: None,
                        ids: inner_request.objects,
                    }),
                    with_url: true,
                })? {
                proto_objects
            } else {
                vec![]
            };

            for proto_object in proto_objects {
                let object_data = match &proto_object.object {
                    Some(p) => p,
                    None => {
                        return Err(Status::invalid_argument("object not found"));
                    }
                };
                let proto_object_uuid =
                    uuid::Uuid::parse_str(&object_data.id).map_err(ArunaError::from)?;

                // Generate presigned download url if object is eligible
                let download_url =
                    if grpc_to_db_object_status(&object_data.status) == ObjectStatus::AVAILABLE {
                        get_object_download_url(
                            database_clone.clone(),
                            &proto_object_uuid,
                            &collection_uuid,
                            &api_token,
                        )?
                    } else {
                        "".to_string()
                    };

                download_urls.push(download_url);
            }

            Ok(download_urls)
        })
        .await
        .map_err(ArunaError::from)??;

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
        let api_token = self
            .authz
            .authorize_verbose(
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
            .await?
            .1
            .ok_or_else(|| {
                ArunaError::InvalidRequest("Request is missing api token".to_string())
            })?;

        // Extract request body
        let inner_request = request.into_inner(); // Consumes the gRPC request
        let mapped_uuids = inner_request
            .objects
            .iter()
            .map(|obj_str| uuid::Uuid::parse_str(obj_str))
            .collect::<Result<Vec<uuid::Uuid>, _>>()
            .map_err(ArunaError::from)?;

        let (tx, rx) = mpsc::channel(4);
        let database_clone = self.database.clone();
        tokio::spawn(async move {
            for object_uuid in mapped_uuids {
                match database_clone.get_object_by_id(&object_uuid, &collection_uuid) {
                    Ok(object_with_url) => {
                        if let Some(object_data) = object_with_url.object {
                            // Parse object id of proto object
                            let proto_object_uuid =
                                uuid::Uuid::parse_str(&object_data.id).map_err(ArunaError::from)?;

                            // Create presigned download url if object is eligible
                            let download_url = if grpc_to_db_object_status(&object_data.status)
                                == ObjectStatus::AVAILABLE
                            {
                                get_object_download_url(
                                    database_clone.clone(),
                                    &proto_object_uuid,
                                    &collection_uuid,
                                    &api_token,
                                )?
                            } else {
                                "".to_string()
                            };

                            tx.send(Ok(CreateDownloadLinksStreamResponse {
                                url: Some(Url { url: download_url }),
                            }))
                            .await
                            .map_err(|err| ArunaError::InvalidRequest(err.to_string()))?;
                        } else {
                            tx.send(Err(Status::invalid_argument(
                                ArunaError::DieselError(diesel::result::Error::NotFound)
                                    .to_string(),
                            )))
                            .await
                            .map_err(|err| ArunaError::InvalidRequest(err.to_string()))?;
                        }
                    }
                    Err(e) => {
                        tx.send(Err(Status::invalid_argument(e.to_string())))
                            .await
                            .map_err(|err| ArunaError::InvalidRequest(err.to_string()))?;
                    }
                }
            }

            match 1 {
                1 => Ok(()),
                _ => Err(ArunaError::InvalidRequest("Won't happen.".to_string())),
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
            database_clone.get_project_collection_ids_by_path(&inner_request_clone.path, false)
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
    async fn get_project_collection_ids_by_path(
        &self,
        request: Request<GetProjectCollectionIdsByPathRequest>,
    ) -> Result<Response<GetProjectCollectionIdsByPathResponse>, Status> {
        log::info!("Received GetProjectCollectionIDsByPathRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Save gRPC request metadata for later usage
        let grpc_metadata = request.metadata().clone();

        // Consume gRPC request
        let inner_request = request.into_inner();

        // Create Objectpaths in database
        let database_clone = self.database.clone();
        let (project_uuid, collection_uuid_option) = task::spawn_blocking(move || {
            database_clone.get_project_collection_ids_by_path(&inner_request.path, false)
        })
        .await
        .map_err(ArunaError::from)??;

        // Validate permissions with fetched ids
        if let Some(collection_uuid) = collection_uuid_option {
            self.authz
                .collection_authorize(
                    &grpc_metadata,
                    collection_uuid, // This is the collection uuid context for the object
                    UserRights::READ, // User needs at least read permission to get ids
                )
                .await?;
        } else {
            self.authz
                .project_authorize(
                    &grpc_metadata,
                    project_uuid,     // This is the project uuid context for the object
                    UserRights::READ, // User needs at least read permission to get ids
                )
                .await?;
        }

        // Create gRPC response
        let response = tonic::Response::new(GetProjectCollectionIdsByPathResponse {
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
    let (location, endpoint, encryption_key) =
        database.get_primary_object_location_with_endpoint(object_uuid)?;

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
                endpoint_id: location.endpoint_id.to_string(),
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

/// Helper function to encapsulate the creation of presigned download urls
fn get_object_download_url(
    database: Arc<Database>,
    object_uuid: &uuid::Uuid,
    collection_uuid: &uuid::Uuid,
    api_token: &ApiToken,
) -> Result<String, ArunaError> {
    let result =
        database.get_primary_object_location_with_endpoint_and_paths(object_uuid, collection_uuid);

    let (_, endpoint, _, paths) = match result {
        Ok((loc, endp, key_opt, paths)) => (loc, endp, key_opt, paths),
        Err(_) => {
            return Err(ArunaError::InvalidRequest(
                "Cannot create download url for object without uploaded data".to_string(),
            ))
        }
    };

    let active_paths = paths.iter().filter(|path| path.active).collect::<Vec<_>>();
    let (object_bucket, object_key) = if let Some(latest_active_path) = active_paths.first() {
        (
            latest_active_path.bucket.to_string(),
            if latest_active_path.path.starts_with('/') {
                latest_active_path.path[1..].to_string()
            } else {
                latest_active_path.path.to_string()
            },
        )
    } else {
        let latest_inactive_path = if let Some(latest_inactive_path) = paths.first() {
            latest_inactive_path
        } else {
            return Err(ArunaError::InvalidRequest(format!("Object has location but no path. This is an internal error.")));
        };

        (
            latest_inactive_path.bucket.to_string(),
            if latest_inactive_path.path.starts_with('/') {
                latest_inactive_path.path[1..].to_string()
            } else {
                latest_inactive_path.path.to_string()
            },
        )
    };

    Ok(sign_download_url(
        &api_token.id.to_string(),
        &api_token.secretkey,
        endpoint.proxy_hostname.starts_with("https://"),
        &object_bucket,
        &object_key,
        &endpoint.proxy_hostname, // Will be "sanitized" in the sign_url(...) function
    )
    .map_err(|err| tonic::Status::new(Code::Internal, format!("Url signing failed: {err}")))?)
}

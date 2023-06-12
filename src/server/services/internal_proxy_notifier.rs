use super::authz::Authz;
use std::str::FromStr;

use crate::database::connection::Database;

use crate::error::{ArunaError, TypeConversionError};
use crate::server::services::utils::{format_grpc_request, format_grpc_response};

use crate::database::models::enums::{Resources, UserRights};
use crate::server::services::authz::Context;
use aruna_rust_api::api::internal::v1::internal_proxy_notifier_service_server::InternalProxyNotifierService;
use aruna_rust_api::api::internal::v1::{
    FinalizeObjectRequest, FinalizeObjectResponse, GetCollectionByBucketRequest,
    GetCollectionByBucketResponse, GetObjectLocationRequest, GetObjectLocationResponse,
    GetOrCreateEncryptionKeyRequest, GetOrCreateEncryptionKeyResponse,
    GetOrCreateObjectByPathRequest, GetOrCreateObjectByPathResponse, Location,
};
use std::sync::Arc;
use tokio::task;
use tonic::{Request, Response, Status};

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(InternalProxyNotifierServiceImpl);

/// Trait created by tonic based on gRPC service definitions from .proto files.
///   The source .proto files are defined in the ArunaStorage/ArunaAPI repo.
#[tonic::async_trait]
impl InternalProxyNotifierService for InternalProxyNotifierServiceImpl {
    /// Fetch the latest revision of an object via its unique path or create a staging object if
    /// the object already exists.
    ///
    /// ## Arguments:
    ///
    /// * `Request<GetOrCreateObjectByPathRequest>` -
    ///   A gRPC request which contains the information needed to fetch or create/update an object.
    ///
    /// ## Returns:
    ///
    /// * `Result<Response<GetOrCreateObjectByPathResponse>, Status>` -
    /// The response contains the id of the fetched/created object, the collection id, the objects data class and
    /// if already available the objects hash(es).
    ///
    /// ## Behaviour:
    ///
    /// - If the object exists and no staging object is provided, the found object id will be returned.
    /// - If the object exists and a staging object is provided, an object update will be initiated and the staging object id returned.
    /// - If no object exists and a staging object is provided, an object creation will be initiated and the staging object id returned.
    /// - If no object exists and no staging object is provided, an error will be returned for an invalid request.
    async fn get_or_create_object_by_path(
        &self,
        request: Request<GetOrCreateObjectByPathRequest>,
    ) -> Result<Response<GetOrCreateObjectByPathResponse>, Status> {
        log::debug!("Received GetOrCreateObjectByPathRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Consume gRPC request
        let inner_request = request.into_inner();

        // Fetch object or create a staging object
        let database_clone = self.database.clone();
        let response = tonic::Response::new(
            task::spawn_blocking(move || {
                database_clone.get_or_create_object_by_path(inner_request)
            })
            .await
            .map_err(ArunaError::from)??,
        );

        // Return gRPC response after everything succeeded
        log::debug!("Sending GetOrCreateObjectByPathResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    /// Finalizes the object after the data proxy has worked its compression/encryption magic.
    ///
    /// ## Arguments:
    ///
    /// * `Request<FinalizeObjectRequest>` -
    ///   A gRPC request which contains the final object location and the calculated hashes of the objects data.
    ///
    /// ## Returns:
    ///
    /// * `Result<Response<FinalizeObjectResponse>, Status>` - An empty FinalizeObjectResponse signals success.
    ///
    /// ## Behaviour:
    ///
    /// Updates the sole existing object location with the provided data of the final location the
    /// object has been moved to. Also validates/creates the provided hashes depending if the individual hash
    /// already exists in the database.
    async fn finalize_object(
        &self,
        request: Request<FinalizeObjectRequest>,
    ) -> Result<Response<FinalizeObjectResponse>, Status> {
        log::debug!("Received FinalizeObjectRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Consume gRPC request
        let inner_request = request.into_inner();

        // Finalize Object in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || database_clone.finalize_object(&inner_request))
                .await
                .map_err(ArunaError::from)??,
        );

        // Return gRPC response after everything succeeded
        log::debug!("Sending FinalizeObjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    /// Get the encryption key associated with the object or create a new one if it does not exist.
    ///
    /// ## Arguments:
    ///
    /// * `Request<GetEncryptionKeyRequest>` -
    ///   A gRPC request which contains the information needed to query a specific encryption key.
    ///
    /// ## Returns:
    ///
    /// * `Result<Response<GetEncryptionKeyResponse>, Status>` - Contains the object data encryption/decryption key.
    ///
    async fn get_or_create_encryption_key(
        &self,
        request: Request<GetOrCreateEncryptionKeyRequest>,
    ) -> Result<Response<GetOrCreateEncryptionKeyResponse>, Status> {
        log::debug!("Received GetEncryptionKeyRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Consume gRPC request
        let inner_request = request.into_inner();

        // Finalize Object in database
        let database_clone = self.database.clone();
        let inner_request_clone = inner_request.clone();
        let (maybe_key, created) = task::spawn_blocking(move || {
            database_clone.get_or_create_encryption_key(&inner_request_clone)
        })
        .await
        .map_err(ArunaError::from)??;

        let response = tonic::Response::new(GetOrCreateEncryptionKeyResponse {
            encryption_key: match maybe_key {
                None => {
                    return Err(tonic::Status::internal(format!(
                        "No encryption key found or created for hash {}",
                        inner_request.hash
                    )))
                }
                Some(encryption_key) => encryption_key.encryption_key,
            },
            created,
        });

        // Return gRPC response after everything succeeded
        log::debug!("Sending GetEncryptionKeyResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    /// Get an object with its location for a specific endpoint. The data specific
    /// encryption/decryption key will be returned also if available.
    ///
    /// ## Arguments:
    ///
    /// * `Request<GetObjectLocationRequest>` -
    ///   A gRPC request which contains the information needed to query a specific object location.
    ///
    /// ## Returns:
    ///
    /// * `Result<Response<GetObjectLocationResponse>, Status>` - Contains the object and the associated location info if present.
    ///
    async fn get_object_location(
        &self,
        request: Request<GetObjectLocationRequest>,
    ) -> Result<Response<GetObjectLocationResponse>, Status> {
        log::debug!("Received GetObjectLocationRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Consume gRPC request
        let inner_request = request.into_inner();

        // Extract object revision number, endpoint id, token id from request
        let object_revision = if inner_request.revision_id.is_empty() {
            -1
        } else {
            inner_request
                .revision_id
                .parse::<i64>()
                .map_err(|_| ArunaError::TypeConversionError(TypeConversionError::STRINGTOINT))?
        };

        let endpoint_uuid = diesel_ulid::DieselUlid::from_str(&inner_request.endpoint_id)
            .map_err(ArunaError::from)?;

        let access_key = diesel_ulid::DieselUlid::from_str(&inner_request.access_key)
            .map_err(ArunaError::from)?;

        // Finalize Object in database
        let database_clone = self.database.clone();
        let (proto_object, db_location, db_endpoint, encryption_key_option) =
            task::spawn_blocking(move || {
                database_clone.get_object_with_location_info(
                    &inner_request.path,
                    object_revision,
                    &endpoint_uuid,
                    access_key,
                )
            })
            .await
            .map_err(ArunaError::from)??;

        let proto_location = Location {
            r#type: db_endpoint.endpoint_type as i32,
            bucket: db_location.bucket,
            path: db_location.path,
            endpoint_id: db_location.endpoint_id.to_string(),
            is_compressed: db_location.is_compressed,
            is_encrypted: db_location.is_encrypted,
            encryption_key: if let Some(enc_key) = encryption_key_option {
                enc_key.encryption_key
            } else {
                "".to_string()
            },
        };

        let response = tonic::Response::new(GetObjectLocationResponse {
            object: Some(proto_object),
            location: Some(proto_location),
            cors_configurations: vec![], //ToDo: Really query this info from DataProxy/S3 backend? ... 
        });

        // Return gRPC response after everything succeeded
        log::debug!("Sending GetObjectLocationResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    /// Get an object with its location for a specific endpoint. The data specific
    /// encryption/decryption key will be returned also if available.
    ///
    /// ## Arguments:
    ///
    /// * `Request<GetCollectionByBucketRequest>` -
    ///   A gRPC request which contains the information needed to query a collection by its path.
    ///
    /// ## Returns:
    ///
    /// * `Result<Response<GetCollectionByBucketResponse>, Status>` - Contains the project id and collection id.
    ///
    async fn get_collection_by_bucket(
        &self,
        request: Request<GetCollectionByBucketRequest>,
    ) -> Result<Response<GetCollectionByBucketResponse>, Status> {
        log::info!("Received GetCollectionByBucketRequest.");
        log::debug!("{}", format_grpc_request(&request));

        //ToDo: Permissions for requests from external? Options:
        //  1.) Rate Limit in Kubernetes for specific API endpoint.
        //  2.) Get ids and check permissions with them.

        // Consume gRPC request
        let inner_request = request.into_inner();

        // Extract token id disguised as access_key
        let access_key = diesel_ulid::DieselUlid::from_str(&inner_request.access_key)
            .map_err(ArunaError::from)?;

        // Finalize Object in database
        let database_clone = self.database.clone();
        let inner_request_clone = inner_request.clone();
        let (project_uuid, collection_uuid_option) = task::spawn_blocking(move || {
            database_clone.get_project_collection_ids_by_path(&inner_request_clone.bucket, true)
        })
        .await
        .map_err(ArunaError::from)??;

        match collection_uuid_option {
            None => Err(tonic::Status::not_found(format!(
                "No collection found for path {}",
                inner_request.bucket
            ))),
            Some(collection_uuid) => {
                // Validate permission with queried collection id
                self.database.get_checked_user_id_from_token(
                    &access_key,
                    &Context {
                        user_right: UserRights::READ,
                        resource_type: Resources::COLLECTION,
                        resource_id: collection_uuid,
                        admin: false,
                        personal: false,
                        oidc_context: false,
                    },
                )?;

                let response = tonic::Response::new(GetCollectionByBucketResponse {
                    project_id: project_uuid.to_string(),
                    collection_id: collection_uuid.to_string(),
                });

                // Return gRPC response after everything succeeded
                log::info!("Sending GetObjectLocationResponse back to client.");
                log::debug!("{}", format_grpc_response(&response));
                Ok(response)
            }
        }
    }
}

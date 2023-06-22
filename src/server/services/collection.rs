use std::str::FromStr;
use std::sync::Arc;

use crate::database::connection::Database;
use crate::database::models::enums::*;
use crate::error::ArunaError;
use crate::error::TypeConversionError;
use crate::server::clients::event_emit_client::NotificationEmitClient;
use crate::server::clients::kube_client::KubeClient;
use crate::server::services::utils::{format_grpc_request, format_grpc_response};
use aruna_rust_api::api::internal::v1::emitted_resource::Resource;
use aruna_rust_api::api::internal::v1::CollectionResource;
use aruna_rust_api::api::internal::v1::EmittedResource;
use aruna_rust_api::api::notification::services::v1::EventType;
use aruna_rust_api::api::storage::models::v1::ResourceType;
use aruna_rust_api::api::storage::services::v1::collection_service_server::CollectionService;
use aruna_rust_api::api::storage::services::v1::*;
use tokio::task;
use tonic::Request;
use tonic::Response;

use super::authz::Authz;

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(
    CollectionServiceImpl,
    kube_client: Option<KubeClient>,
    event_emitter: Option<NotificationEmitClient>
);

#[tonic::async_trait]
impl CollectionService for CollectionServiceImpl {
    /// Create_new_collection request cretes a new collection based on user request
    ///
    /// ## Permissions
    ///
    /// This needs project level `WRITE` permissions.
    ///
    /// ## Behaviour
    ///
    /// A new collection is created when this request is made, it needs project-level "WRITE" permissions to succeed.
    ///
    /// ## Arguments
    ///
    /// `CreateNewCollectionRequest` - Basic information about the freshly created collection like name description etc.
    ///
    /// ## Results
    ///
    /// `CreateNewCollectionResponse` - Overview of the new created collection.
    ///
    async fn create_new_collection(
        &self,
        request: tonic::Request<CreateNewCollectionRequest>,
    ) -> Result<tonic::Response<CreateNewCollectionResponse>, tonic::Status> {
        log::info!("Received CreateNewCollectionRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Parse the uuid
        let project_id = diesel_ulid::DieselUlid::from_str(&request.get_ref().project_id)
            .map_err(ArunaError::from)?;

        // Authorize the request
        let creator_id = self
            .authz
            .project_authorize(request.metadata(), project_id, UserRights::WRITE)
            .await?;

        // Execute request in spawn_blocking task to prevent blocking the API server
        let db = self.database.clone();
        let (response, bucket) = task::spawn_blocking(move || {
            db.create_new_collection(request.get_ref().to_owned(), creator_id)
        })
        .await
        .map_err(ArunaError::from)??;

        match &self.kube_client {
            Some(kc) => {
                if let Err(err) = kc.create_bucket(&bucket).await {
                    log::error!("Unable to create kube_bucket err: {err}")
                }
            }
            None => {}
        }

        // Try to emit event notification
        let collection_ulid_clone = response.collection_id.clone();
        if let Some(emit_client) = &self.event_emitter {
            let event_emitter_clone = emit_client.clone();
            task::spawn(async move {
                if let Err(err) = event_emitter_clone
                    .emit_event(
                        collection_ulid_clone.clone(),
                        ResourceType::Collection,
                        EventType::Created,
                        vec![EmittedResource {
                            resource: Some(Resource::Collection(CollectionResource {
                                project_id: project_id.to_string(),
                                collection_id: collection_ulid_clone,
                            })),
                        }],
                    )
                    .await
                {
                    // Only log error but do not crash function execution at this point
                    log::error!("Failed to emit notification: {}", err)
                }
            })
            .await
            .map_err(ArunaError::from)?;
        }

        let response = Response::new(response);

        log::info!("Sending CreateNewCollectionResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// GetCollectionById queries a single collection via its uuid.
    ///
    /// ## Permissions
    ///
    /// This needs collection level `READ` permissions.
    ///
    /// ## Behaviour
    ///
    /// This returns a single collection by id with all available information excluding all informations about objects / objectgroups etc.
    /// For this the associated methods in objects/objectgroups should be used. This needs collection-level read permissions.
    ///
    /// ## Arguments
    ///
    /// `GetCollectionByIdRequest` - Contains the requested collection_id
    ///
    /// ## Results
    ///
    /// `GetCollectionByIdResponse` - Overview of the new created collection.
    ///
    async fn get_collection_by_id(
        &self,
        request: tonic::Request<GetCollectionByIdRequest>,
    ) -> Result<tonic::Response<GetCollectionByIdResponse>, tonic::Status> {
        log::info!("Received GetCollectionByIdRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Authorize collection - READ
        self.authz
            .collection_authorize(
                request.metadata(),
                diesel_ulid::DieselUlid::from_str(&request.get_ref().collection_id)
                    .map_err(|_| ArunaError::TypeConversionError(TypeConversionError::UUID))?,
                UserRights::READ,
            )
            .await?;

        // Execute request in spawn_blocking task to prevent blocking the API server
        let db = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || db.get_collection_by_id(request.get_ref().to_owned()))
                .await
                .map_err(ArunaError::from)??,
        );

        log::info!("Sending GetCollectionByIdResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// GetCollections queries multiple collections via either multiple uuids or a specific set of label filters.
    ///
    /// ## Permissions
    ///
    /// Needs project-level `READ` permissions.
    ///
    /// ## Behaviour
    ///
    /// Will return the "first" 20 collections if no pagination / uuid / labelfilter is specified.
    ///
    /// This request takes one of two arguments and a pagination filter. The argument to query a subset of collection
    /// can either be a list with uuids or a specific set of labels/hooks, for now this will query both at once and will
    /// not distinguish between labels and hooks. As arguments the user can provide a list of key-value pairs and choose if
    /// all of them should match or any of them. Additionally the user can also choose to just use the keys not the values.
    /// This enables the user to specifically query a subselection of collections via labels if they are generic enough.
    ///
    /// This request also contains a pagination filter. This pagination filter contains the last observed uuid (optional) and
    /// a pagesize. No pagesize (0) will apply the default pagesize of 20. If -1 is set this will not paginate (WARNING: might take some time)
    /// The last uuid is always needed if the next page should be returned. By default all responses are ordered by uuid, by specifying the last
    /// uuid this will return the next "batch" of collections.
    ///
    /// ## Arguments
    ///
    /// `GetCollectionsRequest` - Contains a list of collectionids or a label_filter and an optional pagination information
    ///
    /// ## Results
    ///
    /// `GetCollectionsResponse` - Contains a list with collection_overviews that match the request.
    ///
    async fn get_collections(
        &self,
        request: tonic::Request<GetCollectionsRequest>,
    ) -> Result<tonic::Response<GetCollectionsResponse>, tonic::Status> {
        log::info!("Received GetCollectionsRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Authorize this needs project-level read permissions.
        self.authz
            .project_authorize(
                request.metadata(),
                diesel_ulid::DieselUlid::from_str(&request.get_ref().project_id)
                    .map_err(|_| ArunaError::TypeConversionError(TypeConversionError::UUID))?,
                UserRights::READ,
            )
            .await?;

        // Execute request in spawn_blocking task to prevent blocking the API server
        let db = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || db.get_collections(request.get_ref().to_owned()))
                .await
                .map_err(ArunaError::from)??,
        );

        log::info!("Sending GetCollectionsResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// UpdateCollection updates a specific collection and optional pins a new version.
    ///
    /// ## Permissions
    ///
    /// This needs collection `WRITE` permissions.
    ///
    /// ## Behaviour
    ///
    /// ATTENTION: For now this update will overwrite all existing data with the specified information. Empty fields will be empty,
    /// if you want to keep some information, query them first. This might change in the future.
    ///
    /// This request can be used to update a collection. Two modes of operation are possible, if the collection is mutable and
    /// has no associated version assigned the updates will be executed `in place`. Otherwise this will perform an update including
    /// a `pin` operation. Pin operation copys all objects / objectgroups references etc. and creates a new collection that solely owns
    /// these objects. This makes sure that versioned collections are truely immutable and can not be modified once created.
    /// (Except forceful deletion to be GDPR compliant)
    ///
    /// ## Arguments
    ///
    /// `UpdateCollectionRequest` - Contains the information the collection should be updated to.
    ///
    /// ## Results
    ///
    /// `GetCollectionsResponse` - Responds with an collection_overview for the updated or newly created collection.
    ///
    async fn update_collection(
        &self,
        request: Request<UpdateCollectionRequest>,
    ) -> Result<Response<UpdateCollectionResponse>, tonic::Status> {
        log::info!("Received UpdateCollectionRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Query the user_permissions -> Needs collection "WRITE" permissions
        let user_id = self
            .authz
            .collection_authorize(
                request.metadata(),
                diesel_ulid::DieselUlid::from_str(&request.get_ref().collection_id)
                    .map_err(|_| ArunaError::TypeConversionError(TypeConversionError::UUID))?,
                UserRights::WRITE,
            )
            .await?;

        // Execute request in spawn_blocking task to prevent blocking the API server
        let db = self.database.clone();
        let (response, bucket, project_ulid) = task::spawn_blocking(move || {
            db.update_collection(request.get_ref().to_owned(), user_id)
        })
        .await
        .map_err(ArunaError::from)??;

        match &self.kube_client {
            Some(kc) => {
                if let Err(err) = kc.create_bucket(&bucket).await {
                    log::error!("Unable to create kube_bucket err: {err}")
                }
            }
            None => {}
        }

        // Try to emit event notification
        match &response.collection {
            Some(collection) => {
                let collection_clone = collection.clone();
                if let Some(emit_client) = &self.event_emitter {
                    let event_emitter_clone = emit_client.clone();
                    task::spawn(async move {
                        if let Err(err) = event_emitter_clone
                            .emit_event(
                                collection_clone.id.clone(),
                                ResourceType::Collection,
                                EventType::Updated,
                                vec![EmittedResource {
                                    resource: Some(Resource::Collection(CollectionResource {
                                        project_id: project_ulid.to_string(),
                                        collection_id: collection_clone.id,
                                    })),
                                }],
                            )
                            .await
                        {
                            // Only log error but do not crash function execution at this point
                            log::error!("Failed to emit notification: {}", err)
                        }
                    })
                    .await
                    .map_err(ArunaError::from)?;
                }
            }
            None => {} // Do nothing if no collection in response.
        }

        // Create gRPC response
        let response = Response::new(response);

        log::info!("Sending UpdateCollectionResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// PinCollectionVersion creates a copy of the collection with a specific "pinned" version
    ///
    /// ## Permissions
    ///
    /// This needs collection `WRITE` permissions.
    ///
    /// ## Behaviour
    ///
    /// Similar to update collection but only updates the `version` without any other fields.
    /// Should be mainly used to pin down a dynamic collection to a specific version and freeze all its contents.
    /// Will fail if new version is less than old version. (Backwards updates are not allowed)
    ///
    /// ## Arguments
    ///
    /// `PinCollectionVersionRequest` - Contains the collection_id and the new version.
    ///
    /// ## Results
    ///
    /// `PinCollectionVersionResponse` - Responds with an collection_overview for the new versioned collection.
    ///
    async fn pin_collection_version(
        &self,
        request: tonic::Request<PinCollectionVersionRequest>,
    ) -> Result<tonic::Response<PinCollectionVersionResponse>, tonic::Status> {
        log::info!("Received PinCollectionVersionRequest.");
        log::debug!("{}", format_grpc_request(&request));

        let user_id = self
            .authz
            .collection_authorize(
                request.metadata(),
                diesel_ulid::DieselUlid::from_str(&request.get_ref().collection_id)
                    .map_err(|_| ArunaError::TypeConversionError(TypeConversionError::UUID))?,
                UserRights::WRITE,
            )
            .await?;

        // Execute request in spawn_blocking task to prevent blocking the API server
        let db = self.database.clone();
        let (response, project_ulid, bucket) = task::spawn_blocking(move || {
            db.pin_collection_version(request.get_ref().to_owned(), user_id)
        })
        .await
        .map_err(ArunaError::from)??;

        match &self.kube_client {
            Some(kc) => {
                if let Err(err) = kc.create_bucket(&bucket).await {
                    log::error!("Unable to create kube_bucket err: {err}")
                }
            }
            None => {}
        }

        // Try to emit event notification
        match &response.collection {
            Some(collection) => {
                let collection_clone = collection.clone();
                if let Some(emit_client) = &self.event_emitter {
                    let event_emitter_clone = emit_client.clone();
                    task::spawn(async move {
                        if let Err(err) = event_emitter_clone
                            .emit_event(
                                collection_clone.id.clone(),
                                ResourceType::Collection,
                                EventType::Created,
                                vec![EmittedResource {
                                    resource: Some(Resource::Collection(CollectionResource {
                                        project_id: project_ulid.to_string(),
                                        collection_id: collection_clone.id,
                                    })),
                                }],
                            )
                            .await
                        {
                            // Only log error but do not crash function execution at this point
                            log::error!("Failed to emit notification: {}", err)
                        }
                    })
                    .await
                    .map_err(ArunaError::from)?;
                }
            }
            None => {} // Do nothing if no collection in response.
        }

        // Create gRPC response
        let response = Response::new(response);

        log::info!("Sending PinCollectionVersionResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// DeleteCollection will delete a specific collection including its contents.
    ///
    /// ## Permissions
    ///
    /// This needs collection `WRITE` permissions without force
    /// and project `ADMIN` with force.
    ///
    /// ## Behaviour
    ///
    /// This will delete a specific collection and remove all associated objects / keyvalues / objectgroups etc.
    /// If `force` is not specified this will fail if objects are writeable referenced in any other collection.
    /// Force will delete all references and set all writeable objects to "TRASH", this might interfere with other
    /// collections and could create side-effects. -> Only project admins can forcefully delete a collection.
    /// The preferred way to delete a collection is to first solve all references and delete the collection normally
    /// afterwards.
    ///
    /// ## Arguments
    ///
    /// `DeleteCollectionRequest` - Collection_id, project_id and force bool
    ///
    /// ## Results
    ///
    /// `DeleteCollectionResponse` - Placeholder this response is currently empty, which means success.
    ///
    async fn delete_collection(
        &self,
        request: tonic::Request<DeleteCollectionRequest>,
    ) -> Result<tonic::Response<DeleteCollectionResponse>, tonic::Status> {
        log::info!("Received DeleteCollectionRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Consume gRPC request into its parts
        let (metadata, _, inner_request) = request.into_parts();

        // Parse the inner request parameter
        let collection_ulid = diesel_ulid::DieselUlid::from_str(&inner_request.collection_id)
            .map_err(ArunaError::from)?;
        let force_delete = inner_request.force;

        // Validate user permissions for collection deletion
        let user_id = if force_delete {
            self.authz
                .project_authorize_by_collectionid(&metadata, collection_ulid, UserRights::ADMIN)
                .await?
        } else {
            self.authz
                .collection_authorize(&metadata, collection_ulid, UserRights::WRITE)
                .await?
        };

        // Execute request in spawn_blocking task to prevent blocking the API server
        let db = self.database.clone();
        let project_ulid = task::spawn_blocking(move || {
            db.delete_collection(&collection_ulid, user_id, force_delete)
        })
        .await
        .map_err(ArunaError::from)??;

        // Try to emit event notification
        if let Some(emit_client) = &self.event_emitter {
            let event_emitter_clone = emit_client.clone();
            task::spawn(async move {
                if let Err(err) = event_emitter_clone
                    .emit_event(
                        collection_ulid.to_string(),
                        ResourceType::Collection,
                        EventType::Deleted,
                        vec![EmittedResource {
                            resource: Some(Resource::Collection(CollectionResource {
                                project_id: project_ulid.to_string(),
                                collection_id: collection_ulid.to_string(),
                            })),
                        }],
                    )
                    .await
                {
                    // Only log error but do not crash function execution at this point
                    log::error!("Failed to emit notification: {}", err)
                }
            })
            .await
            .map_err(ArunaError::from)?;
        }

        // Create and send gRPC response
        let response = Response::new(DeleteCollectionResponse {});

        log::info!("Sending DeleteCollectionResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// AddKeyValueToCollection
    ///
    /// Status: BETA
    ///
    /// Adds key values (labels / hooks) to a collection
    async fn add_key_values_to_collection(
        &self,
        _request: tonic::Request<AddKeyValuesToCollectionRequest>,
    ) -> std::result::Result<tonic::Response<AddKeyValuesToCollectionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Currently unimplemented!"))
    }
}

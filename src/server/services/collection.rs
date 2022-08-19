use std::sync::Arc;

use tokio::task;
use tonic::Response;

use crate::api::aruna::api::storage::services::v1::*;
use crate::database::connection::Database;
use crate::database::models::enums::*;
use crate::{
    api::aruna::api::storage::services::v1::collection_service_server::CollectionService,
    error::ArunaError,
};

use super::authz::{Authz, Context};

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(CollectionServiceImpl);

#[tonic::async_trait]
impl CollectionService for CollectionServiceImpl {
    /// Create_new_collection request cretes a new collection based on user request
    async fn create_new_collection(
        &self,
        request: tonic::Request<CreateNewCollectionRequest>,
    ) -> Result<tonic::Response<CreateNewCollectionResponse>, tonic::Status> {
        let project_id =
            uuid::Uuid::parse_str(&request.get_ref().project_id).map_err(ArunaError::from)?;

        let creator_id = self
            .authz
            .authorize(
                request.metadata(),
                Context {
                    user_right: UserRights::WRITE,
                    resource_type: Resources::PROJECT,
                    resource_id: project_id,
                    admin: false,
                    personal: false,
                    oidc_context: false,
                },
            )
            .await?;

        let db = self.database.clone();
        // Execute request in spawn_blocking task to prevent blocking the API server
        Ok(Response::new(
            task::spawn_blocking(move || {
                db.create_new_collection(request.get_ref().to_owned(), creator_id)
            })
            .await
            .map_err(ArunaError::from)??,
        ))
    }

    /// GetCollection queries a specific Collection by ID
    /// The result can be one_of:
    /// CollectionOverview -> default
    /// CollectionWithID
    /// Collection (full)
    /// This can be modified with the optional OutputFormat parameter
    async fn get_collection_by_id(
        &self,
        _request: tonic::Request<GetCollectionByIdRequest>,
    ) -> Result<tonic::Response<GetCollectionByIdResponse>, tonic::Status> {
        todo!()
    }
    /// GetCollections queries multiple collections by ID or by LabelFilter
    /// This returns by default a paginated result with 20 entries.
    async fn get_collections(
        &self,
        _request: tonic::Request<GetCollectionsRequest>,
    ) -> Result<tonic::Response<GetCollectionsResponse>, tonic::Status> {
        todo!()
    }
    /// UpdateCollection updates the current collection
    /// This will update the collection in place if it is unversioned / latest
    /// A versioned (pinned) collection requires a new semantic version after the update
    /// This can be used to pin a collection to a specific version
    /// similar to the PinCollectionVersion request
    async fn update_collection(
        &self,
        _request: tonic::Request<UpdateCollectionRequest>,
    ) -> Result<tonic::Response<UpdateCollectionResponse>, tonic::Status> {
        todo! {}
    }
    /// PinCollectionVersion this pins the current status of the version to a specific version
    /// This effectively creates a copy of the collection with a stable version
    /// All objects will be pinned to an explicit revision number
    /// Pinned collections can not be updated in place
    async fn pin_collection_version(
        &self,
        _request: tonic::Request<PinCollectionVersionRequest>,
    ) -> Result<tonic::Response<PinCollectionVersionResponse>, tonic::Status> {
        todo!()
    }
    /// This request deletes the collection.
    /// If with_version is true, it deletes the collection and all its versions.
    /// If cascade is true, all objects that are owned by the collection will also deleted.
    /// This should be avoided
    async fn delete_collection(
        &self,
        _request: tonic::Request<DeleteCollectionRequest>,
    ) -> Result<tonic::Response<DeleteCollectionResponse>, tonic::Status> {
        todo!()
    }

    // async fn create_collection(
    //     &self,
    //     request: tonic::Request<CreateCollectionRequest>,
    // ) -> Result<tonic::Response<CreateCollectionResponse>, tonic::Status> {
    //     let id = self.database.create_collection(request.into_inner());

    //     Ok(Response::new(CreateCollectionResponse {
    //         id: id.to_string(),
    //     }))
    // }

    // async fn get_collection(
    //     &self,
    //     request: tonic::Request<GetCollectionRequest>,
    // ) -> Result<tonic::Response<GetCollectionResponse>, tonic::Status> {
    //     let request_uuid = uuid::Uuid::parse_str(request.into_inner().id.as_str()).unwrap();

    //     let collection = self.database.get_collection(request_uuid);

    //     Ok(Response::new(GetCollectionResponse {
    //         collection: Some(collection),
    //     }))
    // }
}

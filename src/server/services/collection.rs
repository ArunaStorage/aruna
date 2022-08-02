use std::sync::Arc;

use crate::api::aruna::api::storage::services::v1::collection_service_server::CollectionService;
use crate::api::aruna::api::storage::services::v1::*;
use crate::database::connection::Database;
use crate::database::models::enums::*;

use super::authz::{Authz, Context};

pub struct CollectionServiceImpl {
    database: Arc<Database>,
}

impl CollectionServiceImpl {
    pub async fn new(db: Arc<Database>) -> Self {
        CollectionServiceImpl { database: db }
    }
}

#[tonic::async_trait]
impl CollectionService for CollectionServiceImpl {
    async fn create_new_collection(
        &self,
        request: tonic::Request<CreateNewCollectionRequest>,
    ) -> Result<tonic::Response<CreateNewCollectionResponse>, tonic::Status> {
        let result = Authz::authorize(
            self.database.clone(),
            request.metadata(),
            Context {
                user_right: UserRights::WRITE,
                resource_type: Resources::PROJECT,
                uid: todo!(), // TODO: request.project
            },
        );

        todo!()
    }
    /// GetCollection queries a specific Collection by ID
    /// The result can be one_of:
    /// CollectionOverview -> default
    /// CollectionWithID
    /// Collection (full)
    /// This can be modified with the optional OutputFormat parameter
    async fn get_collection_by_id(
        &self,
        request: tonic::Request<GetCollectionByIdRequest>,
    ) -> Result<tonic::Response<GetCollectionByIdResponse>, tonic::Status> {
        todo!()
    }
    /// GetCollections queries multiple collections by ID or by LabelFilter
    /// This returns by default a paginated result with 20 entries.
    async fn get_collections(
        &self,
        request: tonic::Request<GetCollectionsRequest>,
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
        request: tonic::Request<UpdateCollectionRequest>,
    ) -> Result<tonic::Response<UpdateCollectionResponse>, tonic::Status> {
        todo! {}
    }
    /// PinCollectionVersion this pins the current status of the version to a specific version
    /// This effectively creates a copy of the collection with a stable version
    /// All objects will be pinned to an explicit revision number
    /// Pinned collections can not be updated in place
    async fn pin_collection_version(
        &self,
        request: tonic::Request<PinCollectionVersionRequest>,
    ) -> Result<tonic::Response<PinCollectionVersionResponse>, tonic::Status> {
        todo!()
    }
    /// This request deletes the collection.
    /// If with_version is true, it deletes the collection and all its versions.
    /// If cascade is true, all objects that are owned by the collection will also deleted.
    /// This should be avoided
    async fn delete_collection(
        &self,
        request: tonic::Request<DeleteCollectionRequest>,
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

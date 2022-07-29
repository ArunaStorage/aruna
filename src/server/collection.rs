use std::sync::Arc;

use tonic::Response;

use crate::api::aruna::api::storage::services::v1::collection_service_server::CollectionService;
//use crate::api::aruna::api::storage::services::v1::*;
use crate::database::connection::Database;

pub struct CollectionServiceImpl {
    database: Arc<Database>,
}

impl CollectionServiceImpl {
    pub async fn new(db: Arc<Database>) -> Self {
        let collection_service = CollectionServiceImpl { database: db };

        return collection_service;
    }
}

// #[tonic::async_trait]
// impl CollectionService for CollectionServiceImpl {
//     async fn create_collection(
//         &self,
//         request: tonic::Request<CreateCollectionRequest>,
//     ) -> Result<tonic::Response<CreateCollectionResponse>, tonic::Status> {
//         let id = self.database.create_collection(request.into_inner());
//
//         Ok(Response::new(CreateCollectionResponse {
//             id: id.to_string(),
//         }))
//     }
//
//     async fn get_collection(
//         &self,
//         request: tonic::Request<GetCollectionRequest>,
//     ) -> Result<tonic::Response<GetCollectionResponse>, tonic::Status> {
//         let request_uuid = uuid::Uuid::parse_str(request.into_inner().id.as_str()).unwrap();
//
//         let collection = self.database.get_collection(request_uuid);
//
//         Ok(Response::new(GetCollectionResponse {
//             collection: Some(collection),
//         }))
//     }
// }

use std::io::{Error, ErrorKind};

use super::utils::*;
use crate::{
    api::aruna::api::storage::services::v1::{
        CreateNewCollectionRequest, CreateNewCollectionResponse,
    },
    database::{connection::Database, models},
};

impl Database {
    pub fn create_new_collection(
        &self,
        request: CreateNewCollectionRequest,
        creator: uuid::Uuid,
    ) -> Result<CreateNewCollectionResponse, Box<dyn std::error::Error>> {
        // Dynamic dispatch to return any error type, ok for now but should be changed to custom error types
        // Create a new uuid for collection insert
        let collection_uuid = uuid::Uuid::new_v4();

        let shared_version_uuid = uuid::Uuid::new_v4();

        let key_values = _to_collection_key_values(request.labels, request.hooks, collection_uuid);

        let db_collection = models::collection::Collection {
            id: collection_uuid,
            shared_version_id: shared_version_uuid,
            name: request.name,
            description: request.description,
            created_by: creator,
            created_at: todo!(),
            version_id: None,
            dataclass: None,
            project_id: uuid::Uuid::parse_str(&request.project_id)?,
        };

        Err(Box::new(Error::new(ErrorKind::Other, "oh no!")))
        // self.pg_connection
        //     .get()
        //     .unwrap()
        //     .transaction::<_, Error, _>(|conn| {
        //         db_labels.insert_into(labels).execute(conn)?;
        //         db_collection.insert_into(collections).execute(conn)?;
        //         db_collection_labels
        //             .insert_into(collection_labels)
        //             .execute(conn)?;
        //         Ok(())
        //     })
        //     .unwrap();

        // return collection_uuid;
    }
}

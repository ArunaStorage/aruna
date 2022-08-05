use std::io::{Error, ErrorKind};

use chrono::Local;
use diesel::{insert_into, prelude::*};

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
        use crate::database::schema::collection_key_value::dsl::*;
        use crate::database::schema::collections::dsl::*;
        use diesel::result::Error;

        let collection_uuid = uuid::Uuid::new_v4();

        let shared_version_uuid = uuid::Uuid::new_v4();

        let key_values = to_collection_key_values(request.labels, request.hooks, collection_uuid);

        let db_collection = models::collection::Collection {
            id: collection_uuid,
            shared_version_id: shared_version_uuid,
            name: request.name,
            description: request.description,
            created_by: creator,
            created_at: Local::now().naive_local(),
            version_id: None,
            dataclass: None,
            project_id: uuid::Uuid::parse_str(&request.project_id)?,
        };

        let request_result = self
            .pg_connection
            .get()?
            .transaction::<_, Error, _>(|conn| {
                // Get the API token, if this errors -> no corresponding database token object could be found
                insert_into(collection_key_value)
                    .values(key_values)
                    .execute(conn)?;

                insert_into(collections)
                    .values(db_collection)
                    .execute(conn)?;
                Ok(())
            });

        match request_result {
            Ok(_) => {
                return Ok(CreateNewCollectionResponse {
                    id: collection_uuid.to_string(),
                })
            }
            Err(e) => return Err(Box::new(e)),
        }
    }
}

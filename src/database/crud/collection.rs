use super::utils::*;
use crate::api::aruna::api::storage::models::v1::LabelOntology;
use crate::api::aruna::api::storage::services::v1::{
    CreateNewCollectionRequest, CreateNewCollectionResponse, GetCollectionByIdRequest,
    GetCollectionByIdResponse,
};
use crate::database::connection::Database;
use crate::database::models;
use crate::database::models::collection::RequiredLabel;
use crate::error::ArunaError;
use chrono::Local;
use diesel::insert_into;
use diesel::prelude::*;

impl Database {
    pub fn create_new_collection(
        &self,
        request: CreateNewCollectionRequest,
        creator: uuid::Uuid,
    ) -> Result<CreateNewCollectionResponse, ArunaError> {
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

        self.pg_connection
            .get()?
            .transaction::<_, Error, _>(|conn| {
                insert_into(collections)
                    .values(db_collection)
                    .execute(conn)?;
                // Get the API token, if this errors -> no corresponding database token object could be found
                insert_into(collection_key_value)
                    .values(key_values)
                    .execute(conn)?;

                Ok(())
            })?;

        Ok(CreateNewCollectionResponse {
            collection_id: collection_uuid.to_string(),
        })
    }

    fn get_collection_by_id(
        &self,
        request: GetCollectionByIdRequest,
    ) -> Result<GetCollectionByIdResponse, ArunaError> {
        use crate::database::schema::collection_key_value::dsl as ckv;
        use crate::database::schema::collections::dsl as col;
        use crate::database::schema::required_labels::dsl as rlbl;
        use diesel::prelude::*;
        use diesel::result::Error;

        let collection_id = uuid::Uuid::parse_str(&request.collection_id)?;

        let _collection = self
            .pg_connection
            .get()?
            .transaction::<_, Error, _>(|conn| {
                let collection_info = col::collections
                    .filter(col::id.eq(collection_id))
                    .first::<models::collection::Collection>(conn)?;

                let collection_key_values = ckv::collection_key_value
                    .filter(ckv::collection_id.eq(collection_id))
                    .load::<models::collection::CollectionKeyValue>(conn)?;

                let (labels, hooks) = from_collection_key_values(collection_key_values);

                let label_ontology = rlbl::required_labels
                    .filter(rlbl::collection_id.eq(collection_id))
                    .load::<models::collection::RequiredLabel>(conn)
                    .optional()?;
                todo!()
                // Ok(collection_info)
            })?;

        todo!()
    }
}

/* ----------------- Section for collection specific helper functions ------------------- */

fn req_labels_to_label_ontology(input: Option<Vec<RequiredLabel>>) -> Option<LabelOntology> {
    input.map(|vec| LabelOntology {
        required_label_keys: vec.into_iter().map(|elem| elem.label_key).collect(),
    })
}

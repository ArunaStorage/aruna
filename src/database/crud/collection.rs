use std::time::SystemTime;

use super::utils::*;
use crate::api::aruna::api::storage::models::v1::{
    Collection, CollectionOverview, CollectionWithId, KeyValue, LabelOntology,
};
use crate::api::aruna::api::storage::services::v1::{
    get_collection_by_id_response::Collection as GetCollectionResponseEnum,
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
                // Get the API token, if this errors -> no corresponding database token object could be found
                insert_into(collection_key_value)
                    .values(key_values)
                    .execute(conn)?;

                insert_into(collections)
                    .values(db_collection)
                    .execute(conn)?;
                Ok(())
            })?;

        return Ok(CreateNewCollectionResponse {
            id: collection_uuid.to_string(),
        });
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

        let collection_id = uuid::Uuid::parse_str(&request.id)?;

        let collection = self
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
                let retformat = match request.format {
                    0 | 1 => {
                        let test = "";
                        GetCollectionResponseEnum::CollectionOverview(CollectionOverview {
                            id: collection_info.id.to_string(),
                            name: collection_info.name,
                            description: collection_info.description,
                            labels: labels,
                            hooks: hooks,
                            label_ontology: req_labels_to_label_ontology(label_ontology),
                            created: todo!(),
                            stats: todo!(),
                            is_public: todo!(),
                            version: todo!(),
                        })
                    }
                    2 => GetCollectionResponseEnum::CollectionWithId(CollectionWithId {
                        id: todo!(),
                        name: todo!(),
                        description: todo!(),
                        labels: todo!(),
                        hooks: todo!(),
                        label_ontology: todo!(),
                        created: todo!(),
                        objects: todo!(),
                        specifications: todo!(),
                        object_groups: todo!(),
                        stats: todo!(),
                        is_public: todo!(),
                        version: todo!(),
                    }),
                    3 => GetCollectionResponseEnum::CollectionFull(Collection {
                        id: todo!(),
                        name: todo!(),
                        description: todo!(),
                        labels: todo!(),
                        hooks: todo!(),
                        label_ontology: todo!(),
                        created: todo!(),
                        objects: todo!(),
                        specifications: todo!(),
                        object_groups: todo!(),
                        stats: todo!(),
                        is_public: todo!(),
                        version: todo!(),
                    }),
                    _ => return Err(Error::NotFound), // This should never occur
                };
                todo!()
                // Ok(collection_info)
            })?;

        todo!()
    }
}

/* ----------------- Section for collection specific helper functions ------------------- */

fn req_labels_to_label_ontology(input: Option<Vec<RequiredLabel>>) -> Option<LabelOntology> {
    match input {
        Some(vec) => Some(LabelOntology {
            required_label_keys: vec.into_iter().map(|elem| elem.label_key).collect(),
        }),
        None => None,
    }
}

fn build_collection_response_enum(
    format: i64,
    collection_info: crate::database::models::collection::Collection,
    labels: Vec<KeyValue>,
    hooks: Vec<KeyValue>,
    label_ontology: Option<LabelOntology>,
) -> Result<GetCollectionResponseEnum, diesel::result::Error> {
    match format {
        0 | 1 => {
            let test = "";
            Ok(GetCollectionResponseEnum::CollectionOverview(
                CollectionOverview {
                    id: collection_info.id.to_string(),
                    name: collection_info.name,
                    description: collection_info.description,
                    labels: labels,
                    hooks: hooks,
                    label_ontology: label_ontology,
                    created: Some(
                        naivedatetime_to_prost_time(collection_info.created_at)
                            .map_err(|_| diesel::result::Error::BrokenTransaction)?,
                    ),
                    stats: todo!(),
                        is_public: todo!(),
                    version: todo!(),
                },
            ))
        }
        2 => Ok(GetCollectionResponseEnum::CollectionWithId(
            CollectionWithId {
                id: todo!(),
                name: todo!(),
                description: todo!(),
                labels: todo!(),
                hooks: todo!(),
                label_ontology: todo!(),
                created: todo!(),
                objects: todo!(),
                specifications: todo!(),
                object_groups: todo!(),
                stats: todo!(),
                is_public: todo!(),
                version: todo!(),
            },
        )),
        3 => Ok(GetCollectionResponseEnum::CollectionFull(Collection {
            id: todo!(),
            name: todo!(),
            description: todo!(),
            labels: todo!(),
            hooks: todo!(),
            label_ontology: todo!(),
            created: todo!(),
            objects: todo!(),
            specifications: todo!(),
            object_groups: todo!(),
            stats: todo!(),
            is_public: todo!(),
            version: todo!(),
        })),
        _ => return Err(diesel::result::Error::NotFound),
    }
}
